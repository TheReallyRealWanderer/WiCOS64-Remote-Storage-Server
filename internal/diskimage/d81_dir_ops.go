package diskimage

import (
	"fmt"
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// splitD81DirPath parses an inner D81 path that refers to a directory/partition.
//
// Example inputs:
//
//	"UTILS" -> ["UTILS"]
//	"GAMES/ARCADE" -> ["GAMES", "ARCADE"]
func splitD81DirPath(innerDir string) ([]string, error) {
	inner := strings.Trim(strings.TrimSpace(innerDir), "/")
	if inner == "" {
		return nil, newStatusErr(proto.StatusBadRequest, "missing directory path")
	}
	if strings.ContainsAny(inner, "*?") {
		return nil, newStatusErr(proto.StatusBadRequest, "wildcards are not allowed")
	}
	parts := strings.Split(inner, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := sanitizeD64Name(p)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, newStatusErr(proto.StatusBadRequest, "missing directory path")
	}
	return out, nil
}

// MkdirDirD81 creates a 1581 "partition directory" inside a .D81 image.
//
// This rebuilds (re-packs) the image so that partitions remain contiguous.
//
// parents behaves like "mkdir -p" inside the image.
func MkdirDirD81(imgPath, innerDir string, parents bool) error {
	st, err := os.Stat(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusNotFound, "image not found")
	}
	perm := st.Mode().Perm()

	origImg, err := os.ReadFile(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusInternal, "failed to read image")
	}
	if int64(len(origImg)) < d81BytesNoErrorInfo {
		return newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}

	parts, err := splitD81DirPath(innerDir)
	if err != nil {
		return err
	}

	rootHeader := make([]byte, 256)
	copy(rootHeader, d81ReadSector(origImg, int(d81DirTrack), 0))

	root, err := buildD81TreeFromImage(origImg)
	if err != nil {
		return err
	}

	// Walk (and optionally create) the directory chain.
	cur := root
	for i, seg := range parts {
		key := d81NameKey(seg)
		// A regular file with the same name blocks mkdir.
		if cur.Files != nil {
			if _, exists := cur.Files[key]; exists {
				return newStatusErr(proto.StatusAlreadyExists, "a file with the same name exists")
			}
		}
		if cur.Dirs == nil {
			cur.Dirs = make(map[string]*d81TreeDir)
		}
		nxt, exists := cur.Dirs[key]
		if !exists || nxt == nil {
			// Missing parent segment.
			if i < len(parts)-1 && !parents {
				return newStatusErr(proto.StatusNotFound, fmt.Sprintf("parent directory missing: %s", seg))
			}
			nxt = &d81TreeDir{
				Name:     seg,
				TypeCode: 5, // CBM (1581 partition)
				Files:    make(map[string]*d81TreeFile),
				Dirs:     make(map[string]*d81TreeDir),
			}
			cur.Dirs[key] = nxt
		}
		cur = nxt
	}

	if err := computeD81RepackTracks(root, d81RepackBufferTracks); err != nil {
		return err
	}

	// Build new image with the same total size as the original to preserve error-info bytes (if present).
	noErr := int(d81BytesNoErrorInfo)
	newImg := make([]byte, len(origImg))
	base := newImg
	if len(base) > noErr {
		base = base[:noErr]
	}
	if len(origImg) > noErr {
		copy(newImg[noErr:], origImg[noErr:])
	}
	if err := formatD81Root(base, rootHeader); err != nil {
		return err
	}
	rootCtx := d81FSContext{sysTrack: int(d81DirTrack), dirStartT: uint8(d81DirTrack), dirStartS: uint8(d81DirSector)}
	if err := populateD81Dir(base, rootCtx, root, rootHeader); err != nil {
		return err
	}
	if err := atomicWriteFile(imgPath, newImg, perm); err != nil {
		return newStatusErr(proto.StatusInternal, "failed to write image")
	}
	d81Cache.Delete(imgPath)
	return nil
}

// RmdirDirD81 removes a 1581 "partition directory" inside a .D81 image.
//
// If recursive is false, the directory must be empty (no files and no subdirectories).
//
// This rebuilds (re-packs) the image so that partitions remain contiguous.
func RmdirDirD81(imgPath, innerDir string, recursive bool) error {
	st, err := os.Stat(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusNotFound, "image not found")
	}
	perm := st.Mode().Perm()

	origImg, err := os.ReadFile(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusInternal, "failed to read image")
	}
	if int64(len(origImg)) < d81BytesNoErrorInfo {
		return newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}

	parts, err := splitD81DirPath(innerDir)
	if err != nil {
		return err
	}

	rootHeader := make([]byte, 256)
	copy(rootHeader, d81ReadSector(origImg, int(d81DirTrack), 0))

	root, err := buildD81TreeFromImage(origImg)
	if err != nil {
		return err
	}

	// Traverse to parent.
	cur := root
	for i := 0; i < len(parts)-1; i++ {
		seg := parts[i]
		key := d81NameKey(seg)
		nxt := cur.Dirs[key]
		if nxt == nil {
			return newStatusErr(proto.StatusNotFound, "not found")
		}
		cur = nxt
	}
	leaf := parts[len(parts)-1]
	leafKey := d81NameKey(leaf)
	// If a file exists with that name, rmdir should fail with NOT_A_DIR.
	if cur.Files != nil {
		if _, ok := cur.Files[leafKey]; ok {
			return newStatusErr(proto.StatusNotADir, "not a directory")
		}
	}
	target := cur.Dirs[leafKey]
	if target == nil {
		return newStatusErr(proto.StatusNotFound, "not found")
	}
	if !recursive {
		if len(target.Files) > 0 || len(target.Dirs) > 0 {
			return newStatusErr(proto.StatusDirNotEmpty, "directory not empty")
		}
	}
	delete(cur.Dirs, leafKey)

	if err := computeD81RepackTracks(root, d81RepackBufferTracks); err != nil {
		return err
	}

	// Build new image with the same total size as the original to preserve error-info bytes (if present).
	noErr := int(d81BytesNoErrorInfo)
	newImg := make([]byte, len(origImg))
	base := newImg
	if len(base) > noErr {
		base = base[:noErr]
	}
	if len(origImg) > noErr {
		copy(newImg[noErr:], origImg[noErr:])
	}
	if err := formatD81Root(base, rootHeader); err != nil {
		return err
	}
	rootCtx := d81FSContext{sysTrack: int(d81DirTrack), dirStartT: uint8(d81DirTrack), dirStartS: uint8(d81DirSector)}
	if err := populateD81Dir(base, rootCtx, root, rootHeader); err != nil {
		return err
	}
	if err := atomicWriteFile(imgPath, newImg, perm); err != nil {
		return newStatusErr(proto.StatusInternal, "failed to write image")
	}
	d81Cache.Delete(imgPath)
	return nil
}

// RenameDirD81 renames a 1581 "partition directory" (subdirectory) inside a .D81 image.
//
// On a 1581, "directories" are implemented as partitions. Renaming a directory therefore
// updates both:
//   - the directory entry name in the parent directory
//   - the label stored in the partition header (track start, sector 0).
//
// For safety, moving a directory across different parent partitions is not supported
// (it would require physically relocating the partition). Only renames within the
// same parent are allowed.
func RenameDirD81(imgPath, oldDirPath, newDirPath string, allowOverwrite bool) error {
	if strings.ContainsAny(oldDirPath, "*?") || strings.ContainsAny(newDirPath, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards are not allowed")
	}

	oldParts, err := splitD81DirPath(oldDirPath)
	if err != nil {
		return err
	}
	newParts, err := splitD81DirPath(newDirPath)
	if err != nil {
		return err
	}
	if len(oldParts) == 0 || len(newParts) == 0 {
		return newStatusErr(proto.StatusBadRequest, "missing directory path")
	}

	oldParent := oldParts[:len(oldParts)-1]
	oldLeaf := oldParts[len(oldParts)-1]
	newParent := newParts[:len(newParts)-1]
	newLeaf := newParts[len(newParts)-1]

	// No-op if unchanged.
	if oldLeaf == newLeaf && equalStringSlices(oldParent, newParent) {
		return nil
	}

	// Renaming/moving a directory across different parents is not supported without repack.
	if !equalStringSlices(oldParent, newParent) {
		return newStatusErr(proto.StatusNotSupported, "moving directories across partitions is not supported")
	}

	st, err := os.Stat(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusNotFound, "image not found")
	}
	perm := st.Mode().Perm()

	origImg, err := os.ReadFile(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusInternal, "failed to read image")
	}
	if int64(len(origImg)) < d81BytesNoErrorInfo {
		return newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}

	img := make([]byte, len(origImg))
	copy(img, origImg)

	ctx, err := resolveD81WriteContext(img, oldParent)
	if err != nil {
		return err
	}

	loc, _, _, err := findD81DirSlot(img, ctx, oldLeaf)
	if err != nil {
		return err
	}
	if !loc.found {
		return newStatusErr(proto.StatusNotFound, "not found")
	}
	if loc.entryTypeCode != 5 && loc.entryTypeCode != 6 {
		return newStatusErr(proto.StatusNotADir, "not a directory")
	}

	// Destination collision check.
	loc2, _, _, err := findD81DirSlot(img, ctx, newLeaf)
	if err != nil {
		return err
	}
	if loc2.found && loc2.slotOff != loc.slotOff {
		// Never overwrite an existing directory.
		if loc2.entryTypeCode == 5 || loc2.entryTypeCode == 6 {
			return newStatusErr(proto.StatusAlreadyExists, "destination exists")
		}
		if !allowOverwrite {
			return newStatusErr(proto.StatusAlreadyExists, "destination exists")
		}
		// Overwrite regular file at destination by deleting it.
		b, err := newD81BAMAt(img, ctx.sysTrack)
		if err != nil {
			return err
		}
		if err := deleteD81FileChain(img, b, loc2.startT, loc2.startS); err != nil {
			return err
		}
		clearD81DirSlot(img, loc2)
	}

	// Update directory entry name bytes in parent slot (slot[5..20]).
	slot := img[loc.slotOff : loc.slotOff+32]
	for i := 0; i < 16; i++ {
		slot[5+i] = 0xA0
	}
	nb := []byte(newLeaf)
	if len(nb) > 16 {
		nb = nb[:16]
	}
	copy(slot[5:21], nb)

	// Update label in partition header (best effort).
	startT := int(loc.startT)
	if startT > 0 && startT <= d81Tracks {
		// Prefer the entry's start sector.
		updated := false
		if int(loc.startS) >= 0 && int(loc.startS) < d81SectorsPerTrack {
			hdr := d81ReadSector(img, startT, int(loc.startS))
			if hdr[2] == 'D' {
				for i := 4; i < 20; i++ {
					hdr[i] = 0xA0
				}
				lb := []byte(strings.ToUpper(strings.TrimSpace(newLeaf)))
				if len(lb) > 16 {
					lb = lb[:16]
				}
				copy(hdr[4:20], lb)
				updated = true
			}
		}
		if !updated {
			// Common layout: header is always sector 0.
			hdr := d81ReadSector(img, startT, 0)
			if hdr[2] == 'D' {
				for i := 4; i < 20; i++ {
					hdr[i] = 0xA0
				}
				lb := []byte(strings.ToUpper(strings.TrimSpace(newLeaf)))
				if len(lb) > 16 {
					lb = lb[:16]
				}
				copy(hdr[4:20], lb)
			}
		}
	}

	if err := atomicWriteFile(imgPath, img, perm); err != nil {
		return newStatusErr(proto.StatusInternal, "failed to write image")
	}
	d81Cache.Delete(imgPath)
	return nil
}
