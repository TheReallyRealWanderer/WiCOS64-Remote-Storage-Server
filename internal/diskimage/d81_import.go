package diskimage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"wicos64-server/internal/proto"
)

// ImportDirD81 imports a filesystem directory tree into a D81 image as a 1581
// "partition" directory.
//
// It works by rebuilding (repacking) the image so that:
//   - missing partition directories are created as needed
//   - partitions are allocated in contiguous track ranges
//
// dstInnerDir is the path *inside* the image where the new directory should live
// (without a leading slash). It may contain subdirectories (partitions).
//
// When allowOverwrite is true and the destination already exists (file or
// directory), it is replaced.
func ImportDirD81(imgPath string, dstInnerDir string, srcDirAbs string, allowOverwrite bool, fallbackPRG bool, maxFileBytes uint64) error {
	dstInnerDir = strings.Trim(strings.TrimSpace(dstInnerDir), "/")
	if dstInnerDir == "" {
		return newStatusErr(proto.StatusBadRequest, "empty destination directory")
	}
	if strings.ContainsAny(dstInnerDir, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards not allowed")
	}

	// Validate source directory.
	srcInfo, err := os.Stat(srcDirAbs)
	if err != nil {
		if os.IsNotExist(err) {
			return newStatusErr(proto.StatusNotFound, "source directory not found")
		}
		return newStatusErr(proto.StatusInternal, err.Error())
	}
	if !srcInfo.IsDir() {
		return newStatusErr(proto.StatusNotADir, "source is not a directory")
	}

	// Read original image.
	st, err := os.Stat(imgPath)
	if err != nil {
		if os.IsNotExist(err) {
			return newStatusErr(proto.StatusNotFound, "image not found")
		}
		return newStatusErr(proto.StatusInternal, err.Error())
	}
	perm := st.Mode().Perm()

	orig, err := os.ReadFile(imgPath)
	if err != nil {
		return newStatusErr(proto.StatusInternal, err.Error())
	}
	if int64(len(orig)) < d81BytesNoErrorInfo {
		return newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}

	// Keep the root header template (track 40 sector 0) so disk name/ID stays.
	headerTemplate := make([]byte, 256)
	copy(headerTemplate, d81ReadSector(orig, int(d81DirTrack), 0))

	root, err := buildD81TreeFromImage(orig)
	if err != nil {
		return err
	}

	if err := applyImportDirToD81Tree(root, dstInnerDir, srcDirAbs, allowOverwrite, fallbackPRG, maxFileBytes); err != nil {
		return err
	}

	if err := computeD81RepackTracks(root, d81RepackBufferTracks); err != nil {
		return err
	}

	// Rebuild the image.
	newImg := make([]byte, len(orig))
	copy(newImg, orig)

	base := newImg
	if int64(len(base)) > d81BytesNoErrorInfo {
		base = base[:int(d81BytesNoErrorInfo)]
	}

	if err := formatD81Root(base, headerTemplate); err != nil {
		return err
	}
	rootCtx := d81FSContext{sysTrack: int(d81DirTrack), dirStartT: uint8(d81DirTrack), dirStartS: uint8(d81DirSector)}
	if err := populateD81Dir(base, rootCtx, root, headerTemplate); err != nil {
		return err
	}

	if err := atomicWriteFile(imgPath, newImg, perm); err != nil {
		return newStatusErr(proto.StatusInternal, err.Error())
	}

	// Invalidate parse cache.
	d81Cache.Delete(imgPath)
	return nil
}

func applyImportDirToD81Tree(root *d81TreeDir, dstInnerDir string, srcDirAbs string, allowOverwrite bool, fallbackPRG bool, maxFileBytes uint64) error {
	parts, err := splitD81DirPath(dstInnerDir)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return newStatusErr(proto.StatusBadRequest, "empty destination")
	}

	cur := root
	for i, seg := range parts {
		key := d81NameKey(seg)
		if key == "" {
			return newStatusErr(proto.StatusBadRequest, "invalid directory name")
		}

		isLeaf := i == len(parts)-1
		if !isLeaf {
			if _, ok := cur.Files[key]; ok {
				return newStatusErr(proto.StatusNotADir, "not a directory")
			}
			nxt := cur.Dirs[key]
			if nxt == nil {
				nxt = &d81TreeDir{Name: seg, TypeCode: 5, Files: map[string]*d81TreeFile{}, Dirs: map[string]*d81TreeDir{}}
				cur.Dirs[key] = nxt
			}
			cur = nxt
			continue
		}

		// Leaf: replace/create destination directory.
		if _, ok := cur.Dirs[key]; ok {
			if !allowOverwrite {
				return newStatusErr(proto.StatusAlreadyExists, "destination exists")
			}
		}
		if _, ok := cur.Files[key]; ok {
			if !allowOverwrite {
				return newStatusErr(proto.StatusAlreadyExists, "destination exists")
			}
			delete(cur.Files, key)
		}

		dst := &d81TreeDir{Name: seg, TypeCode: 5, Files: map[string]*d81TreeFile{}, Dirs: map[string]*d81TreeDir{}}
		if err := importFsDirToD81Tree(srcDirAbs, dst, fallbackPRG, maxFileBytes); err != nil {
			return err
		}
		cur.Dirs[key] = dst
	}

	return nil
}

func importFsDirToD81Tree(srcDirAbs string, dst *d81TreeDir, fallbackPRG bool, maxFileBytes uint64) error {
	entries, err := os.ReadDir(srcDirAbs)
	if err != nil {
		return newStatusErr(proto.StatusInternal, err.Error())
	}

	// Deterministic order.
	sort.SliceStable(entries, func(i, j int) bool {
		return strings.ToUpper(entries[i].Name()) < strings.ToUpper(entries[j].Name())
	})

	for _, e := range entries {
		name := e.Name()
		info, err := e.Info()
		if err != nil {
			return newStatusErr(proto.StatusInternal, err.Error())
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return newStatusErr(proto.StatusInvalidPath, "symlink not allowed")
		}

		if info.IsDir() {
			seg, err := sanitizeD64Name(name)
			if err != nil {
				return err
			}
			key := d81NameKey(seg)
			if _, ok := dst.Files[key]; ok {
				return newStatusErr(proto.StatusAlreadyExists, fmt.Sprintf("name conflict: %s", seg))
			}
			if _, ok := dst.Dirs[key]; ok {
				return newStatusErr(proto.StatusAlreadyExists, fmt.Sprintf("name conflict: %s", seg))
			}

			sub := &d81TreeDir{Name: seg, TypeCode: 5, Files: map[string]*d81TreeFile{}, Dirs: map[string]*d81TreeDir{}}
			if err := importFsDirToD81Tree(filepath.Join(srcDirAbs, name), sub, fallbackPRG, maxFileBytes); err != nil {
				return err
			}
			dst.Dirs[key] = sub
			continue
		}

		if !info.Mode().IsRegular() {
			return newStatusErr(proto.StatusBadRequest, "unsupported file type")
		}
		if maxFileBytes > 0 && uint64(info.Size()) > maxFileBytes {
			return newStatusErr(proto.StatusTooLarge, "file too large")
		}

		seg, err := sanitizeD64Name(name)
		if err != nil {
			return err
		}
		if fallbackPRG {
			u := strings.ToUpper(seg)
			if strings.HasSuffix(u, ".PRG") {
				base := strings.TrimSpace(seg[:len(seg)-4])
				if base != "" {
					seg = base
				}
			}
		}

		key := d81NameKey(seg)
		if _, ok := dst.Dirs[key]; ok {
			return newStatusErr(proto.StatusIsADir, "path is a directory")
		}
		if _, ok := dst.Files[key]; ok {
			return newStatusErr(proto.StatusAlreadyExists, fmt.Sprintf("name conflict: %s", seg))
		}

		data, err := os.ReadFile(filepath.Join(srcDirAbs, name))
		if err != nil {
			return newStatusErr(proto.StatusInternal, err.Error())
		}
		dst.Files[key] = &d81TreeFile{Name: seg, TypeCode: 2, Data: data}
	}

	return nil
}
