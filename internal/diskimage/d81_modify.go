package diskimage

import (
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// DeleteFileD81 deletes a regular file from a .D81 image (1581).
//
// Supports nested "subdirectories" (which are 1581 partitions) via paths like "UTILS/FILE".
func DeleteFileD81(imgPath, innerPath string) error {
	if strings.ContainsAny(innerPath, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards are not allowed")
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

	// Work on a copy so we can safely preserve trailing error-info bytes (if present).
	img := make([]byte, len(origImg))
	copy(img, origImg)

	dirParts, leaf, err := splitD81InnerPath(innerPath)
	if err != nil {
		return err
	}
	ctx, err := resolveD81WriteContext(img, dirParts)
	if err != nil {
		return err
	}

	loc, _, _, err := findD81DirSlot(img, ctx, leaf)
	if err != nil {
		return err
	}
	if !loc.found {
		return newStatusErr(proto.StatusNotFound, "not found")
	}
	// Guard: do not delete directories/partitions.
	if loc.entryTypeCode == 5 || loc.entryTypeCode == 6 {
		return newStatusErr(proto.StatusIsADir, "is a directory")
	}

	b, err := newD81BAMAt(img, ctx.sysTrack)
	if err != nil {
		return err
	}

	if err := deleteD81FileChain(img, b, loc.startT, loc.startS); err != nil {
		return err
	}
	clearD81DirSlot(img, loc)

	if err := atomicWriteFile(imgPath, img, perm); err != nil {
		return newStatusErr(proto.StatusInternal, "failed to write image")
	}
	d81Cache.Delete(imgPath)
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func normalizeD81TypeCode(typeCode uint8) uint8 {
	// Only regular files should be moved.
	if typeCode >= 1 && typeCode <= 4 {
		return typeCode
	}
	return 2 // PRG fallback
}

// moveFileD81AcrossPartitionsBytes moves a regular file across partitions/root in-place.
//
// This requires the destination filesystem to have enough free space to allocate the new
// copy. If the destination runs out of space, callers may choose to fallback to repacking.
func moveFileD81AcrossPartitionsBytes(img []byte, oldDir []string, oldLeaf string, newDir []string, newLeaf string, allowOverwrite bool) error {
	srcCtx, err := resolveD81WriteContext(img, oldDir)
	if err != nil {
		return err
	}
	dstCtx, err := resolveD81WriteContext(img, newDir)
	if err != nil {
		return err
	}

	srcLoc, _, _, err := findD81DirSlot(img, srcCtx, oldLeaf)
	if err != nil {
		return err
	}
	if !srcLoc.found {
		return newStatusErr(proto.StatusNotFound, "not found")
	}
	if srcLoc.entryTypeCode == 5 || srcLoc.entryTypeCode == 6 {
		return newStatusErr(proto.StatusIsADir, "is a directory")
	}
	srcType := normalizeD81TypeCode(srcLoc.entryTypeCode)

	srcData, err := readD81FileData(img, srcLoc.startT, srcLoc.startS)
	if err != nil {
		return err
	}

	// Destination lookup.
	dstLoc, freeLoc, lastDirTS, err := findD81DirSlot(img, dstCtx, newLeaf)
	if err != nil {
		return err
	}
	// Destination BAM.
	bDst, err := newD81BAMAt(img, dstCtx.sysTrack)
	if err != nil {
		return err
	}

	if dstLoc.found {
		if dstLoc.entryTypeCode == 5 || dstLoc.entryTypeCode == 6 {
			return newStatusErr(proto.StatusIsADir, "destination is a directory")
		}
		if !allowOverwrite {
			return newStatusErr(proto.StatusAlreadyExists, "destination exists")
		}
		if err := deleteD81FileChain(img, bDst, dstLoc.startT, dstLoc.startS); err != nil {
			return err
		}
		clearD81DirSlot(img, dstLoc)
		freeLoc = dstLoc
		freeLoc.found = true
	}

	// Ensure we have a free directory slot in the destination.
	if !freeLoc.found {
		newS, err := allocD81DirSector(img, bDst, dstCtx.sysTrack)
		if err != nil {
			return err
		}
		// Link the old last sector to the new one.
		lastSec := d81ReadSector(img, int(lastDirTS.T), int(lastDirTS.S))
		lastSec[0] = uint8(dstCtx.sysTrack)
		lastSec[1] = newS
		freeLoc = d81DirSlotLoc{
			found:   true,
			slotOff: d81SectorOffset(dstCtx.sysTrack, int(newS)),
			slotIdx: 0,
		}
	}

	// Write the file data into the destination filesystem.
	firstT, firstS, blocks, err := writeNewD81File(img, bDst, srcData)
	if err != nil {
		return err
	}
	writeD81DirEntry(img, freeLoc, newLeaf, firstT, firstS, blocks, srcType)

	// Delete the source file.
	bSrc, err := newD81BAMAt(img, srcCtx.sysTrack)
	if err != nil {
		return err
	}
	if err := deleteD81FileChain(img, bSrc, srcLoc.startT, srcLoc.startS); err != nil {
		return err
	}
	clearD81DirSlot(img, srcLoc)

	return nil
}

// RenameFileD81 renames a regular file inside a .D81 image (1581).
//
// Supports nested partition paths:
//   - rename within the same partition: in-place directory entry update
//   - move across partitions: copy+delete inside the image (with repack fallback on "disk full")
func RenameFileD81(imgPath, oldPath, newPath string, allowOverwrite bool) error {
	if strings.ContainsAny(oldPath, "*?") || strings.ContainsAny(newPath, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards are not allowed")
	}

	oldDir, oldLeaf, err := splitD81InnerPath(oldPath)
	if err != nil {
		return err
	}
	newDir, newLeaf, err := splitD81InnerPath(newPath)
	if err != nil {
		return err
	}

	// No-op if the full normalized path is unchanged.
	if oldLeaf == newLeaf && equalStringSlices(oldDir, newDir) {
		return nil
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

	// Fast path: rename within the same directory/partition.
	if equalStringSlices(oldDir, newDir) {
		ctx, err := resolveD81WriteContext(img, oldDir)
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
		if loc.entryTypeCode == 5 || loc.entryTypeCode == 6 {
			return newStatusErr(proto.StatusIsADir, "is a directory")
		}

		// Check destination collision inside same directory.
		loc2, _, _, err := findD81DirSlot(img, ctx, newLeaf)
		if err != nil {
			return err
		}
		if loc2.found && loc2.slotOff != loc.slotOff {
			if loc2.entryTypeCode == 5 || loc2.entryTypeCode == 6 {
				return newStatusErr(proto.StatusIsADir, "destination is a directory")
			}
			if !allowOverwrite {
				return newStatusErr(proto.StatusAlreadyExists, "destination exists")
			}

			b, err := newD81BAMAt(img, ctx.sysTrack)
			if err != nil {
				return err
			}
			if err := deleteD81FileChain(img, b, loc2.startT, loc2.startS); err != nil {
				return err
			}
			clearD81DirSlot(img, loc2)
		}

		// Update filename bytes (slot[5..20]).
		slot := img[loc.slotOff : loc.slotOff+32]
		for i := 0; i < 16; i++ {
			slot[5+i] = 0xA0
		}
		nb := []byte(newLeaf)
		if len(nb) > 16 {
			nb = nb[:16]
		}
		copy(slot[5:21], nb)

		if err := atomicWriteFile(imgPath, img, perm); err != nil {
			return newStatusErr(proto.StatusInternal, "failed to write image")
		}
		d81Cache.Delete(imgPath)
		return nil
	}

	// Cross-partition move.
	if err := moveFileD81AcrossPartitionsBytes(img, oldDir, oldLeaf, newDir, newLeaf, allowOverwrite); err != nil {
		if isDiskFullStatus(err) {
			// Repack-based move can resize partitions to make room.
			return repackD81ForMove(imgPath, origImg, oldPath, newPath, allowOverwrite, perm)
		}
		return err
	}

	if err := atomicWriteFile(imgPath, img, perm); err != nil {
		return newStatusErr(proto.StatusInternal, "failed to write image")
	}
	d81Cache.Delete(imgPath)
	return nil
}
