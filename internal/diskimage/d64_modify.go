package diskimage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// DeleteFileD64 deletes a file in the root directory of a D64 image.
//
// D64 has no subdirectories; fileName must not contain '/'. The sector chain is
// freed in the BAM and the directory entry is cleared.
func DeleteFileD64(imgPath string, fileName string) error {
	nameKey := strings.ToUpper(strings.TrimSpace(fileName))
	if nameKey == "" {
		return newStatusErr(proto.StatusBadRequest, "empty filename")
	}
	if strings.Contains(nameKey, "/") {
		return newStatusErr(proto.StatusNotSupported, "subdirectories are not supported in D64")
	}
	if strings.ContainsAny(nameKey, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards are not supported for delete")
	}

	fi, err := os.Stat(imgPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return newStatusErr(proto.StatusNotFound, "disk image not found")
		}
		return fmt.Errorf("stat %s: %w", imgPath, err)
	}
	if fi.IsDir() {
		return newStatusErr(proto.StatusIsADir, "disk image is a directory")
	}

	sizeBytes, tracks, err := detectD64Layout(fi.Size())
	if err != nil {
		return newStatusErr(proto.StatusBadRequest, err.Error())
	}

	// Track offsets
	trackOffsets := make([]int64, tracks+1)
	cum := int64(0)
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		cum += int64(sectorsPerTrack(t)) * 256
	}
	if cum != sizeBytes {
		return newStatusErr(proto.StatusBadRequest, "disk image size mismatch")
	}

	sectorOff := func(track, sector int) (int64, error) {
		if track < 1 || track > tracks {
			return 0, newStatusErr(proto.StatusBadRequest, "track out of range")
		}
		sp := sectorsPerTrack(track)
		if sector < 0 || sector >= sp {
			return 0, newStatusErr(proto.StatusBadRequest, "sector out of range")
		}
		return trackOffsets[track] + int64(sector)*256, nil
	}

	f, err := os.OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", imgPath, err)
	}
	defer f.Close()

	readSector := func(track, sector int) ([]byte, error) {
		off, err := sectorOff(track, sector)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 256)
		if _, err := f.ReadAt(buf, off); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, newStatusErr(proto.StatusInvalidPath, "unexpected EOF reading sector")
			}
			return nil, err
		}
		return buf, nil
	}

	writeSector := func(track, sector int, data []byte) error {
		if len(data) != 256 {
			return newStatusErr(proto.StatusInternal, "sector write length mismatch")
		}
		off, err := sectorOff(track, sector)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(data, off); err != nil {
			return err
		}
		return nil
	}

	// Load BAM
	bam, err := readSector(18, 0)
	if err != nil {
		return err
	}

	bamOffset := func(track int) (int, error) {
		// BAM layout starts at 0x04; 4 bytes per track.
		idx := 0x04 + (track-1)*4
		if idx < 0x04 || idx+3 >= 256 {
			return 0, newStatusErr(proto.StatusBadRequest, "BAM offset out of range")
		}
		return idx, nil
	}

	bamMarkFree := func(track, sector int) error {
		idx, err := bamOffset(track)
		if err != nil {
			return err
		}
		// bytes: [freeCount][bitmap0][bitmap1][bitmap2]
		bit := uint(sector)
		byteIdx := int(bit / 8)
		bitIdx := bit % 8
		maskPos := idx + 1 + byteIdx
		if maskPos > idx+3 {
			return newStatusErr(proto.StatusBadRequest, "sector bitmap out of range")
		}
		mask := bam[maskPos]
		alreadyFree := (mask & (1 << bitIdx)) != 0
		if alreadyFree {
			return nil
		}
		bam[maskPos] = mask | (1 << bitIdx)
		if bam[idx] < 0xFF {
			bam[idx]++
		}
		return nil
	}

	// Locate directory entry
	dirTrack, dirSector := 18, 1
	var foundSec []byte
	foundTrack, foundSector := 0, 0
	foundEntOff := 0
	var startT, startS byte

	for {
		sec, err := readSector(dirTrack, dirSector)
		if err != nil {
			return err
		}
		nextT, nextS := int(sec[0]), int(sec[1])
		for i := 0; i < 8; i++ {
			off := 2 + i*32
			ft := sec[off]
			if ft == 0x00 {
				continue
			}
			name := strings.ToUpper(petsciiToASCIIName(sec[off+3 : off+19]))
			if name == nameKey {
				foundSec = sec
				foundTrack, foundSector = dirTrack, dirSector
				foundEntOff = off
				startT = sec[off+1]
				startS = sec[off+2]
				break
			}
		}
		if foundSec != nil {
			break
		}
		if nextT == 0 {
			break
		}
		dirTrack, dirSector = nextT, nextS
	}

	if foundSec == nil {
		return newStatusErr(proto.StatusNotFound, "file not found in image")
	}

	// Free sector chain
	if startT != 0 {
		seen := make(map[uint16]bool)
		t, s := int(startT), int(startS)
		for t != 0 {
			if t < 1 || t > tracks {
				return newStatusErr(proto.StatusInvalidPath, "invalid sector chain")
			}
			if s < 0 || s >= sectorsPerTrack(t) {
				return newStatusErr(proto.StatusInvalidPath, "invalid sector chain")
			}
			key := uint16(t)<<8 | uint16(s)
			if seen[key] {
				return newStatusErr(proto.StatusInvalidPath, "loop in sector chain")
			}
			seen[key] = true

			sec, err := readSector(t, s)
			if err != nil {
				return err
			}
			nextT, nextS := int(sec[0]), int(sec[1])
			if err := bamMarkFree(t, s); err != nil {
				return err
			}
			t, s = nextT, nextS
		}
	}

	// Clear directory entry
	for i := 0; i < 30; i++ {
		foundSec[foundEntOff+i] = 0x00
	}

	// Write back directory + BAM
	if err := writeSector(18, 0, bam); err != nil {
		return err
	}
	if err := writeSector(foundTrack, foundSector, foundSec); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	d64Cache.Delete(imgPath)
	return nil
}

// RenameFileD64 renames a file in the root directory of a D64 image.
//
// If allowOverwrite is true and the destination exists, the destination file
// will be deleted first.
func RenameFileD64(imgPath string, oldName string, newName string, allowOverwrite bool) error {
	srcKey := strings.ToUpper(strings.TrimSpace(oldName))
	dstKey := strings.ToUpper(strings.TrimSpace(newName))
	if srcKey == "" || dstKey == "" {
		return newStatusErr(proto.StatusBadRequest, "empty filename")
	}
	if strings.Contains(srcKey, "/") || strings.Contains(dstKey, "/") {
		return newStatusErr(proto.StatusNotSupported, "subdirectories are not supported in D64")
	}
	if strings.ContainsAny(srcKey, "*?") || strings.ContainsAny(dstKey, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards are not supported for rename")
	}
	if srcKey == dstKey {
		return nil
	}

	fi, err := os.Stat(imgPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return newStatusErr(proto.StatusNotFound, "disk image not found")
		}
		return fmt.Errorf("stat %s: %w", imgPath, err)
	}
	if fi.IsDir() {
		return newStatusErr(proto.StatusIsADir, "disk image is a directory")
	}

	sizeBytes, tracks, err := detectD64Layout(fi.Size())
	if err != nil {
		return newStatusErr(proto.StatusBadRequest, err.Error())
	}

	trackOffsets := make([]int64, tracks+1)
	cum := int64(0)
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		cum += int64(sectorsPerTrack(t)) * 256
	}
	if cum != sizeBytes {
		return newStatusErr(proto.StatusBadRequest, "disk image size mismatch")
	}

	sectorOff := func(track, sector int) (int64, error) {
		if track < 1 || track > tracks {
			return 0, newStatusErr(proto.StatusBadRequest, "track out of range")
		}
		sp := sectorsPerTrack(track)
		if sector < 0 || sector >= sp {
			return 0, newStatusErr(proto.StatusBadRequest, "sector out of range")
		}
		return trackOffsets[track] + int64(sector)*256, nil
	}

	f, err := os.OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", imgPath, err)
	}
	defer f.Close()

	readSector := func(track, sector int) ([]byte, error) {
		off, err := sectorOff(track, sector)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 256)
		if _, err := f.ReadAt(buf, off); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, newStatusErr(proto.StatusInvalidPath, "unexpected EOF reading sector")
			}
			return nil, err
		}
		return buf, nil
	}

	writeSector := func(track, sector int, data []byte) error {
		if len(data) != 256 {
			return newStatusErr(proto.StatusInternal, "sector write length mismatch")
		}
		off, err := sectorOff(track, sector)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(data, off); err != nil {
			return err
		}
		return nil
	}

	bam, err := readSector(18, 0)
	if err != nil {
		return err
	}

	bamOffset := func(track int) (int, error) {
		idx := 0x04 + (track-1)*4
		if idx < 0x04 || idx+3 >= 256 {
			return 0, newStatusErr(proto.StatusBadRequest, "BAM offset out of range")
		}
		return idx, nil
	}

	bamMarkFree := func(track, sector int) error {
		idx, err := bamOffset(track)
		if err != nil {
			return err
		}
		bit := uint(sector)
		byteIdx := int(bit / 8)
		bitIdx := bit % 8
		maskPos := idx + 1 + byteIdx
		if maskPos > idx+3 {
			return newStatusErr(proto.StatusBadRequest, "sector bitmap out of range")
		}
		mask := bam[maskPos]
		alreadyFree := (mask & (1 << bitIdx)) != 0
		if alreadyFree {
			return nil
		}
		bam[maskPos] = mask | (1 << bitIdx)
		if bam[idx] < 0xFF {
			bam[idx]++
		}
		return nil
	}

	type entryLoc struct {
		track  int
		sector int
		sec    []byte
		off    int
		startT byte
		startS byte
	}

	var src, dst *entryLoc

	dirTrack, dirSector := 18, 1
	for {
		sec, err := readSector(dirTrack, dirSector)
		if err != nil {
			return err
		}
		nextT, nextS := int(sec[0]), int(sec[1])

		for i := 0; i < 8; i++ {
			off := 2 + i*32
			ft := sec[off]
			if ft == 0x00 {
				continue
			}
			name := strings.ToUpper(petsciiToASCIIName(sec[off+3 : off+19]))
			if src == nil && name == srcKey {
				src = &entryLoc{track: dirTrack, sector: dirSector, sec: sec, off: off, startT: sec[off+1], startS: sec[off+2]}
			}
			if dst == nil && name == dstKey {
				dst = &entryLoc{track: dirTrack, sector: dirSector, sec: sec, off: off, startT: sec[off+1], startS: sec[off+2]}
			}
		}

		if nextT == 0 {
			break
		}
		dirTrack, dirSector = nextT, nextS
	}

	if src == nil {
		return newStatusErr(proto.StatusNotFound, "source file not found in image")
	}

	bamDirty := false
	dstDirty := false

	// If destination exists, handle overwrite.
	if dst != nil {
		if !allowOverwrite {
			return newStatusErr(proto.StatusAlreadyExists, "destination exists")
		}
		// Delete destination file first.
		if dst.startT != 0 {
			seen := make(map[uint16]bool)
			t, s := int(dst.startT), int(dst.startS)
			for t != 0 {
				if t < 1 || t > tracks {
					return newStatusErr(proto.StatusInvalidPath, "invalid sector chain")
				}
				if s < 0 || s >= sectorsPerTrack(t) {
					return newStatusErr(proto.StatusInvalidPath, "invalid sector chain")
				}
				key := uint16(t)<<8 | uint16(s)
				if seen[key] {
					return newStatusErr(proto.StatusInvalidPath, "loop in sector chain")
				}
				seen[key] = true

				sec, err := readSector(t, s)
				if err != nil {
					return err
				}
				nextT, nextS := int(sec[0]), int(sec[1])
				if err := bamMarkFree(t, s); err != nil {
					return err
				}
				bamDirty = true
				t, s = nextT, nextS
			}
		}
		for i := 0; i < 30; i++ {
			dst.sec[dst.off+i] = 0x00
		}
		dstDirty = true
	}

	// Update source name.
	nameBytes := encodeD64Name16(dstKey)
	copy(src.sec[src.off+3:src.off+19], nameBytes)

	// Write back modified sectors.
	if bamDirty {
		if err := writeSector(18, 0, bam); err != nil {
			return err
		}
	}
	if dstDirty {
		if err := writeSector(dst.track, dst.sector, dst.sec); err != nil {
			return err
		}
	}
	if err := writeSector(src.track, src.sector, src.sec); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	d64Cache.Delete(imgPath)
	return nil
}

func encodeD64Name16(name string) []byte {
	n := strings.ToUpper(strings.TrimSpace(name))
	b := make([]byte, 16)
	for i := range b {
		b[i] = 0xA0
	}
	for i := 0; i < len(n) && i < 16; i++ {
		c := n[i]
		if c == '_' || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b[i] = c
		} else {
			b[i] = '_'
		}
	}
	return b
}
