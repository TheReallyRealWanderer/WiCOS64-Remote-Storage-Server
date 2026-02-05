package diskimage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// DeleteFileD71 removes a file from a .d71 (1571) disk image and frees its blocks.
// Root directory only.
func DeleteFileD71(imgPath, fileName string) error {
	normName, err := sanitizeD64Name(fileName)
	if err != nil {
		return err
	}

	img, err := NewD71(imgPath)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	sectorOff := img.sectorOff
	tracks := img.Tracks

	sectorsPerTrack := func(track int) int {
		t := track
		if t > 35 {
			t -= 35
		}
		switch {
		case t >= 1 && t <= 17:
			return 21
		case t >= 18 && t <= 24:
			return 19
		case t >= 25 && t <= 30:
			return 18
		case t >= 31 && t <= 35:
			return 17
		default:
			return 0
		}
	}

	readSector := func(track, sector int) ([]byte, error) {
		off := sectorOff(track, sector)
		if off < 0 {
			return nil, newStatusErr(proto.StatusBadPath, "bad sector")
		}
		buf := make([]byte, 256)
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, err
		}
		return buf, nil
	}

	writeSector := func(track, sector int, buf []byte) error {
		if len(buf) != 256 {
			return fmt.Errorf("sector buffer must be 256 bytes")
		}
		off := sectorOff(track, sector)
		if off < 0 {
			return newStatusErr(proto.StatusBadPath, "bad sector")
		}
		_, err := f.WriteAt(buf, off)
		return err
	}

	bam0, err := readSector(18, 0)
	if err != nil {
		return err
	}
	bam1, err := readSector(53, 0)
	if err != nil {
		return err
	}

	doubleSided := (bam0[3] & 0x80) != 0

	bamMeta := func(track int) (*byte, []byte, error) {
		if track < 1 || track > tracks {
			return nil, nil, newStatusErr(proto.StatusBadPath, "bad track")
		}
		if track <= 35 {
			off := 4 + (track-1)*4
			return &bam0[off], bam0[off+1 : off+4], nil
		}
		idx := track - 36 // 0..34
		fcOff := 0xDD + idx
		bmOff := idx * 3
		if fcOff < 0 || fcOff >= len(bam0) || bmOff < 0 || bmOff+3 > len(bam1) {
			return nil, nil, newStatusErr(proto.StatusInternal, "bam layout out of range")
		}
		return &bam0[fcOff], bam1[bmOff : bmOff+3], nil
	}

	bamMarkFree := func(track, sector int) {
		if !doubleSided && track > 35 {
			return
		}
		fc, bm, berr := bamMeta(track)
		if berr != nil {
			return
		}
		if sector < 0 || sector >= sectorsPerTrack(track) {
			return
		}
		if sector >= 24 {
			return
		}
		byteIndex := sector >> 3
		mask := byte(1 << (sector & 7))
		if (bm[byteIndex] & mask) == 0 {
			bm[byteIndex] |= mask
			if fc != nil {
				*fc = *fc + 1
			}
		}
	}

	// Find directory entry.
	var entryTrack, entrySector, entryOff int
	var startT, startS int
	found := false
	curT, curS := 18, 1
	for curT != 0 {
		sec, err := readSector(curT, curS)
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
			nameASCII := petsciiToASCIIName(sec[off+3 : off+19])
			if strings.EqualFold(nameASCII, normName) {
				entryTrack, entrySector, entryOff = curT, curS, off
				startT, startS = int(sec[off+1]), int(sec[off+2])
				found = true
				break
			}
		}
		if found {
			break
		}
		curT, curS = nextT, nextS
	}
	if !found {
		return newStatusErr(proto.StatusNotFound, "not found")
	}

	// Free chain.
	visited := map[[2]int]bool{}
	t, sct := startT, startS
	for t != 0 {
		if t < 1 || t > tracks {
			return newStatusErr(proto.StatusBadRequest, "bad file chain")
		}
		if sct < 0 || sct >= sectorsPerTrack(t) {
			return newStatusErr(proto.StatusBadRequest, "bad file chain")
		}
		key := [2]int{t, sct}
		if visited[key] {
			return newStatusErr(proto.StatusBadRequest, "file chain loop")
		}
		visited[key] = true
		buf, err := readSector(t, sct)
		if err != nil {
			return err
		}
		nextT, nextS := int(buf[0]), int(buf[1])
		bamMarkFree(t, sct)
		t, sct = nextT, nextS
	}

	// Clear directory entry.
	sec, err := readSector(entryTrack, entrySector)
	if err != nil {
		return err
	}
	copy(sec[entryOff:entryOff+30], bytes.Repeat([]byte{0x00}, 30))
	if err := writeSector(entryTrack, entrySector, sec); err != nil {
		return err
	}

	// Persist BAMs.
	if err := writeSector(18, 0, bam0); err != nil {
		return err
	}
	if err := writeSector(53, 0, bam1); err != nil {
		return err
	}

	if err := f.Sync(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		// ignore
	}

	d71Cache.Delete(imgPath)
	return nil
}

// RenameFileD71 renames a file inside a .d71 (1571) disk image.
// Root directory only.
func RenameFileD71(imgPath, oldName, newName string, allowOverwrite bool) error {
	oldNorm, err := sanitizeD64Name(oldName)
	if err != nil {
		return err
	}
	newNorm, err := sanitizeD64Name(newName)
	if err != nil {
		return err
	}
	if strings.EqualFold(oldNorm, newNorm) {
		return nil
	}

	img, err := NewD71(imgPath)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	sectorOff := img.sectorOff

	readSector := func(track, sector int) ([]byte, error) {
		off := sectorOff(track, sector)
		if off < 0 {
			return nil, newStatusErr(proto.StatusBadPath, "bad sector")
		}
		buf := make([]byte, 256)
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, err
		}
		return buf, nil
	}

	writeSector := func(track, sector int, buf []byte) error {
		if len(buf) != 256 {
			return fmt.Errorf("sector buffer must be 256 bytes")
		}
		off := sectorOff(track, sector)
		if off < 0 {
			return newStatusErr(proto.StatusBadPath, "bad sector")
		}
		_, err := f.WriteAt(buf, off)
		return err
	}

	// Find source and destination entries.
	var srcT, srcS, srcOff int
	var dstT, dstS, dstOff int
	var srcFound, dstFound bool

	curT, curS := 18, 1
	for curT != 0 {
		sec, err := readSector(curT, curS)
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
			nameASCII := petsciiToASCIIName(sec[off+3 : off+19])
			if !srcFound && strings.EqualFold(nameASCII, oldNorm) {
				srcT, srcS, srcOff = curT, curS, off
				srcFound = true
			}
			if !dstFound && strings.EqualFold(nameASCII, newNorm) {
				dstT, dstS, dstOff = curT, curS, off
				dstFound = true
			}
		}
		if srcFound && dstFound {
			break
		}
		curT, curS = nextT, nextS
	}
	if !srcFound {
		return newStatusErr(proto.StatusNotFound, "not found")
	}

	if dstFound {
		if !allowOverwrite {
			return newStatusErr(proto.StatusAlreadyExists, "destination exists")
		}
		// Remove destination first.
		if err := DeleteFileD71(imgPath, newNorm); err != nil {
			return err
		}
		// Re-open (DeleteFileD71 invalidates cache and closes file handle).
		f2, err := os.OpenFile(imgPath, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		defer f2.Close()
		f = f2
		// Need to re-bind read/write closures to the new file.
		readSector = func(track, sector int) ([]byte, error) {
			off := sectorOff(track, sector)
			if off < 0 {
				return nil, newStatusErr(proto.StatusBadPath, "bad sector")
			}
			buf := make([]byte, 256)
			if _, err := f.ReadAt(buf, off); err != nil {
				return nil, err
			}
			return buf, nil
		}
		writeSector = func(track, sector int, buf []byte) error {
			if len(buf) != 256 {
				return fmt.Errorf("sector buffer must be 256 bytes")
			}
			off := sectorOff(track, sector)
			if off < 0 {
				return newStatusErr(proto.StatusBadPath, "bad sector")
			}
			_, err := f.WriteAt(buf, off)
			return err
		}
		// Destination entry coordinates are no longer valid after delete, so we only
		// keep renaming the source entry below.
		_ = dstT
		_ = dstS
		_ = dstOff
	}

	// Rename source entry in-place.
	sec, err := readSector(srcT, srcS)
	if err != nil {
		return err
	}
	copy(sec[srcOff+3:srcOff+19], encodeD64Name16(newNorm))
	if err := writeSector(srcT, srcS, sec); err != nil {
		return err
	}

	if err := f.Sync(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		// ignore
	}

	d71Cache.Delete(imgPath)
	return nil
}
