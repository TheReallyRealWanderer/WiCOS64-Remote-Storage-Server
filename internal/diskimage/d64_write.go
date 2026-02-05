package diskimage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// StatusError is a small helper so callers (server ops) can map disk image
// failures to W64F status codes.
type StatusError struct {
	status byte
	msg    string
}

func (e *StatusError) Error() string {
	if e.msg == "" {
		return fmt.Sprintf("diskimage: status=%d", e.status)
	}
	return e.msg
}

func (e *StatusError) Status() byte { return e.status }

func newStatusErr(st byte, msg string) error {
	return &StatusError{status: st, msg: msg}
}

// sectorsPerTrack returns the number of 256-byte sectors on the given track
// for a 1541-style disk layout. This matches the logic used in d64.go.
func sectorsPerTrack(track int) int {
	switch {
	case track >= 1 && track <= 17:
		return 21
	case track >= 18 && track <= 24:
		return 19
	case track >= 25 && track <= 30:
		return 18
	case track >= 31:
		// Track 31+ (incl. 40/42 track images) use 17 sectors per track.
		return 17
	default:
		return 0
	}
}

// WriteFileRangeD64 writes a chunk of data into a .d64 image file. The image is
// treated like a directory and fileName is the inner file name.
//
// Semantics:
//   - Writes are append-only: offset must match the current file size, except
//     for truncate writes where offset must be 0.
//   - Overwriting an existing file requires truncate=true AND allowOverwrite=true.
//   - Creating a new file requires create=true.
//
// The function updates BAM, directory entry and sector chain.
func WriteFileRangeD64(imgPath string, fileName string, offset uint32, data []byte, truncate bool, create bool, allowOverwrite bool) (uint32, error) {
	if fileName == "" {
		return 0, newStatusErr(proto.StatusBadRequest, "empty inner file name")
	}
	if strings.Contains(fileName, "/") || strings.Contains(fileName, "\\") {
		return 0, newStatusErr(proto.StatusNotSupported, "subdirectories in .d64 are not supported")
	}
	// Wildcards while writing are ambiguous and dangerous.
	if strings.ContainsAny(fileName, "*?") {
		return 0, newStatusErr(proto.StatusBadRequest, "wildcards are not allowed for writes")
	}

	st, err := os.Stat(imgPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, newStatusErr(proto.StatusNotFound, "disk image not found")
		}
		return 0, newStatusErr(proto.StatusInternal, "stat disk image failed")
	}
	if st.IsDir() {
		return 0, newStatusErr(proto.StatusIsADir, "disk image path is a directory")
	}

	sizeBytes, tracks, err := detectD64Layout(st.Size())
	if err != nil {
		return 0, newStatusErr(proto.StatusBadRequest, "unsupported .d64 size")
	}

	f, err := os.OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to open disk image for writing")
	}
	defer f.Close()

	trackOffsets := make([]int64, tracks+1)
	var cum int64
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		cum += int64(sectorsPerTrack(t) * 256)
	}
	if cum != sizeBytes {
		return 0, newStatusErr(proto.StatusInternal, "image layout mismatch")
	}
	sectorOff := func(track, sector int) (int64, error) {
		if track < 1 || track > tracks {
			return 0, fmt.Errorf("invalid track %d", track)
		}
		if sector < 0 || sector >= sectorsPerTrack(track) {
			return 0, fmt.Errorf("invalid sector %d for track %d", sector, track)
		}
		return trackOffsets[track] + int64(sector*256), nil
	}

	readSector := func(track, sector int) ([]byte, error) {
		off, err := sectorOff(track, sector)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 256)
		n, err := f.ReadAt(buf, off)
		if err != nil {
			return nil, err
		}
		if n != 256 {
			return nil, fmt.Errorf("short read: %d", n)
		}
		return buf, nil
	}
	writeSector := func(track, sector int, buf []byte) error {
		if len(buf) != 256 {
			return fmt.Errorf("sector buffer must be 256 bytes")
		}
		off, err := sectorOff(track, sector)
		if err != nil {
			return err
		}
		n, err := f.WriteAt(buf, off)
		if err != nil {
			return err
		}
		if n != 256 {
			return fmt.Errorf("short write: %d", n)
		}
		return nil
	}

	// BAM is always at 18/0 for .d64
	bam, err := readSector(18, 0)
	if err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to read BAM")
	}

	bamTrackBase := func(track int) int { return 4 + (track-1)*4 }
	bamIsFree := func(track, sector int) bool {
		base := bamTrackBase(track)
		if base+3 >= len(bam) {
			return false
		}
		b := bam[base+1+(sector/8)]
		return (b & (1 << uint(sector%8))) != 0
	}
	bamMarkUsed := func(track, sector int) {
		base := bamTrackBase(track)
		bIdx := base + 1 + (sector / 8)
		mask := byte(1 << uint(sector%8))
		if bam[bIdx]&mask != 0 {
			bam[bIdx] &^= mask
			if bam[base] > 0 {
				bam[base]--
			}
		}
	}
	bamMarkFree := func(track, sector int) {
		base := bamTrackBase(track)
		bIdx := base + 1 + (sector / 8)
		mask := byte(1 << uint(sector%8))
		if bam[bIdx]&mask == 0 {
			bam[bIdx] |= mask
			bam[base]++
		}
	}

	type dirSlot struct {
		track  byte
		sector byte
		index  int // 0..7
	}

	normName := strings.ToUpper(strings.TrimSpace(fileName))
	if normName == "" {
		return 0, newStatusErr(proto.StatusBadRequest, "empty inner file name")
	}

	// Find existing entry or a free slot.
	var existing *dirSlot
	var free *dirSlot
	var existingStartT, existingStartS byte
	var lastDirT, lastDirS byte

	dirT := byte(18)
	dirS := byte(1)
	for dirT != 0 {
		sec, err := readSector(int(dirT), int(dirS))
		if err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to read directory sector")
		}
		nextT := sec[0]
		nextS := sec[1]
		lastDirT, lastDirS = dirT, dirS

		for i := 0; i < 8; i++ {
			off := 2 + i*32
			ft := sec[off]
			if ft == 0 {
				if free == nil {
					free = &dirSlot{track: dirT, sector: dirS, index: i}
				}
				continue
			}
			name := petsciiToASCIIName(sec[off+3 : off+19])
			if strings.EqualFold(name, normName) {
				if existing == nil {
					existing = &dirSlot{track: dirT, sector: dirS, index: i}
					existingStartT = sec[off+1]
					existingStartS = sec[off+2]
				}
			}
		}
		dirT, dirS = nextT, nextS
	}

	// Helper to allocate a free data sector.
	allocSector := func() (byte, byte, error) {
		for t := 1; t <= tracks; t++ {
			sp := sectorsPerTrack(t)
			for sct := 0; sct < sp; sct++ {
				if bamIsFree(t, sct) {
					bamMarkUsed(t, sct)
					return byte(t), byte(sct), nil
				}
			}
		}
		return 0, 0, newStatusErr(proto.StatusTooLarge, "disk image full")
	}

	// Extend directory if no free slot is available.
	if existing == nil && free == nil {
		// Allocate a new directory sector on track 18 (preferred).
		sp := sectorsPerTrack(18)
		var newS byte
		found := false
		for sct := 2; sct < sp; sct++ {
			if bamIsFree(18, sct) {
				newS = byte(sct)
				found = true
				break
			}
		}
		if !found {
			return 0, newStatusErr(proto.StatusTooLarge, "no free directory sector")
		}
		bamMarkUsed(18, int(newS))

		// Link last directory sector to the new one.
		if lastDirT == 0 {
			return 0, newStatusErr(proto.StatusInternal, "invalid directory chain")
		}
		lastSec, err := readSector(int(lastDirT), int(lastDirS))
		if err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to read last directory sector")
		}
		lastSec[0] = 18
		lastSec[1] = newS
		if err := writeSector(int(lastDirT), int(lastDirS), lastSec); err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to link new directory sector")
		}

		// Initialise new directory sector.
		newSec := make([]byte, 256)
		newSec[0] = 0
		newSec[1] = 0
		if err := writeSector(18, int(newS), newSec); err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to init new directory sector")
		}

		free = &dirSlot{track: 18, sector: newS, index: 0}
	}

	// Determine target slot and whether we're overwriting.
	slot := existing
	overwriting := false
	if slot == nil {
		slot = free
	} else {
		overwriting = true
	}
	if slot == nil {
		return 0, newStatusErr(proto.StatusTooLarge, "no free directory entry")
	}

	creatingNew := false

	// Writing rules.
	if truncate {
		if offset != 0 {
			return 0, newStatusErr(proto.StatusRangeInvalid, "truncate write must use offset 0")
		}
		if overwriting {
			if !allowOverwrite {
				return 0, newStatusErr(proto.StatusAccessDenied, "overwrite is disabled")
			}
			// Free old chain.
			if existingStartT != 0 {
				t := existingStartT
				s := existingStartS
				visited := map[uint16]bool{}
				for t != 0 {
					key := uint16(t)<<8 | uint16(s)
					if visited[key] {
						break
					}
					visited[key] = true
					sec, err := readSector(int(t), int(s))
					if err != nil {
						return 0, newStatusErr(proto.StatusInternal, "failed to read file chain")
					}
					nextT := sec[0]
					nextS := sec[1]
					bamMarkFree(int(t), int(s))
					if nextT == 0 {
						break
					}
					t, s = nextT, nextS
				}
			}
		} else {
			if !create {
				return 0, newStatusErr(proto.StatusNotFound, "file does not exist")
			}
		}
	} else {
		// Non-truncate: allow existing file (append-only) OR create a new file when CREATE is set.
		if !overwriting {
			if !create {
				return 0, newStatusErr(proto.StatusNotFound, "file does not exist")
			}
			if offset != 0 {
				return 0, newStatusErr(proto.StatusRangeInvalid, "offset must be 0 when creating a new file")
			}
			creatingNew = true
		}
	}

	// Read directory sector for the slot.
	dirSec, err := readSector(int(slot.track), int(slot.sector))
	if err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to read directory sector")
	}
	entOff := 2 + slot.index*32
	startT := dirSec[entOff+1]
	startS := dirSec[entOff+2]

	if truncate || creatingNew {
		// Reset entry (keep name below).
		for i := 0; i < 30; i++ {
			dirSec[entOff+i] = 0
		}
		startT, startS = 0, 0
	}

	// Determine current size/last sector info.
	curSize := uint32(0)
	blocks := uint16(0)
	lastT, lastS := startT, startS
	lastLen := 0 // data bytes in last sector

	if startT != 0 {
		t := startT
		s := startS
		visited := map[uint16]bool{}
		for t != 0 {
			key := uint16(t)<<8 | uint16(s)
			if visited[key] {
				break
			}
			visited[key] = true
			blocks++
			sec, err := readSector(int(t), int(s))
			if err != nil {
				return 0, newStatusErr(proto.StatusInternal, "failed to read file sector")
			}
			nextT := sec[0]
			nextS := sec[1]
			if nextT == 0 {
				dl := int(nextS)
				if dl <= 0 {
					dl = dataBytesPerSector
				}
				if dl > dataBytesPerSector {
					dl = dataBytesPerSector
				}
				curSize += uint32(dl)
				lastT, lastS = t, s
				lastLen = dl
				break
			}
			curSize += uint32(dataBytesPerSector)
			lastT, lastS = t, s
			t, s = nextT, nextS
		}
	}

	// Overwrite protection: if a file exists and the client starts writing at offset 0,
	// require an explicit truncate flag.
	if overwriting && !truncate && offset == 0 && curSize > 0 && len(data) > 0 {
		return 0, newStatusErr(proto.StatusAlreadyExists, "overwrite requires truncate flag")
	}

	if !truncate {
		// Enforce append-only writes.
		if offset != curSize {
			return 0, newStatusErr(proto.StatusRangeInvalid, fmt.Sprintf("offset %d does not match current size %d", offset, curSize))
		}
	}

	// Append data.
	startTrack := startT
	startSector := startS
	curBlocks := blocks
	curLastT := lastT
	curLastS := lastS
	curLastLen := lastLen

	// Empty file: allocate first sector if we have something to write.
	if startTrack == 0 && len(data) > 0 {
		t, s, err := allocSector()
		if err != nil {
			return 0, err
		}
		startTrack, startSector = t, s
		curLastT, curLastS = t, s
		curLastLen = 0
		curBlocks = 1
		// Initialise new sector.
		if err := writeSector(int(t), int(s), make([]byte, 256)); err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to init first file sector")
		}
	}

	remaining := data
	for len(remaining) > 0 {
		if curLastT == 0 {
			// This can happen if someone tries to write into a 0-byte file entry.
			t, s, err := allocSector()
			if err != nil {
				return 0, err
			}
			startTrack, startSector = t, s
			curLastT, curLastS = t, s
			curLastLen = 0
			curBlocks = 1
			if err := writeSector(int(t), int(s), make([]byte, 256)); err != nil {
				return 0, newStatusErr(proto.StatusInternal, "failed to init file sector")
			}
		}

		sec, err := readSector(int(curLastT), int(curLastS))
		if err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to read last sector")
		}

		avail := dataBytesPerSector - curLastLen
		if avail > 0 {
			n := len(remaining)
			if n > avail {
				n = avail
			}
			copy(sec[2+curLastLen:2+curLastLen+n], remaining[:n])
			curLastLen += n
			remaining = remaining[n:]
		}

		if len(remaining) == 0 {
			// Final sector.
			sec[0] = 0
			if curLastLen >= dataBytesPerSector {
				sec[1] = 0
			} else {
				sec[1] = byte(curLastLen)
			}
			if err := writeSector(int(curLastT), int(curLastS), sec); err != nil {
				return 0, newStatusErr(proto.StatusInternal, "failed to write last sector")
			}
			break
		}

		// Need a new sector.
		if curLastLen < dataBytesPerSector {
			// Should not happen because remaining != 0 implies sector is full.
			// But be safe.
			continue
		}
		newT, newS, err := allocSector()
		if err != nil {
			return 0, err
		}
		// Link current sector to the new one.
		sec[0] = newT
		sec[1] = newS
		if err := writeSector(int(curLastT), int(curLastS), sec); err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to link sectors")
		}

		// Init new sector and continue.
		if err := writeSector(int(newT), int(newS), make([]byte, 256)); err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to init new sector")
		}
		curLastT, curLastS = newT, newS
		curLastLen = 0
		curBlocks++
	}

	// Update directory entry.
	nameBytes := make([]byte, 16)
	for i := range nameBytes {
		nameBytes[i] = 0xA0
	}
	// Directory names are stored in PETSCII, but for our purposes an
	// ASCII-to-PETSCII-lite mapping works fine.
	up := strings.ToUpper(fileName)
	up = strings.ReplaceAll(up, "\\", "_")
	up = strings.ReplaceAll(up, "/", "_")
	src := []byte(up)
	for i := 0; i < len(src) && i < 16; i++ {
		b := src[i]
		switch {
		case b == ' ':
			nameBytes[i] = 0xA0
		case b < 0x20 || b > 0x7e:
			nameBytes[i] = '_'
		default:
			nameBytes[i] = b
		}
	}

	// 0x82: closed PRG.
	dirSec[entOff+0] = 0x82
	dirSec[entOff+1] = startTrack
	dirSec[entOff+2] = startSector
	copy(dirSec[entOff+3:entOff+19], nameBytes)
	binary.LittleEndian.PutUint16(dirSec[entOff+28:entOff+30], curBlocks)
	if err := writeSector(int(slot.track), int(slot.sector), dirSec); err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to write directory entry")
	}

	// Write BAM back.
	if err := writeSector(18, 0, bam); err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to write BAM")
	}
	if err := f.Sync(); err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to sync image")
	}

	d64Cache.Delete(imgPath)
	return uint32(len(data)), nil
}
