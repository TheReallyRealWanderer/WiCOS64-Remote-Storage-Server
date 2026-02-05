package diskimage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// WriteFileRangeD71 writes data into a file stored inside a .d71 (1571) disk image.
//
// Behaviour is intentionally similar to WriteFileRangeD64:
//   - Root directory only (no sub-directories in D71).
//   - If create==false and the file does not exist: StatusNotFound.
//   - If create==true and truncate==true, an existing file will be replaced
//     only if allowOverwrite==true.
//   - If truncate==false, data is written starting at offset (file grows if needed).
//
// Returned value is the number of bytes written.
func WriteFileRangeD71(imgPath, fileName string, offset uint32, data []byte, truncate, create, allowOverwrite bool) (uint32, error) {
	normName, err := sanitizeD64Name(fileName)
	if err != nil {
		return 0, err
	}
	namePetscii := encodeD64Name16(normName)

	img, err := NewD71(imgPath)
	if err != nil {
		return 0, err
	}

	f, err := os.OpenFile(imgPath, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sectorOff := img.sectorOff
	tracks := img.Tracks // should be 70

	// 1541-style sectors-per-track for the given logical track.
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
			return nil, newStatusErr(proto.StatusBadPath, "invalid track/sector")
		}
		buf := make([]byte, 256)
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, err
		}
		return buf, nil
	}

	writeSector := func(track, sector int, buf []byte) error {
		if len(buf) != 256 {
			return fmt.Errorf("sector write requires 256 bytes")
		}
		off := sectorOff(track, sector)
		if off < 0 {
			return newStatusErr(proto.StatusBadPath, "invalid track/sector")
		}
		_, err := f.WriteAt(buf, off)
		return err
	}

	// Load BAM sectors (18/0 + 53/0).
	//
	// D71 BAM layout (1571):
	// - Track 18/0 contains the normal 35*4 BAM entries for tracks 1-35.
	// - Track 18/0 also contains the *free sector counts* for tracks 36-70 at $DD-$FF.
	// - Track 53/0 contains the allocation bitmaps for tracks 36-70: 3 bytes per track starting at $00.
	// - Byte $03 in 18/0 has bit 7 set (0x80) for double-sided disks.
	bam0, err := readSector(18, 0)
	if err != nil {
		return 0, err
	}
	bam1, err := readSector(53, 0)
	if err != nil {
		return 0, err
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

	bamIsFree := func(track, sector int) bool {
		if !doubleSided && track > 35 {
			return false
		}
		_, bm, berr := bamMeta(track)
		if berr != nil {
			return false
		}
		if sector < 0 || sector >= 24 {
			return false
		}
		mask := byte(1 << (sector % 8))
		return (bm[sector/8] & mask) != 0
	}

	bamMarkUsed := func(track, sector int) {
		if !doubleSided && track > 35 {
			return
		}
		fc, bm, berr := bamMeta(track)
		if berr != nil {
			return
		}
		if sector < 0 || sector >= 24 {
			return
		}
		byteIdx := sector / 8
		mask := byte(1 << (sector % 8))
		if (bm[byteIdx] & mask) != 0 {
			bm[byteIdx] &^= mask
			if fc != nil && *fc > 0 {
				*fc = *fc - 1
			}
		}
	}

	bamMarkFree := func(track, sector int) {
		if !doubleSided && track > 35 {
			return
		}
		fc, bm, berr := bamMeta(track)
		if berr != nil {
			return
		}
		if sector < 0 || sector >= 24 {
			return
		}
		byteIdx := sector / 8
		mask := byte(1 << (sector % 8))
		if (bm[byteIdx] & mask) == 0 {
			bm[byteIdx] |= mask
			if fc != nil {
				*fc = *fc + 1
			}
		}
	}

	allocSector := func() (int, int, error) {
		maxTracks := tracks
		if !doubleSided {
			maxTracks = 35
		}
		for t := 1; t <= maxTracks; t++ {
			spt := sectorsPerTrack(t)
			for sct := 0; sct < spt; sct++ {
				if bamIsFree(t, sct) {
					bamMarkUsed(t, sct)
					return t, sct, nil
				}
			}
		}
		return 0, 0, newStatusErr(proto.StatusTooLarge, "disk full")
	}

	// Walk directory to find existing entry and/or a free slot.
	var (
		foundEntryTrack  int
		foundEntrySector int
		foundEntryOff    int
		foundStartT      int
		foundStartS      int
		foundBlocks      uint16
		foundType        byte
		freeEntryTrack   int
		freeEntrySector  int
		freeEntryOff     int
	)

	dirT, dirS := 18, 1
	for {
		sec, err := readSector(dirT, dirS)
		if err != nil {
			return 0, err
		}

		// Scan entries.
		for i := 0; i < 8; i++ {
			o := 2 + i*32 // file type offset within the 32-byte directory slot
			ft := sec[o]
			if ft == 0x00 {
				if freeEntryTrack == 0 {
					freeEntryTrack, freeEntrySector, freeEntryOff = dirT, dirS, o
				}
				continue
			}
			nameASCII := strings.TrimRight(petsciiToASCIIName(sec[o+3:o+19]), " ")
			if strings.EqualFold(nameASCII, normName) {
				foundEntryTrack, foundEntrySector, foundEntryOff = dirT, dirS, o
				foundType = ft
				foundStartT = int(sec[o+1])
				foundStartS = int(sec[o+2])
				foundBlocks = uint16(sec[o+28]) | (uint16(sec[o+29]) << 8)
				break
			}
		}
		if foundEntryTrack != 0 {
			break
		}

		nextT, nextS := int(sec[0]), int(sec[1])
		if nextT == 0 {
			break
		}
		dirT, dirS = nextT, nextS
	}

	fileExists := foundEntryTrack != 0
	if !fileExists && !create {
		return 0, newStatusErr(proto.StatusNotFound, "file not found")
	}

	// Helper: free a sector chain.
	freeChain := func(startT, startS int) error {
		t, sct := startT, startS
		visited := 0
		for t != 0 {
			visited++
			if visited > 4096 {
				return newStatusErr(proto.StatusInternal, "cycle in chain")
			}
			sec, err := readSector(t, sct)
			if err != nil {
				return err
			}
			nextT, nextS := int(sec[0]), int(sec[1])
			bamMarkFree(t, sct)
			t, sct = nextT, nextS
		}
		return nil
	}

	// If truncating an existing file, require overwrite permission and free its chain.
	if fileExists && truncate {
		if !allowOverwrite {
			return 0, newStatusErr(proto.StatusAccessDenied, "overwrite disabled")
		}
		if err := freeChain(foundStartT, foundStartS); err != nil {
			return 0, err
		}
		foundStartT, foundStartS, foundBlocks = 0, 0, 0
	}

	// Read old content if we are doing a ranged write without truncation and the file exists.
	var old []byte
	if fileExists && !truncate {
		// Reconstruct bytes by walking the chain.
		buf := make([]byte, 0, int(foundBlocks)*254)
		t, sct := foundStartT, foundStartS
		for t != 0 {
			sec, err := readSector(t, sct)
			if err != nil {
				return 0, err
			}
			nextT, nextS := int(sec[0]), int(sec[1])
			dataLen := 254
			if nextT == 0 {
				dataLen = int(sec[1])
				if dataLen == 0 {
					dataLen = 254
				}
				if dataLen < 0 {
					dataLen = 0
				}
				if dataLen > 254 {
					dataLen = 254
				}
			}
			buf = append(buf, sec[2:2+dataLen]...)
			t, sct = nextT, nextS
		}
		old = buf
	}

	// Build final content.
	var final []byte
	if truncate {
		final = data
	} else {
		needLen := int(offset) + len(data)
		if needLen < len(old) {
			needLen = len(old)
		}
		final = make([]byte, needLen)
		copy(final, old)
		copy(final[int(offset):], data)
	}

	// If file existed and we are rewriting (truncate OR ranged write that changes size/content),
	// free old chain now (for ranged writes we did not free above).
	if fileExists && !truncate {
		// Always rewrite whole file to keep implementation simple.
		if err := freeChain(foundStartT, foundStartS); err != nil {
			return 0, err
		}
		foundStartT, foundStartS, foundBlocks = 0, 0, 0
	}

	// Allocate sectors and write chain.
	const dataBytesPerSector = 254
	needSectors := 0
	if len(final) == 0 {
		needSectors = 0
	} else {
		needSectors = (len(final) + dataBytesPerSector - 1) / dataBytesPerSector
	}

	var chain [][2]int
	for i := 0; i < needSectors; i++ {
		t, sct, err := allocSector()
		if err != nil {
			// Rollback allocated sectors on error.
			for _, p := range chain {
				bamMarkFree(p[0], p[1])
			}
			return 0, err
		}
		chain = append(chain, [2]int{t, sct})
	}

	for i, p := range chain {
		t, sct := p[0], p[1]
		buf := make([]byte, 256)
		var nextT, nextS byte
		if i == len(chain)-1 {
			// Last sector: 0, usedBytes (0 => full 254).
			remaining := len(final) - i*dataBytesPerSector
			used := remaining
			if used < 0 {
				used = 0
			}
			if used > 254 {
				used = 254
			}
			nextT = 0
			if used == 254 {
				nextS = 0
			} else {
				nextS = byte(used)
			}
		} else {
			nextT = byte(chain[i+1][0])
			nextS = byte(chain[i+1][1])
		}
		buf[0], buf[1] = nextT, nextS
		start := i * dataBytesPerSector
		end := start + dataBytesPerSector
		if end > len(final) {
			end = len(final)
		}
		copy(buf[2:], final[start:end])
		if err := writeSector(t, sct, buf); err != nil {
			return 0, err
		}
	}

	// Directory entry: if missing, we need a free slot (possibly by extending directory).
	if !fileExists {
		if freeEntryTrack == 0 {
			// Need to allocate a new directory sector on track 18.
			// Find last dir sector first.
			lastT, lastS := 18, 1
			for {
				sec, err := readSector(lastT, lastS)
				if err != nil {
					return 0, err
				}
				nextT, nextS := int(sec[0]), int(sec[1])
				if nextT == 0 {
					break
				}
				lastT, lastS = nextT, nextS
			}

			// Allocate a free sector on track 18 (avoid 0 and 1).
			newDirS := -1
			for sct := 2; sct < sectorsPerTrack(18); sct++ {
				if bamIsFree(18, sct) {
					newDirS = sct
					bamMarkUsed(18, sct)
					break
				}
			}
			if newDirS < 0 {
				return 0, newStatusErr(proto.StatusTooLarge, "directory full")
			}

			// Link it.
			lastSec, err := readSector(lastT, lastS)
			if err != nil {
				return 0, err
			}
			lastSec[0] = 18
			lastSec[1] = byte(newDirS)
			if err := writeSector(lastT, lastS, lastSec); err != nil {
				return 0, err
			}

			// Init new dir sector.
			newSec := make([]byte, 256)
			newSec[0], newSec[1] = 0, 0
			if err := writeSector(18, newDirS, newSec); err != nil {
				return 0, err
			}
			freeEntryTrack, freeEntrySector, freeEntryOff = 18, newDirS, 2
		}

		foundEntryTrack, foundEntrySector, foundEntryOff = freeEntryTrack, freeEntrySector, freeEntryOff
		foundType = 0x82
	}

	// Update directory entry.
	entrySec, err := readSector(foundEntryTrack, foundEntrySector)
	if err != nil {
		return 0, err
	}
	// Clear entry bytes (keep the first two bytes of the 32-byte slot intact).
	for i := 0; i < 30; i++ {
		entrySec[foundEntryOff+i] = 0
	}
	entrySec[foundEntryOff] = foundType
	if needSectors > 0 {
		entrySec[foundEntryOff+1] = byte(chain[0][0])
		entrySec[foundEntryOff+2] = byte(chain[0][1])
	} else {
		entrySec[foundEntryOff+1] = 0
		entrySec[foundEntryOff+2] = 0
	}
	copy(entrySec[foundEntryOff+3:foundEntryOff+19], namePetscii)
	entrySec[foundEntryOff+28] = byte(needSectors & 0xFF)
	entrySec[foundEntryOff+29] = byte((needSectors >> 8) & 0xFF)
	if err := writeSector(foundEntryTrack, foundEntrySector, entrySec); err != nil {
		return 0, err
	}

	// Persist BAMs.
	if err := writeSector(18, 0, bam0); err != nil {
		return 0, err
	}
	if err := writeSector(53, 0, bam1); err != nil {
		return 0, err
	}

	// Make sure everything is on disk.
	if err := f.Sync(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		// Sync can fail on some file systems; do not hard-fail if write succeeded.
	}

	// Drop cache so subsequent reads see updated content.
	d71Cache.Delete(imgPath)

	return uint32(len(data)), nil
}
