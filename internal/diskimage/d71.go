package diskimage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// D71 represents a parsed Commodore 1571 disk image (.d71).
//
// A D71 is basically a double-sided 1541 disk: 2 * 35 tracks.
// The directory is stored on track 18 (side 1), but file data can
// live on either side and directory entries may point to tracks 36..70.
//
// Notes:
//   - We expose a flat namespace (no subdirectories).
//   - Error-information bytes (if present) are ignored by using a sector size of 256 bytes.
type D71 struct {
	Path    string
	ModTime time.Time
	Size    int64 // byte size without error bytes

	Tracks int // 70

	Files  []*FileEntry
	byName map[string]*FileEntry
}

const (
	d71Tracks       = 70
	d71TotalSectors = 1366 // 2 * 683 (standard D64 sectors)
)

type cacheEntryD71 struct {
	modTime time.Time
	size    int64
	img     *D71
	err     error
}

var d71Cache sync.Map // map[string]cacheEntryD71

// LoadD71 parses a .d71 image and caches the parsed directory for faster repeat access.
func LoadD71(path string) (*D71, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	mt := st.ModTime()
	sz := st.Size()

	if v, ok := d71Cache.Load(path); ok {
		ce := v.(cacheEntryD71)
		if ce.modTime.Equal(mt) && ce.size == sz {
			return ce.img, ce.err
		}
	}

	img, err := parseD71(path, st)
	d71Cache.Store(path, cacheEntryD71{
		modTime: mt,
		size:    sz,
		img:     img,
		err:     err,
	})
	return img, err
}

func detectD71Layout(fileSize int64) (sizeBytes int64, tracks int, err error) {
	if fileSize <= 0 {
		return 0, 0, errors.New("empty image")
	}

	var sectors int64
	switch {
	case fileSize%257 == 0:
		// with per-sector error bytes
		sectors = fileSize / 257
		sizeBytes = sectors * sectorSize
	case fileSize%256 == 0:
		sectors = fileSize / 256
		sizeBytes = fileSize
	default:
		return 0, 0, fmt.Errorf("unsupported image size %d (not divisible by 256/257)", fileSize)
	}

	if sectors != d71TotalSectors {
		return 0, 0, fmt.Errorf("unsupported D71 sector count %d (expected %d)", sectors, d71TotalSectors)
	}
	return sizeBytes, d71Tracks, nil
}

func sectorsOnD64Track(track int) int {
	// Standard 1541/1571 sector layout per track (1..35).
	switch {
	case track >= 1 && track <= 17:
		return 21
	case track >= 18 && track <= 24:
		return 19
	case track >= 25 && track <= 30:
		return 18
	case track >= 31 && track <= 35:
		return 17
	default:
		return 0
	}
}

func parseD71(path string, st os.FileInfo) (*D71, error) {
	sizeBytes, tracks, err := detectD71Layout(st.Size())
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Build track offsets for the full 70-track image.
	trackOffsets := make([]int64, tracks+1) // 1-based
	var cum int64
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		tt := t
		if tt > 35 {
			tt -= 35 // side 2 repeats 1..35 layout
		}
		sec := sectorsOnD64Track(tt)
		if sec == 0 {
			return nil, fmt.Errorf("invalid track %d (tt=%d)", t, tt)
		}
		cum += int64(sec) * sectorSize
	}
	if cum != sizeBytes {
		return nil, fmt.Errorf("layout mismatch: computed %d bytes but expected %d", cum, sizeBytes)
	}

	sectorOff := func(track int, sector int) (int64, error) {
		if track <= 0 || track > tracks {
			return 0, fmt.Errorf("track out of range: %d", track)
		}
		tt := track
		if tt > 35 {
			tt -= 35
		}
		maxSec := sectorsOnD64Track(tt)
		if maxSec == 0 {
			return 0, fmt.Errorf("invalid track: %d", track)
		}
		if sector < 0 || sector >= maxSec {
			return 0, fmt.Errorf("sector out of range: t=%d s=%d (max=%d)", track, sector, maxSec)
		}
		return trackOffsets[track] + int64(sector)*sectorSize, nil
	}

	readSector := func(track int, sector int) ([]byte, error) {
		off, err := sectorOff(track, sector)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, sectorSize)
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, err
		}
		return buf, nil
	}

	// Directory: track 18, sector 1 (same as D64).
	nextT, nextS := 18, 1
	files := make([]*FileEntry, 0, 64)
	byName := make(map[string]*FileEntry, 128)

	seen := make(map[uint16]struct{}, 256) // detect loops (track<<8|sector)
	for nextT != 0 {
		key := (uint16(nextT) << 8) | uint16(nextS)
		if _, ok := seen[key]; ok {
			break
		}
		seen[key] = struct{}{}

		buf, err := readSector(nextT, nextS)
		if err != nil {
			return nil, err
		}
		nextT = int(buf[0])
		nextS = int(buf[1])

		for i := 0; i < 8; i++ {
			slot := buf[i*32 : (i+1)*32]
			ft := slot[2]
			if ft == 0x00 {
				continue
			}

			startT := int(slot[3])
			startS := int(slot[4])
			name := petsciiToASCIIName(slot[5:21])
			blocks := binary.LittleEndian.Uint16(slot[30:32])

			typeCode := ft & 0x07
			var typ byte
			switch typeCode {
			case 0: // DEL
				continue
			case 1:
				typ = 1
			case 2:
				typ = 2 // SEQ
			case 3:
				typ = 3 // PRG
			case 4:
				typ = 4 // USR
			case 5:
				typ = 5 // REL (ignored for now)
			default:
				typ = 0
			}

			// Parse file chain (tolerant: skip broken entries).
			chain, size, starts, err := parseFileChain(f, sectorOff, tracks, startT, startS, blocks)
			if err != nil {
				continue
			}

			fe := &FileEntry{
				Name:        name,
				Type:        typ,
				Size:        size,
				Blocks:      blocks,
				StartTrack:  byte(startT),
				StartSector: byte(startS),
				Sectors:     chain,
				starts:      starts,
			}

			keyName := strings.ToUpper(fe.Name)
			if _, exists := byName[keyName]; exists {
				// Disambiguate duplicates by appending ~n.
				for n := 2; n < 100; n++ {
					alt := fmt.Sprintf("%s~%d", keyName, n)
					if _, ok := byName[alt]; !ok {
						fe.Name = alt
						keyName = alt
						break
					}
				}
			}

			byName[keyName] = fe
			files = append(files, fe)
		}
	}

	img := &D71{
		Path:    path,
		ModTime: st.ModTime(),
		Size:    sizeBytes,
		Tracks:  tracks,
		Files:   files,
		byName:  byName,
	}
	return img, nil
}

func (img *D71) Lookup(name string) (*FileEntry, bool) {
	if img == nil {
		return nil, false
	}
	fe, ok := img.byName[strings.ToUpper(name)]
	return fe, ok
}

func (img *D71) SortedEntries() []*FileEntry {
	if img == nil {
		return nil
	}
	out := make([]*FileEntry, 0, len(img.Files))
	out = append(out, img.Files...)
	sort.Slice(out, func(i, j int) bool {
		return strings.ToUpper(out[i].Name) < strings.ToUpper(out[j].Name)
	})
	return out
}
