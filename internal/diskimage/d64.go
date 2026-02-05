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

// D64 represents a parsed Commodore 1541 disk image (.d64).
//
// This implementation is intentionally minimal and optimized for read-only
// access from the WiCOS64 Remote Storage server.
//
// Notes:
//   - Subdirectories are not supported (1541 directories are flat).
//   - REL files are parsed like any other file (no REL-side-sector support).
//   - Error-info variants (".d64 with error bytes") are accepted; the error
//     bytes are ignored.

const (
	sectorSize         = 256
	dataBytesPerSector = 254
	maxTracksSupported = 42
)

// SectorRef references one physical sector of a file chain.
// Offset points to the start of the 256-byte sector within the image.
// DataLen is the amount of data bytes used from this sector (<= 254).
//
// The two link bytes are not counted; data starts at offset+2.
type SectorRef struct {
	Track   byte
	Sector  byte
	Offset  int64
	DataLen int
}

type FileEntry struct {
	Name        string
	Type        byte   // low 3 bits of the CBM file type (1=SEQ,2=PRG,3=USR,4=REL)
	Size        uint64 // bytes
	Blocks      uint16 // blocks from directory entry
	StartTrack  byte
	StartSector byte
	Sectors     []SectorRef
	starts      []uint64 // cumulative byte offsets per sector (same length as Sectors)
}

type D64 struct {
	Path    string
	ModTime time.Time
	Size    int64 // size of raw image data (without error bytes)
	Tracks  int

	Files  []*FileEntry
	byName map[string]*FileEntry // upper-name -> entry
}

type cacheEntry struct {
	modTime time.Time
	size    int64
	img     *D64
}

var d64Cache sync.Map // map[string]cacheEntry

// LoadD64 loads and parses a .d64 image with a small in-memory cache.
// Cache is invalidated when mtime or size changes.
func LoadD64(path string) (*D64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("not a file")
	}

	if v, ok := d64Cache.Load(path); ok {
		ce := v.(cacheEntry)
		if ce.modTime.Equal(fi.ModTime()) && ce.size == fi.Size() {
			return ce.img, nil
		}
	}

	img, err := parseD64(path, fi.ModTime(), fi.Size())
	if err != nil {
		return nil, err
	}
	d64Cache.Store(path, cacheEntry{modTime: fi.ModTime(), size: fi.Size(), img: img})
	return img, nil
}

func parseD64(path string, modTime time.Time, fileSize int64) (*D64, error) {
	sizeBytes, tracks, err := detectD64Layout(fileSize)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Build track offsets.
	sectorsOnTrack := func(t int) int {
		switch {
		case t >= 1 && t <= 17:
			return 21
		case t >= 18 && t <= 24:
			return 19
		case t >= 25 && t <= 30:
			return 18
		case t >= 31:
			return 17
		default:
			return 0
		}
	}

	trackOffsets := make([]int64, tracks+1) // 1..tracks
	var cum int64
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		cum += int64(sectorsOnTrack(t) * sectorSize)
	}
	if cum != sizeBytes {
		return nil, fmt.Errorf("layout mismatch")
	}

	sectorOff := func(track, sector int) (int64, error) {
		if track < 1 || track > tracks {
			return 0, fmt.Errorf("invalid track %d", track)
		}
		sp := sectorsOnTrack(track)
		if sector < 0 || sector >= sp {
			return 0, fmt.Errorf("invalid sector %d on track %d", sector, track)
		}
		return trackOffsets[track] + int64(sector*sectorSize), nil
	}

	readSector := func(track, sector int, buf []byte) error {
		off, err := sectorOff(track, sector)
		if err != nil {
			return err
		}
		if off < 0 || off+sectorSize > sizeBytes {
			return fmt.Errorf("sector out of bounds")
		}
		_, err = f.ReadAt(buf, off)
		return err
	}

	img := &D64{
		Path:    path,
		ModTime: modTime,
		Size:    sizeBytes,
		Tracks:  tracks,
		Files:   []*FileEntry{},
		byName:  map[string]*FileEntry{},
	}

	// Directory chain starts at track 18 sector 1.
	t := 18
	s := 1
	buf := make([]byte, sectorSize)

	for {
		if t == 0 {
			break
		}
		if err := readSector(t, s, buf); err != nil {
			return nil, fmt.Errorf("read dir sector: %w", err)
		}

		nextT := int(buf[0])
		nextS := int(buf[1])

		// Directory sectors contain 8 *slots* of 32 bytes.
		//
		// Important: The next directory link (next track/sector) lives in bytes
		// 0..1 of the sector. A classic .d64 directory still has 8 entries per
		// sector (max 144 files total), so the entry fields begin at offset 2
		// within each 32 byte slot.
		for i := 0; i < 8; i++ {
			slot := buf[i*32 : (i+1)*32]

			ft := slot[2]
			if ft == 0x00 {
				continue
			}
			typeCode := ft & 0x07
			if typeCode == 0x00 {
				// DEL
				continue
			}

			startT := slot[3]
			startS := slot[4]
			name := petsciiToASCIIName(slot[5:21])
			if name == "" {
				name = "NONAME"
			}
			blocks := binary.LittleEndian.Uint16(slot[30:32])

			sectors, size, starts, err := parseFileChain(f, sectorOff, tracks, int(startT), int(startS), blocks)
			if err != nil {
				// If parsing fails, skip the entry rather than rejecting the entire image.
				continue
			}

			fe := &FileEntry{
				Name:        name,
				Type:        typeCode,
				Size:        size,
				Blocks:      blocks,
				StartTrack:  startT,
				StartSector: startS,
				Sectors:     sectors,
				starts:      starts,
			}

			// Disambiguate duplicate names.
			baseKey := strings.ToUpper(fe.Name)
			key := baseKey
			if _, exists := img.byName[key]; exists {
				for n := 2; ; n++ {
					cand := fmt.Sprintf("%s~%d", fe.Name, n)
					key = strings.ToUpper(cand)
					if _, exists2 := img.byName[key]; !exists2 {
						fe.Name = cand
						break
					}
				}
			}

			img.Files = append(img.Files, fe)
			img.byName[strings.ToUpper(fe.Name)] = fe
			_ = baseKey
		}

		if nextT == 0 {
			break
		}
		t = nextT
		s = nextS
	}

	return img, nil
}

func petsciiToASCIIName(b []byte) string {
	// Disk names are usually stored padded with 0xA0.
	// We'll map 0xA0 to space and keep ASCII range as-is.
	runes := make([]rune, 0, len(b))
	for _, c := range b {
		switch {
		case c == 0xA0:
			runes = append(runes, ' ')
		case c >= 0x20 && c <= 0x7E:
			r := rune(c)
			// Avoid path separators.
			if r == '/' || r == '\\' {
				r = '_'
			}
			runes = append(runes, r)
		default:
			runes = append(runes, '_')
		}
	}
	s := strings.TrimRight(string(runes), " ")
	s = strings.TrimSpace(s)
	return strings.ToUpper(s)
}

func parseFileChain(
	f *os.File,
	sectorOff func(track, sector int) (int64, error),
	tracks int,
	startTrack int,
	startSector int,
	blocks uint16,
) ([]SectorRef, uint64, []uint64, error) {
	if startTrack == 0 {
		return nil, 0, nil, errors.New("empty start track")
	}
	if startTrack < 1 || startTrack > tracks {
		return nil, 0, nil, errors.New("invalid start track")
	}

	visited := map[uint16]bool{}
	sectors := make([]SectorRef, 0, int(blocks))
	starts := make([]uint64, 0, int(blocks))
	var size uint64

	buf := make([]byte, sectorSize)
	t := startTrack
	s := startSector
	for {
		key := uint16((t << 8) | (s & 0xFF))
		if visited[key] {
			return nil, 0, nil, errors.New("loop in chain")
		}
		visited[key] = true

		off, err := sectorOff(t, s)
		if err != nil {
			return nil, 0, nil, err
		}
		_, err = f.ReadAt(buf, off)
		if err != nil {
			return nil, 0, nil, err
		}

		nextT := int(buf[0])
		nextS := int(buf[1])
		dataLen := dataBytesPerSector
		if nextT == 0 {
			// Last sector: buf[1] stores bytes used (0..254). Treat 0 as full.
			dataLen = nextS
			if dataLen <= 0 {
				dataLen = dataBytesPerSector
			}
			if dataLen > dataBytesPerSector {
				dataLen = dataBytesPerSector
			}
		}

		starts = append(starts, size)
		sectors = append(sectors, SectorRef{Track: byte(t), Sector: byte(s), Offset: off, DataLen: dataLen})
		size += uint64(dataLen)

		if nextT == 0 {
			break
		}
		if nextT < 1 || nextT > tracks {
			return nil, 0, nil, errors.New("invalid next track")
		}
		t = nextT
		s = nextS

		// Safety cap (avoid infinite loops on broken images).
		if len(sectors) > 2000 {
			return nil, 0, nil, errors.New("chain too long")
		}
	}

	return sectors, size, starts, nil
}

func detectD64Layout(fileSize int64) (sizeBytes int64, tracks int, err error) {
	if fileSize <= 0 {
		return 0, 0, errors.New("empty file")
	}

	// Determine sector count and whether error bytes are present.
	var sectors int64
	switch {
	case fileSize%257 == 0:
		// Error-info variant: 256 bytes sector data + 1 error byte per sector.
		sectors = fileSize / 257
		sizeBytes = sectors * 256
	case fileSize%256 == 0:
		sectors = fileSize / 256
		sizeBytes = fileSize
	default:
		return 0, 0, fmt.Errorf("unsupported .d64 size %d", fileSize)
	}

	if sectors < 683 {
		return 0, 0, fmt.Errorf("unsupported .d64: too few sectors (%d)", sectors)
	}

	extra := sectors - 683
	if extra%17 != 0 {
		return 0, 0, fmt.Errorf("unsupported .d64 sector count (%d)", sectors)
	}

	tracks = 35 + int(extra/17)
	if tracks < 35 || tracks > maxTracksSupported {
		return 0, 0, fmt.Errorf("unsupported .d64 tracks (%d)", tracks)
	}

	return sizeBytes, tracks, nil
}

// Lookup returns a file entry by name (case-insensitive).
func (img *D64) Lookup(name string) (*FileEntry, bool) {
	if img == nil {
		return nil, false
	}
	fe, ok := img.byName[strings.ToUpper(name)]
	return fe, ok
}

// Names returns all exposed entry names (sorted, uppercased).
func (img *D64) Names() []string {
	if img == nil {
		return nil
	}
	names := make([]string, 0, len(img.Files))
	for _, fe := range img.Files {
		names = append(names, fe.Name)
	}
	sort.Strings(names)
	return names
}

// SortedEntries returns entries sorted by name.
func (img *D64) SortedEntries() []*FileEntry {
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

// ReadFileRange reads a byte range from a file entry inside an image.
// It uses the pre-parsed sector chain.
func ReadFileRange(imgPath string, fe *FileEntry, offset, length uint64) ([]byte, error) {
	if fe == nil {
		return nil, errors.New("nil file")
	}
	if length == 0 {
		return []byte{}, nil
	}
	if offset > fe.Size {
		return nil, errors.New("offset out of range")
	}
	if offset+length > fe.Size {
		return nil, errors.New("range out of range")
	}

	f, err := os.Open(imgPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Find initial sector by binary search on starts.
	idx := sort.Search(len(fe.starts), func(i int) bool { return fe.starts[i] > offset }) - 1
	if idx < 0 {
		idx = 0
	}

	out := make([]byte, 0, length)
	buf := make([]byte, sectorSize)
	curOff := offset
	remaining := length

	for idx < len(fe.Sectors) && remaining > 0 {
		sec := fe.Sectors[idx]
		secStart := fe.starts[idx]
		if curOff < secStart {
			curOff = secStart
		}
		offIn := curOff - secStart
		if offIn >= uint64(sec.DataLen) {
			idx++
			continue
		}

		_, err := f.ReadAt(buf, sec.Offset)
		if err != nil {
			return nil, err
		}

		data := buf[2 : 2+sec.DataLen]
		avail := uint64(len(data)) - offIn
		take := minU64(avail, remaining)
		out = append(out, data[offIn:offIn+take]...)
		remaining -= take
		curOff += take
		idx++
	}

	if uint64(len(out)) != length {
		return nil, fmt.Errorf("short read")
	}
	return out, nil
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
