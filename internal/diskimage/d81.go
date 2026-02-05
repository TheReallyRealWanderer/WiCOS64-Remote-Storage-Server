package diskimage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	d81Tracks           = 80
	d81SectorsPerTrack  = 40
	d81TotalSectors     = d81Tracks * d81SectorsPerTrack // 3200
	d81BytesNoErrorInfo = int64(d81TotalSectors * sectorSize)
	d81BytesWithErrors  = d81BytesNoErrorInfo + int64(d81TotalSectors)
	// Convenience: size in bytes without the optional error-info bytes.
	d81Size = d81TotalSectors * sectorSize

	d81DirTrack  = 40
	d81DirSector = 3
)

// D81 represents a parsed Commodore 1581 disk image (.d81).
//
// The server uses this to transparently mount disk images as "virtual" directories.
// For milestone 4 we keep it read-only.
//
// Layout assumptions:
//   - Standard 1581 layout (80 tracks, 40 sectors per track, 256 bytes per sector)
//   - Directory starts at track 40, sector 3 (chained like other CBM DOS directories)
//   - Optional 1-byte error info per sector at the end of the file is tolerated
//     (we ignore the trailing error bytes).
//
// If you run into a D81 that does not match this standard layout, we can extend
// detection later (but the first goal is to support the common 1581 images).
//
// Source note: We intentionally keep the parser small and self-contained.
// It reuses the generic file-chain logic from d64.go.
//
// Supports:
//   - Root directory listing
//   - Subdirectory listing (DIR entries) by following the directory chain
//   - File reads (range reads) for files inside root or subdirectories
//
// Naming:
//   - Names are normalized to ASCII using the same PETSCII conversion as D64.
//   - Lookups are case-insensitive.
//
// Concurrency:
//   - Parsed images are cached by absolute path + (mtime,size) fingerprint.
//   - Reading file data always opens the underlying image file anew.
//
// Size:
//   - For robust range reads, each entry stores a sector map + cumulative starts.
//
// (Yes, this is a lot of comment, but it saves future confusion.)
type D81 struct {
	Path            string
	ModTime         time.Time
	SizeBytes       int64
	Tracks          int
	SectorsPerTrack int

	Files []*FileEntry

	byName map[string]*FileEntry

	// dirCache memoizes parsed directory listings for subdirectories.
	// Key is "<track>:<sector>".
	// Root directory (track 40/sector 3) is already available via Files/byName.
	dirCache sync.Map // map[string]d81DirCacheEntry
}

type d81DirCacheEntry struct {
	entries []*FileEntry
	byName  map[string]*FileEntry
}

type cacheEntryD81 struct {
	modTime time.Time
	size    int64
	img     *D81
}

var d81Cache sync.Map // map[string]cacheEntryD81

func detectD81Layout(fileSize int64) (sizeBytes int64, hasErrorInfo bool, err error) {
	switch fileSize {
	case d81BytesNoErrorInfo:
		return d81BytesNoErrorInfo, false, nil
	case d81BytesWithErrors:
		// Error bytes are present, but we treat the image as the first 819200 bytes.
		return d81BytesNoErrorInfo, true, nil
	default:
		return 0, false, fmt.Errorf("d81: unsupported file size %d (expected %d or %d)", fileSize, d81BytesNoErrorInfo, d81BytesWithErrors)
	}
}

// LoadD81 loads and parses a D81 image from disk.
//
// The result is cached by absolute path + (mtime,size) to avoid re-parsing
// on every request.
func LoadD81(path string) (*D81, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if v, ok := d81Cache.Load(path); ok {
		ce, ok := v.(cacheEntryD81)
		if ok && ce.img != nil && ce.size == st.Size() && ce.modTime.Equal(st.ModTime()) {
			return ce.img, nil
		}
	}

	img, err := parseD81(path, st)
	if err != nil {
		return nil, err
	}

	d81Cache.Store(path, cacheEntryD81{modTime: st.ModTime(), size: st.Size(), img: img})
	return img, nil
}

func parseD81(path string, st os.FileInfo) (*D81, error) {
	sizeBytes, _, err := detectD81Layout(st.Size())
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sectorOff := func(track, sector int) (int64, error) {
		if track <= 0 || track > d81Tracks {
			return 0, errors.New("d81: track out of range")
		}
		if sector < 0 || sector >= d81SectorsPerTrack {
			return 0, errors.New("d81: sector out of range")
		}
		idx := (track-1)*d81SectorsPerTrack + sector
		off := int64(idx * sectorSize)
		if off < 0 || off+sectorSize > sizeBytes {
			return 0, errors.New("d81: sector offset out of bounds")
		}
		return off, nil
	}

	files := make([]*FileEntry, 0, 64)
	byName := make(map[string]*FileEntry, 64)

	dirT := byte(d81DirTrack)
	dirS := byte(d81DirSector)
	visited := make(map[[2]byte]bool)

	buf := make([]byte, sectorSize)

	for {
		if dirT == 0 {
			break
		}
		key := [2]byte{dirT, dirS}
		if visited[key] {
			return nil, fmt.Errorf("d81: directory loop detected at t=%d s=%d", dirT, dirS)
		}
		visited[key] = true

		off, err := sectorOff(int(dirT), int(dirS))
		if err != nil {
			return nil, err
		}
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, err
		}

		nextT := buf[0]
		nextS := buf[1]

		for i := 0; i < 8; i++ {
			slot := buf[i*32 : (i+1)*32]
			ft := slot[2]
			if ft == 0 {
				continue
			}
			typeCode := ft & 0x07
			if typeCode == 0 {
				continue
			}

			startT := slot[3]
			startS := slot[4]
			if startT == 0 {
				continue
			}

			name := petsciiToASCIIName(slot[5:21])
			if name == "" {
				continue
			}

			blocks := binary.LittleEndian.Uint16(slot[30:32])

			// Directories/partitions (type 6=DIR, 5=CBM) are stored as special entries. We treat them as folders and
			// don't need their sector chain for listing/navigation.
			var (
				sectors []SectorRef
				size    uint64
				starts  []uint64
			)
			if typeCode != 6 && typeCode != 5 {
				var err error
				sectors, size, starts, err = parseFileChain(f, sectorOff, d81Tracks, int(startT), int(startS), blocks)
				if err != nil {
					// Be tolerant: skip individual entries that have a broken chain.
					continue
				}
			}

			fe := &FileEntry{
				Name:        name,
				Type:        typeCode,
				StartTrack:  startT,
				StartSector: startS,
				Blocks:      blocks,
				Size:        size,
				Sectors:     sectors,
			}
			fe.starts = starts

			files = append(files, fe)
			upper := strings.ToUpper(strings.TrimSpace(name))
			if _, exists := byName[upper]; !exists {
				byName[upper] = fe
			}
		}

		if nextT == 0 {
			break
		}
		dirT = nextT
		dirS = nextS
	}

	return &D81{
		Path:            path,
		ModTime:         st.ModTime(),
		SizeBytes:       sizeBytes,
		Tracks:          d81Tracks,
		SectorsPerTrack: d81SectorsPerTrack,
		Files:           files,
		byName:          byName,
	}, nil
}

// Lookup returns a file entry by name (case-insensitive).
func (img *D81) Lookup(name string) (*FileEntry, bool) {
	if img == nil {
		return nil, false
	}
	key := strings.ToUpper(strings.TrimSpace(name))
	fe, ok := img.byName[key]
	return fe, ok
}

// SortedEntries returns the directory entries sorted by name.
func (img *D81) SortedEntries() []*FileEntry {
	if img == nil || len(img.Files) == 0 {
		return nil
	}
	out := make([]*FileEntry, len(img.Files))
	copy(out, img.Files)
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToUpper(out[i].Name) < strings.ToUpper(out[j].Name)
	})
	return out
}

// Dir returns the directory listing for the directory starting at (startTrack,startSector).
// Root directory is (d81DirTrack,d81DirSector) and is already available via Files/byName.
// Subdirectories are parsed on-demand and cached.
func (img *D81) Dir(startTrack, startSector byte) ([]*FileEntry, map[string]*FileEntry, error) {
	if img == nil {
		return nil, nil, errors.New("nil image")
	}
	if startTrack == d81DirTrack && startSector == d81DirSector {
		return img.Files, img.byName, nil
	}
	key := fmt.Sprintf("%d:%d", startTrack, startSector)
	if v, ok := img.dirCache.Load(key); ok {
		ce := v.(d81DirCacheEntry)
		return ce.entries, ce.byName, nil
	}
	entries, byName, err := img.readDir(startTrack, startSector)
	if err != nil {
		return nil, nil, err
	}
	img.dirCache.Store(key, d81DirCacheEntry{entries: entries, byName: byName})
	return entries, byName, nil
}

// SortedDirEntries returns a name-sorted copy of the passed entries.
func (img *D81) SortedDirEntries(entries []*FileEntry) []*FileEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*FileEntry, len(entries))
	copy(out, entries)
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToUpper(out[i].Name) < strings.ToUpper(out[j].Name)
	})
	return out
}

func (img *D81) readDir(startTrack, startSector byte) ([]*FileEntry, map[string]*FileEntry, error) {
	f, err := os.Open(img.Path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	sectorOff := func(track, sector int) (int64, error) {
		if track < 1 || track > d81Tracks {
			return 0, errors.New("track out of range")
		}
		if sector < 0 || sector >= d81SectorsPerTrack {
			return 0, errors.New("sector out of range")
		}
		idx := int64((track-1)*d81SectorsPerTrack + sector)
		off := idx * int64(sectorSize)
		if off+int64(sectorSize) > img.SizeBytes {
			return 0, errors.New("offset out of range")
		}
		return off, nil
	}

	files := make([]*FileEntry, 0, 64)
	byName := make(map[string]*FileEntry)
	visited := make(map[string]struct{})

	dirT := startTrack
	dirS := startSector
	buf := make([]byte, sectorSize)
	firstSector := true

	for {
		key := fmt.Sprintf("%d:%d", dirT, dirS)
		if _, ok := visited[key]; ok {
			return nil, nil, errors.New("directory chain loop")
		}
		visited[key] = struct{}{}

		off, err := sectorOff(int(dirT), int(dirS))
		if err != nil {
			return nil, nil, err
		}
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return nil, nil, err
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, nil, err
		}

		nextT := buf[0]
		nextS := buf[1]

		// 1581 subdirectories are usually referenced by a directory *header* block.
		// The header block contains the DOS version byte 'D' at offset 2 and bytes 0..1
		// point to the first actual directory sector.
		//
		// If we parse the header as directory entries, clients may see "wilde Zeichen"
		// or only the directory name instead of the actual contents.
		if firstSector && buf[2] == 'D' {
			firstSector = false
			if nextT == 0 {
				break // empty directory
			}
			dirT = nextT
			dirS = nextS
			continue
		}
		firstSector = false

		for i := 0; i < 8; i++ {
			slot := buf[i*32 : (i+1)*32]
			ft := slot[2]
			if ft == 0x00 {
				continue
			}
			typeCode := ft & 0x07
			if typeCode == 0x00 {
				continue
			}
			startT := slot[3]
			startS := slot[4]
			if startT == 0x00 {
				continue
			}
			name := petsciiToASCIIName(slot[5:21])
			if name == "" {
				continue
			}
			blocks := binary.LittleEndian.Uint16(slot[30:32])

			fe := &FileEntry{
				Name:        name,
				Type:        typeCode,
				StartTrack:  startT,
				StartSector: startS,
				Blocks:      blocks,
				Size:        0,
			}

			// Parse sector chain for regular files so read_range/hash work.
			// For directories/partitions (type=6 DIR, type=5 CBM), we keep it light and only store the
			// start T/S for navigation.
			if typeCode != 0x06 && typeCode != 0x05 {
				sectors, size, starts, err := parseFileChain(f, sectorOff, d81Tracks, int(startT), int(startS), blocks)
				if err != nil {
					// Be robust: skip broken entries instead of rejecting the whole directory.
					continue
				}
				fe.Size = size
				fe.Sectors = sectors
				fe.starts = starts
			}

			files = append(files, fe)
			upper := strings.ToUpper(strings.TrimSpace(name))
			if _, exists := byName[upper]; !exists {
				byName[upper] = fe
			}
		}

		if nextT == 0 {
			break
		}
		dirT = nextT
		dirS = nextS
	}

	return files, byName, nil
}
