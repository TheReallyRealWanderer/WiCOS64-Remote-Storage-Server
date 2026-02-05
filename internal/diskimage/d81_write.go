package diskimage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"wicos64-server/internal/proto"
)

// NOTE: 1581 (.d81) "subdirectories" are implemented as partitions.
// Reading already supports nested partitions; this file adds write support.
//
// Supported write semantics:
//
//   - Existing files are append-only unless truncate+allowOverwrite is true.
//   - Creating a new file requires offset == 0.
//   - Appending to an existing file requires offset == current file size.
//   - Overwriting requires: truncate == true, allowOverwrite == true, offset == 0.

type d81FSContext struct {
	sysTrack  int   // directory/BAM track (root: 40, partitions: first track)
	dirStartT uint8 // first directory sector track (usually sysTrack)
	dirStartS uint8 // first directory sector sector (usually 3)
}

type d81TS struct{ T, S uint8 }

type d81DirSlotLoc struct {
	found bool

	slotOff int // absolute offset in image
	slotIdx int // 0..7 within sector

	// valid when this loc refers to an existing entry
	entryTypeCode uint8 // low 3 bits from directory entry file type (1..6)
	startT        uint8
	startS        uint8
}

// WriteFileRangeD81 writes into a .d81 image at imgPath.
// innerPath may contain nested partitions separated by '/'.
func WriteFileRangeD81(imgPath, innerPath string, offset uint32, data []byte, truncate, create, allowOverwrite bool) (uint32, error) {
	if truncate && offset != 0 {
		return 0, newStatusErr(proto.StatusBadRequest, "truncate requires offset=0")
	}
	if strings.ContainsAny(innerPath, "*?") {
		return 0, newStatusErr(proto.StatusBadRequest, "wildcards are not allowed")
	}

	st, err := os.Stat(imgPath)
	if err != nil {
		return 0, newStatusErr(proto.StatusNotFound, "image not found")
	}
	perm := st.Mode().Perm()

	origImg, err := os.ReadFile(imgPath)
	if err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to read image")
	}
	if int64(len(origImg)) < d81BytesNoErrorInfo {
		return 0, newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}

	// Work on a copy so that on failure we can still repack from the unmodified original bytes.
	img := make([]byte, len(origImg))
	copy(img, origImg)

	dirParts, leaf, err := splitD81InnerPath(innerPath)
	if err != nil {
		return 0, err
	}
	ctx, err := resolveD81WriteContext(img, dirParts)
	if err != nil {
		return 0, err
	}

	newSize, err := writeFileRangeD81Bytes(img, ctx, leaf, offset, data, truncate, create, allowOverwrite)
	if err == nil {
		if err := atomicWriteFile(imgPath, img, perm); err != nil {
			return 0, newStatusErr(proto.StatusInternal, "failed to write image")
		}
		d81Cache.Delete(imgPath)
		return newSize, nil
	}

	// Auto-resize & repack when writing inside a partition runs out of space.
	if isDiskFullStatus(err) && len(dirParts) > 0 {
		return repackD81ForWrite(imgPath, origImg, innerPath, offset, data, truncate, create, allowOverwrite, perm)
	}

	return 0, err
}

func splitD81InnerPath(innerPath string) (dirParts []string, leaf string, err error) {
	p := strings.TrimSpace(innerPath)
	if strings.HasSuffix(p, "/") {
		return nil, "", newStatusErr(proto.StatusIsADir, "path is a directory")
	}
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return nil, "", newStatusErr(proto.StatusBadRequest, "empty path")
	}

	rawParts := strings.Split(p, "/")
	parts := make([]string, 0, len(rawParts))
	for _, r := range rawParts {
		if strings.TrimSpace(r) == "" {
			continue
		}
		n, err := sanitizeD64Name(r)
		if err != nil {
			return nil, "", err
		}
		parts = append(parts, n)
	}
	if len(parts) == 0 {
		return nil, "", newStatusErr(proto.StatusBadRequest, "empty path")
	}
	leaf = parts[len(parts)-1]
	dirParts = parts[:len(parts)-1]
	return dirParts, leaf, nil
}

func resolveD81WriteContext(img []byte, dirParts []string) (d81FSContext, error) {
	ctx := d81FSContext{
		sysTrack:  int(d81DirTrack),
		dirStartT: uint8(d81DirTrack),
		dirStartS: uint8(d81DirSector),
	}

	for _, seg := range dirParts {
		loc, _, _, err := findD81DirSlot(img, ctx, seg)
		if err != nil {
			return d81FSContext{}, err
		}
		if !loc.found {
			return d81FSContext{}, newStatusErr(proto.StatusNotFound, fmt.Sprintf("directory not found: %s", seg))
		}
		if loc.entryTypeCode != 5 && loc.entryTypeCode != 6 {
			return d81FSContext{}, newStatusErr(proto.StatusNotFound, fmt.Sprintf("not a directory: %s", seg))
		}
		if loc.startT == 0 {
			return d81FSContext{}, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid directory entry: %s", seg))
		}

		// Partition directory entries typically point at a header block. If the first sector has 'D' at [2],
		// treat it as a header and follow its link to the first directory block.
		hdr := d81ReadSector(img, int(loc.startT), int(loc.startS))
		dirT := loc.startT
		dirS := loc.startS
		if hdr[2] == 'D' && hdr[0] != 0 {
			dirT = hdr[0]
			dirS = hdr[1]
		}

		ctx = d81FSContext{
			sysTrack:  int(loc.startT),
			dirStartT: dirT,
			dirStartS: dirS,
		}
	}

	return ctx, nil
}

func writeFileRangeD81Bytes(img []byte, ctx d81FSContext, name string, offset uint32, data []byte, truncate, create, allowOverwrite bool) (uint32, error) {
	if truncate && offset != 0 {
		return 0, newStatusErr(proto.StatusBadRequest, "truncate requires offset=0")
	}

	b, err := newD81BAMAt(img, ctx.sysTrack)
	if err != nil {
		return 0, err
	}

	loc, freeLoc, lastDirTS, err := findD81DirSlot(img, ctx, name)
	if err != nil {
		return 0, err
	}

	if loc.found && (loc.entryTypeCode == 5 || loc.entryTypeCode == 6) {
		return 0, newStatusErr(proto.StatusIsADir, "path is a directory")
	}

	exists := loc.found
	// Preserve original file type when overwriting.
	newEntryType := uint8(2) // PRG default for new files
	if exists && loc.entryTypeCode >= 1 && loc.entryTypeCode <= 4 {
		newEntryType = loc.entryTypeCode
	}

	if exists && truncate {
		if !allowOverwrite {
			return 0, newStatusErr(proto.StatusAlreadyExists, "file exists")
		}
		// Delete the existing chain, clear entry, and then fall through to create path.
		if err := deleteD81FileChain(img, b, loc.startT, loc.startS); err != nil {
			return 0, err
		}
		clearD81DirSlot(img, loc)

		exists = false
		freeLoc = loc
		freeLoc.found = true
	}

	if exists {
		curSize, lastT, lastS, blocks, err := scanD81File(img, loc.startT, loc.startS)
		if err != nil {
			return 0, err
		}
		if offset != uint32(curSize) {
			return 0, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("append-only: offset %d != eof %d", offset, curSize))
		}
		addedBlocks, err := appendD81File(img, b, lastT, lastS, data)
		if err != nil {
			return 0, err
		}
		// Update blocks in directory entry.
		binary.LittleEndian.PutUint16(img[loc.slotOff+30:loc.slotOff+32], uint16(blocks+addedBlocks))
		return uint32(curSize) + uint32(len(data)), nil
	}

	// Create new.
	if !create {
		return 0, newStatusErr(proto.StatusNotFound, "file not found")
	}
	if offset != 0 {
		return 0, newStatusErr(proto.StatusBadRequest, "offset beyond EOF")
	}

	if !freeLoc.found {
		// No free dir slots in chain; try to allocate a new directory sector on the directory track.
		newS, err := allocD81DirSector(img, b, ctx.sysTrack)
		if err != nil {
			return 0, err
		}
		lastSec := d81ReadSector(img, int(lastDirTS.T), int(lastDirTS.S))
		lastSec[0] = uint8(ctx.sysTrack)
		lastSec[1] = newS
		freeLoc = d81DirSlotLoc{
			found:   true,
			slotOff: d81SectorOffset(ctx.sysTrack, int(newS)),
			slotIdx: 0,
		}
	}

	firstT, firstS, blocks, err := writeNewD81File(img, b, data)
	if err != nil {
		return 0, err
	}
	writeD81DirEntry(img, freeLoc, name, firstT, firstS, blocks, newEntryType)
	return uint32(len(data)), nil
}

func isDiskFullStatus(err error) bool {
	var se *StatusError
	if errors.As(err, &se) && se.Status() == proto.StatusTooLarge {
		// Keep this narrow: we only want to trigger an auto-repack on actual
		// space exhaustion, not e.g. "directory full".
		return se.Error() == "disk full"
	}
	return false
}

func atomicWriteFile(path string, data []byte, perm fs.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp := filepath.Join(dir, fmt.Sprintf(".%s.tmp.%d", base, os.Getpid()))

	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	// Best-effort: on rename failure, try to remove the temp file.
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func d81SectorOffset(track, sector int) int {
	// track: 1..80, sector: 0..39
	return ((track-1)*d81SectorsPerTrack + sector) * 256
}

func d81ReadSector(img []byte, track, sector int) []byte {
	off := d81SectorOffset(track, sector)
	return img[off : off+256]
}

type d81BAM struct {
	sysTrack int
	bam1     []byte
	bam2     []byte
}

func newD81BAMAt(img []byte, sysTrack int) (*d81BAM, error) {
	if sysTrack < 1 || sysTrack > d81Tracks {
		return nil, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid system track: %d", sysTrack))
	}
	// BAM is always on sectors 1 and 2 of the system track.
	off := d81SectorOffset(sysTrack, 2) + 256
	if off > len(img) || d81SectorOffset(sysTrack, 1)+256 > len(img) {
		return nil, newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}
	return &d81BAM{
		sysTrack: sysTrack,
		bam1:     d81ReadSector(img, sysTrack, 1),
		bam2:     d81ReadSector(img, sysTrack, 2),
	}, nil
}

// entry returns the BAM sector slice and the byte offset for the track entry.
func (b *d81BAM) entry(track int) ([]byte, int, error) {
	if track < 1 || track > d81Tracks {
		return nil, 0, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid track: %d", track))
	}
	var sec []byte
	var idx int
	if track <= 40 {
		sec = b.bam1
		idx = track - 1
	} else {
		sec = b.bam2
		idx = track - 41
	}
	off := 0x10 + idx*6
	return sec, off, nil
}

func (b *d81BAM) isFree(track, sector int) (bool, error) {
	if sector < 0 || sector >= d81SectorsPerTrack {
		return false, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid sector: %d", sector))
	}
	sec, off, err := b.entry(track)
	if err != nil {
		return false, err
	}
	// bytes [off+1..off+5] form a 40-bit bitmap (little-endian)
	bits := uint64(sec[off+1]) |
		(uint64(sec[off+2]) << 8) |
		(uint64(sec[off+3]) << 16) |
		(uint64(sec[off+4]) << 24) |
		(uint64(sec[off+5]) << 32)
	return (bits & (1 << sector)) != 0, nil
}

func (b *d81BAM) markUsed(track, sector int) error {
	sec, off, err := b.entry(track)
	if err != nil {
		return err
	}
	if sector < 0 || sector >= d81SectorsPerTrack {
		return newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid sector: %d", sector))
	}
	// build 40-bit bitmap
	bits := uint64(sec[off+1]) |
		(uint64(sec[off+2]) << 8) |
		(uint64(sec[off+3]) << 16) |
		(uint64(sec[off+4]) << 24) |
		(uint64(sec[off+5]) << 32)
	if bits&(1<<sector) == 0 {
		return nil // already used
	}
	bits &^= (1 << sector)
	sec[off+1] = byte(bits)
	sec[off+2] = byte(bits >> 8)
	sec[off+3] = byte(bits >> 16)
	sec[off+4] = byte(bits >> 24)
	sec[off+5] = byte(bits >> 32)
	if sec[off] > 0 {
		sec[off]--
	}
	return nil
}

func (b *d81BAM) markFree(track, sector int) error {
	sec, off, err := b.entry(track)
	if err != nil {
		return err
	}
	if sector < 0 || sector >= d81SectorsPerTrack {
		return newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid sector: %d", sector))
	}
	bits := uint64(sec[off+1]) |
		(uint64(sec[off+2]) << 8) |
		(uint64(sec[off+3]) << 16) |
		(uint64(sec[off+4]) << 24) |
		(uint64(sec[off+5]) << 32)
	if bits&(1<<sector) != 0 {
		return nil // already free
	}
	bits |= (1 << sector)
	sec[off+1] = byte(bits)
	sec[off+2] = byte(bits >> 8)
	sec[off+3] = byte(bits >> 16)
	sec[off+4] = byte(bits >> 24)
	sec[off+5] = byte(bits >> 32)
	sec[off]++
	return nil
}

// setTrackAllUsed marks all sectors of a track as used.
func (b *d81BAM) setTrackAllUsed(track int) error {
	sec, off, err := b.entry(track)
	if err != nil {
		return err
	}
	sec[off] = 0
	for i := 1; i <= 5; i++ {
		sec[off+i] = 0
	}
	return nil
}

// setTrackAllFree marks all sectors of a track as free.
func (b *d81BAM) setTrackAllFree(track int) error {
	sec, off, err := b.entry(track)
	if err != nil {
		return err
	}
	sec[off] = byte(d81SectorsPerTrack)
	for i := 1; i <= 5; i++ {
		sec[off+i] = 0xFF
	}
	return nil
}

func (b *d81BAM) trackFreeCount(track int) (int, error) {
	sec, off, err := b.entry(track)
	if err != nil {
		return 0, err
	}
	return int(sec[off]), nil
}

func (b *d81BAM) allocDataSector() (uint8, uint8, error) {
	// IMPORTANT:
	//	- On the 1581 root filesystem, the system/directory track (40) must never
	//	  be used for file data.
	//	- For 1581 sub-partitions, we *can* technically use the system track for
	//	  file data (it is counted as free blocks), but doing so too eagerly can
	//	  starve the directory chain of sectors and trigger "directory full" even
	//	  while lots of blocks remain free elsewhere in the partition.
	//	  Therefore we prefer allocating file data on non-system tracks first and
	//	  only fall back to the system track as a last resort.

	// Root: never allocate file data on the directory/BAM track.
	if b.sysTrack == int(d81DirTrack) {
		for t := 1; t <= d81Tracks; t++ {
			if t == b.sysTrack {
				continue
			}
			for s := 0; s < d81SectorsPerTrack; s++ {
				free, err := b.isFree(t, s)
				if err != nil {
					return 0, 0, err
				}
				if free {
					if err := b.markUsed(t, s); err != nil {
						return 0, 0, err
					}
					return uint8(t), uint8(s), nil
				}
			}
		}
		return 0, 0, newStatusErr(proto.StatusTooLarge, "disk full")
	}

	// Partition: prefer non-system tracks first.
	for t := 1; t <= d81Tracks; t++ {
		if t == b.sysTrack {
			continue
		}
		for s := 0; s < d81SectorsPerTrack; s++ {
			free, err := b.isFree(t, s)
			if err != nil {
				return 0, 0, err
			}
			if free {
				if err := b.markUsed(t, s); err != nil {
					return 0, 0, err
				}
				return uint8(t), uint8(s), nil
			}
		}
	}

	// Fallback: allocate on the system track (avoid reserved system sectors).
	for s := 4; s < d81SectorsPerTrack; s++ {
		free, err := b.isFree(b.sysTrack, s)
		if err != nil {
			return 0, 0, err
		}
		if free {
			if err := b.markUsed(b.sysTrack, s); err != nil {
				return 0, 0, err
			}
			return uint8(b.sysTrack), uint8(s), nil
		}
	}

	return 0, 0, newStatusErr(proto.StatusTooLarge, "disk full")
}

// allocD81DirSector allocates a directory sector on the directory track (sysTrack), if available.
func allocD81DirSector(img []byte, b *d81BAM, sysTrack int) (uint8, error) {
	for s := 4; s < d81SectorsPerTrack; s++ {
		free, err := b.isFree(sysTrack, s)
		if err != nil {
			return 0, err
		}
		if free {
			if err := b.markUsed(sysTrack, s); err != nil {
				return 0, err
			}
			sec := d81ReadSector(img, sysTrack, s)
			for i := 0; i < 256; i++ {
				sec[i] = 0
			}
			sec[0] = 0
			sec[1] = 0xFF
			return uint8(s), nil
		}
	}
	return 0, newStatusErr(proto.StatusTooLarge, "directory full")
}

func findD81DirSlot(img []byte, ctx d81FSContext, name string) (loc d81DirSlotLoc, freeLoc d81DirSlotLoc, lastDirTS d81TS, err error) {
	t := int(ctx.dirStartT)
	s := int(ctx.dirStartS)
	if t < 1 || t > d81Tracks || s < 0 || s >= d81SectorsPerTrack {
		return d81DirSlotLoc{}, d81DirSlotLoc{}, d81TS{}, newStatusErr(proto.StatusBadRequest, "invalid directory start")
	}

	visited := make(map[int]bool)
	for {
		key := t*100 + s
		if visited[key] {
			return d81DirSlotLoc{}, d81DirSlotLoc{}, d81TS{}, newStatusErr(proto.StatusBadRequest, "directory loop")
		}
		visited[key] = true

		sec := d81ReadSector(img, t, s)
		lastDirTS = d81TS{T: uint8(t), S: uint8(s)}

		// 8 slots of 32 bytes
		for i := 0; i < 8; i++ {
			slot := sec[i*32 : (i+1)*32]
			ft := slot[2]
			if ft == 0 {
				if !freeLoc.found {
					freeLoc = d81DirSlotLoc{
						found:   true,
						slotOff: d81SectorOffset(t, s) + i*32,
						slotIdx: i,
					}
				}
				continue
			}
			typeCode := ft & 0x07
			if typeCode == 0 {
				continue
			}
			slotName := petsciiToASCIIName(slot[5:21])
			if strings.EqualFold(slotName, name) {
				loc = d81DirSlotLoc{
					found:         true,
					slotOff:       d81SectorOffset(t, s) + i*32,
					slotIdx:       i,
					entryTypeCode: typeCode,
					startT:        slot[3],
					startS:        slot[4],
				}
				return loc, freeLoc, lastDirTS, nil
			}
		}

		nextT := int(sec[0])
		nextS := int(sec[1])
		if nextT == 0 {
			break
		}
		t, s = nextT, nextS
	}
	return d81DirSlotLoc{}, freeLoc, lastDirTS, nil
}

func clearD81DirSlot(img []byte, loc d81DirSlotLoc) {
	if !loc.found {
		return
	}
	if loc.slotIdx == 0 {
		for i := 2; i < 32; i++ {
			img[loc.slotOff+i] = 0
		}
		return
	}
	for i := 0; i < 32; i++ {
		img[loc.slotOff+i] = 0
	}
}

func writeD81DirEntry(img []byte, loc d81DirSlotLoc, name string, firstT, firstS uint8, blocks uint16, typeCode uint8) {
	// Zero the slot (preserving link bytes for slot 0).
	clearD81DirSlot(img, loc)

	slot := img[loc.slotOff : loc.slotOff+32]
	slot[2] = 0x80 | (typeCode & 0x07) // closed + type
	slot[3] = firstT
	slot[4] = firstS

	// File name (16 bytes), padded with 0xA0.
	for i := 0; i < 16; i++ {
		slot[5+i] = 0xA0
	}
	nb := []byte(name)
	if len(nb) > 16 {
		nb = nb[:16]
	}
	copy(slot[5:21], nb)

	binary.LittleEndian.PutUint16(slot[30:32], blocks)
}

func scanD81File(img []byte, startT, startS uint8) (size int, lastT, lastS uint8, blocks uint16, err error) {
	if startT == 0 {
		return 0, 0, 0, 0, nil
	}
	t := startT
	s := startS
	visited := make(map[uint16]bool)
	for {
		key := uint16(t)<<8 | uint16(s)
		if visited[key] {
			return 0, 0, 0, 0, newStatusErr(proto.StatusBadRequest, "file chain loop")
		}
		visited[key] = true

		sec := d81ReadSector(img, int(t), int(s))
		blocks++
		nextT := sec[0]
		nextS := sec[1]
		if nextT == 0 {
			// last sector
			ll := int(nextS)
			if ll == 0 {
				ll = 254
			}
			size += ll
			lastT, lastS = t, s
			break
		}
		size += 254
		lastT, lastS = t, s
		t, s = nextT, nextS
	}
	return size, lastT, lastS, blocks, nil
}

func writeNewD81File(img []byte, b *d81BAM, data []byte) (firstT, firstS uint8, blocks uint16, err error) {
	// Always allocate at least one sector.
	t, s, err := b.allocDataSector()
	if err != nil {
		return 0, 0, 0, err
	}
	firstT, firstS = t, s
	blocks = 1

	sec := d81ReadSector(img, int(t), int(s))
	for i := 0; i < 256; i++ {
		sec[i] = 0
	}

	if len(data) == 0 {
		sec[0] = 0
		sec[1] = 0 // treated as full 254 by most readers; matches existing behavior
		return firstT, firstS, blocks, nil
	}

	pos := 0
	curT, curS := t, s
	for {
		sec = d81ReadSector(img, int(curT), int(curS))
		chunk := 254
		if len(data)-pos < 254 {
			chunk = len(data) - pos
		}
		copy(sec[2:2+chunk], data[pos:pos+chunk])
		pos += chunk

		if pos >= len(data) {
			// last block
			sec[0] = 0
			if chunk == 254 {
				sec[1] = 0
			} else {
				sec[1] = byte(chunk)
			}
			break
		}

		// allocate next
		nt, ns, err := b.allocDataSector()
		if err != nil {
			return 0, 0, 0, err
		}
		blocks++
		sec[0] = nt
		sec[1] = ns
		curT, curS = nt, ns
	}
	return firstT, firstS, blocks, nil
}

func appendD81File(img []byte, b *d81BAM, lastT, lastS uint8, data []byte) (newBlocks uint16, err error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Determine how much of the current last sector is used.
	sec := d81ReadSector(img, int(lastT), int(lastS))
	used := int(sec[1])
	if used == 0 {
		used = 254
	}
	if used > 254 {
		used = 254
	}

	pos := 0

	// If there is still space in the last sector, fill it first.
	if used < 254 {
		space := 254 - used
		n := len(data)
		if n > space {
			n = space
		}
		copy(sec[2+used:2+used+n], data[:n])
		used += n
		pos = n

		// If we're done, keep this sector as the last one and update its length.
		if pos >= len(data) {
			sec[0] = 0
			if used == 254 {
				sec[1] = 0
			} else {
				sec[1] = byte(used)
			}
			return 0, nil
		}

		// We filled the last sector to full; it must become a link block now.
		sec[1] = 0 // full
	}

	// Allocate the first new sector and link it from the previous last sector.
	nt, ns, err := b.allocDataSector()
	if err != nil {
		return 0, err
	}
	sec[0] = nt
	sec[1] = ns
	newBlocks++

	curT, curS := nt, ns

	// Write remaining data into newly allocated sectors.
	for {
		sec = d81ReadSector(img, int(curT), int(curS))
		for i := 0; i < 256; i++ {
			sec[i] = 0
		}

		chunk := 254
		if len(data)-pos < 254 {
			chunk = len(data) - pos
		}
		copy(sec[2:2+chunk], data[pos:pos+chunk])
		pos += chunk

		if pos >= len(data) {
			sec[0] = 0
			if chunk == 254 {
				sec[1] = 0
			} else {
				sec[1] = byte(chunk)
			}
			break
		}

		nt, ns, err := b.allocDataSector()
		if err != nil {
			return newBlocks, err
		}
		newBlocks++
		sec[0] = nt
		sec[1] = ns
		curT, curS = nt, ns
	}

	return newBlocks, nil
}

func deleteD81FileChain(img []byte, b *d81BAM, startT, startS uint8) error {
	if startT == 0 {
		return nil
	}
	t := startT
	s := startS
	visited := make(map[uint16]bool)
	for {
		key := uint16(t)<<8 | uint16(s)
		if visited[key] {
			return newStatusErr(proto.StatusBadRequest, "file chain loop")
		}
		visited[key] = true

		sec := d81ReadSector(img, int(t), int(s))
		nextT := sec[0]
		nextS := sec[1]

		if err := b.markFree(int(t), int(s)); err != nil {
			return err
		}
		for i := 0; i < 256; i++ {
			sec[i] = 0
		}

		if nextT == 0 {
			break
		}
		t, s = nextT, nextS
	}
	return nil
}
