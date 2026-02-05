package diskimage

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"wicos64-server/internal/proto"
)

const d81RepackBufferTracks = 1

type d81TreeFile struct {
	Name     string
	TypeCode uint8 // 1..4 (SEQ,PRG,USR,REL) - REL not written specially
	Data     []byte
}

type d81TreeDir struct {
	Name     string
	TypeCode uint8 // 5 or 6 for partition entries; 0 for root

	Files map[string]*d81TreeFile
	Dirs  map[string]*d81TreeDir

	RequiredTracks int
}

type d81RawDirEntry struct {
	Name     string
	TypeCode uint8
	StartT   uint8
	StartS   uint8
	Blocks   uint16
}

func repackD81ForWrite(imgPath string, origImg []byte, innerPath string, offset uint32, data []byte, truncate, create, allowOverwrite bool, perm fs.FileMode) (uint32, error) {
	rootHeader := make([]byte, 256)
	copy(rootHeader, d81ReadSector(origImg, int(d81DirTrack), 0))

	root, err := buildD81TreeFromImage(origImg)
	if err != nil {
		return 0, err
	}

	newFileSize, err := applyWriteToD81Tree(root, innerPath, offset, data, truncate, create, allowOverwrite)
	if err != nil {
		return 0, err
	}

	if err := computeD81RepackTracks(root, d81RepackBufferTracks); err != nil {
		return 0, err
	}

	// Build new image with the same total size as the original to preserve error-info bytes (if present).
	noErr := int(d81BytesNoErrorInfo)
	newImg := make([]byte, len(origImg))
	base := newImg
	if len(newImg) >= noErr {
		base = newImg[:noErr]
	}
	// Preserve tail bytes (error info) if present.
	if len(origImg) > noErr && len(newImg) == len(origImg) {
		copy(newImg[noErr:], origImg[noErr:])
	}

	// Format root filesystem
	if err := formatD81Root(base, rootHeader); err != nil {
		return 0, err
	}

	// Populate from tree
	rootCtx := d81FSContext{
		sysTrack:  int(d81DirTrack),
		dirStartT: uint8(d81DirTrack),
		dirStartS: uint8(d81DirSector),
	}
	if err := populateD81Dir(base, rootCtx, root, rootHeader); err != nil {
		return 0, err
	}

	if err := atomicWriteFile(imgPath, newImg, perm); err != nil {
		return 0, newStatusErr(proto.StatusInternal, "failed to write resized image")
	}
	d81Cache.Delete(imgPath)

	return newFileSize, nil
}

func buildD81TreeFromImage(img []byte) (*d81TreeDir, error) {
	root := &d81TreeDir{
		Name:     "",
		TypeCode: 0,
		Files:    map[string]*d81TreeFile{},
		Dirs:     map[string]*d81TreeDir{},
	}

	entries, err := readD81DirEntries(img, uint8(d81DirTrack), uint8(d81DirSector))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		key := d81NameKey(e.Name)
		if key == "" {
			continue
		}
		switch e.TypeCode {
		case 5, 6:
			sub, err := buildD81SubTree(img, e.Name, e.TypeCode, e.StartT, e.StartS)
			if err != nil {
				return nil, err
			}
			root.Dirs[key] = sub
		default:
			data, err := readD81FileData(img, e.StartT, e.StartS)
			if err != nil {
				return nil, err
			}
			root.Files[key] = &d81TreeFile{
				Name:     e.Name,
				TypeCode: e.TypeCode,
				Data:     data,
			}
		}
	}
	return root, nil
}

func buildD81SubTree(img []byte, name string, typeCode uint8, startT, startS uint8) (*d81TreeDir, error) {
	// 1581 "directories" are partitions. On a real 1581 they show up as type "CBM".
	// Some images (or older server versions) may use type 6; normalize to 5 so
	// the directory listing matches real hardware.
	if typeCode == 6 {
		typeCode = 5
	}
	node := &d81TreeDir{
		Name:     name,
		TypeCode: typeCode,
		Files:    map[string]*d81TreeFile{},
		Dirs:     map[string]*d81TreeDir{},
	}

	entries, err := readD81DirEntries(img, startT, startS)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		key := d81NameKey(e.Name)
		if key == "" {
			continue
		}
		switch e.TypeCode {
		case 5, 6:
			sub, err := buildD81SubTree(img, e.Name, e.TypeCode, e.StartT, e.StartS)
			if err != nil {
				return nil, err
			}
			node.Dirs[key] = sub
		default:
			data, err := readD81FileData(img, e.StartT, e.StartS)
			if err != nil {
				return nil, err
			}
			node.Files[key] = &d81TreeFile{
				Name:     e.Name,
				TypeCode: e.TypeCode,
				Data:     data,
			}
		}
	}
	return node, nil
}

func readD81DirEntries(img []byte, startT, startS uint8) ([]d81RawDirEntry, error) {
	var out []d81RawDirEntry

	t := int(startT)
	s := int(startS)
	firstSector := true
	visited := make(map[uint16]bool)

	for {
		if t < 1 || t > d81Tracks || s < 0 || s >= d81SectorsPerTrack {
			return nil, newStatusErr(proto.StatusBadRequest, "invalid directory chain")
		}
		key := uint16(t)<<8 | uint16(s)
		if visited[key] {
			return nil, newStatusErr(proto.StatusBadRequest, "directory loop")
		}
		visited[key] = true

		sec := d81ReadSector(img, t, s)
		nextT := int(sec[0])
		nextS := int(sec[1])

		// If this is a directory header block (common for partitions), skip to the real directory sector.
		if firstSector && sec[2] == 'D' {
			firstSector = false
			if nextT == 0 {
				break
			}
			t, s = nextT, nextS
			continue
		}
		firstSector = false

		for i := 0; i < 8; i++ {
			slot := sec[i*32 : (i+1)*32]
			ft := slot[2]
			if ft == 0 {
				continue
			}
			typeCode := ft & 0x07
			if typeCode == 0 {
				continue
			}
			name := petsciiToASCIIName(slot[5:21])
			if strings.TrimSpace(name) == "" {
				continue
			}
			blocks := binary.LittleEndian.Uint16(slot[30:32])
			out = append(out, d81RawDirEntry{
				Name:     name,
				TypeCode: typeCode,
				StartT:   slot[3],
				StartS:   slot[4],
				Blocks:   blocks,
			})
		}

		if nextT == 0 {
			break
		}
		t, s = nextT, nextS
	}

	return out, nil
}

func readD81FileData(img []byte, startT, startS uint8) ([]byte, error) {
	if startT == 0 {
		return []byte{}, nil
	}
	t := startT
	s := startS
	visited := make(map[uint16]bool)
	out := make([]byte, 0, 1024)

	for {
		key := uint16(t)<<8 | uint16(s)
		if visited[key] {
			return nil, newStatusErr(proto.StatusBadRequest, "file chain loop")
		}
		visited[key] = true

		sec := d81ReadSector(img, int(t), int(s))
		nextT := sec[0]
		nextS := sec[1]

		if nextT == 0 {
			n := int(nextS)
			if n == 0 {
				n = 254
			}
			if n > 254 {
				n = 254
			}
			out = append(out, sec[2:2+n]...)
			break
		}

		out = append(out, sec[2:256]...)
		t, s = nextT, nextS
	}

	return out, nil
}

func applyWriteToD81Tree(root *d81TreeDir, innerPath string, offset uint32, data []byte, truncate, create, allowOverwrite bool) (uint32, error) {
	if truncate && offset != 0 {
		return 0, newStatusErr(proto.StatusBadRequest, "truncate requires offset=0")
	}

	dirParts, leaf, err := splitD81InnerPath(innerPath)
	if err != nil {
		return 0, err
	}

	cur := root
	for _, seg := range dirParts {
		key := d81NameKey(seg)
		next := cur.Dirs[key]
		if next == nil {
			return 0, newStatusErr(proto.StatusNotFound, fmt.Sprintf("directory not found: %s", seg))
		}
		cur = next
	}

	leafKey := d81NameKey(leaf)
	if leafKey == "" {
		return 0, newStatusErr(proto.StatusBadRequest, "empty file name")
	}
	if _, isDir := cur.Dirs[leafKey]; isDir {
		return 0, newStatusErr(proto.StatusIsADir, "path is a directory")
	}

	f := cur.Files[leafKey]
	if f == nil {
		if !create {
			return 0, newStatusErr(proto.StatusNotFound, "file not found")
		}
		if offset != 0 {
			return 0, newStatusErr(proto.StatusBadRequest, "offset beyond EOF")
		}
		cur.Files[leafKey] = &d81TreeFile{
			Name:     leaf,
			TypeCode: 2, // PRG default
			Data:     append([]byte(nil), data...),
		}
		return uint32(len(data)), nil
	}

	// Existing file.
	if truncate {
		if !allowOverwrite {
			return 0, newStatusErr(proto.StatusAlreadyExists, "file exists")
		}
		if offset != 0 {
			return 0, newStatusErr(proto.StatusBadRequest, "truncate requires offset=0")
		}
		f.Data = append([]byte(nil), data...)
		return uint32(len(f.Data)), nil
	}

	// Append-only.
	if offset != uint32(len(f.Data)) {
		return 0, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("append-only: offset %d != eof %d", offset, len(f.Data)))
	}
	f.Data = append(f.Data, data...)
	return uint32(len(f.Data)), nil
}

func computeD81RepackTracks(root *d81TreeDir, bufferTracks int) error {
	// Compute bottom-up for partitions.
	for _, d := range root.Dirs {
		if err := computeD81DirTracks(d, bufferTracks); err != nil {
			return err
		}
	}

	// Root capacity check.
	partTracks := 0
	for _, d := range root.Dirs {
		partTracks += d.RequiredTracks
	}
	if partTracks > 79 { // 80 tracks total, track 40 reserved
		return newStatusErr(proto.StatusTooLarge, "disk full")
	}

	rootFileSectors := 0
	for _, f := range root.Files {
		rootFileSectors += d81SectorsForFile(len(f.Data))
	}
	capacitySectors := (79 - partTracks) * d81SectorsPerTrack
	if rootFileSectors > capacitySectors {
		return newStatusErr(proto.StatusTooLarge, "disk full")
	}
	return nil
}

func computeD81DirTracks(dir *d81TreeDir, bufferTracks int) error {
	// children first
	childTracks := 0
	for _, sub := range dir.Dirs {
		if err := computeD81DirTracks(sub, bufferTracks); err != nil {
			return err
		}
		childTracks += sub.RequiredTracks
	}

	fileSectors := 0
	for _, f := range dir.Files {
		fileSectors += d81SectorsForFile(len(f.Data))
	}
	fileTracks := ceilDiv(fileSectors, d81SectorsPerTrack)

	req := 1 + childTracks + fileTracks + bufferTracks
	if req < 3 {
		req = 3
	}

	// Practical limit: a partition can never exceed 40 tracks on a real 1581 due to track 40 being reserved.
	// (and nested partitions are inside their parent which itself is <=40).
	if req > 40 {
		return newStatusErr(proto.StatusTooLarge, fmt.Sprintf("partition too large: %s needs %d tracks", dir.Name, req))
	}

	dir.RequiredTracks = req
	return nil
}

func d81SectorsForFile(n int) int {
	if n <= 0 {
		return 1
	}
	return (n + 253) / 254
}

func ceilDiv(a, b int) int {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func d81NameKey(name string) string {
	n := strings.TrimSpace(name)
	if n == "" {
		return ""
	}
	return strings.ToUpper(n)
}

func formatD81Root(img []byte, headerTemplate []byte) error {
	if int64(len(img)) < d81BytesNoErrorInfo {
		return newStatusErr(proto.StatusBadRequest, "invalid d81 image")
	}

	// Header sector 40/0
	hdr := d81ReadSector(img, int(d81DirTrack), 0)
	for i := 0; i < 256; i++ {
		hdr[i] = 0
	}
	if len(headerTemplate) == 256 {
		copy(hdr, headerTemplate)
	}
	hdr[0] = uint8(d81DirTrack)
	hdr[1] = uint8(d81DirSector)
	hdr[2] = 'D'

	// Directory sectors 40/3..39 chained.
	for s := 3; s < d81SectorsPerTrack; s++ {
		sec := d81ReadSector(img, int(d81DirTrack), s)
		for i := 0; i < 256; i++ {
			sec[i] = 0
		}
		if s < d81SectorsPerTrack-1 {
			sec[0] = uint8(d81DirTrack)
			sec[1] = uint8(s + 1)
		} else {
			sec[0] = 0
			sec[1] = 0xFF
		}
	}

	// BAM sectors 40/1 and 40/2
	b, err := newD81BAMAt(img, int(d81DirTrack))
	if err != nil {
		return err
	}
	for t := 1; t <= d81Tracks; t++ {
		if t == int(d81DirTrack) {
			if err := b.setTrackAllUsed(t); err != nil {
				return err
			}
		} else {
			if err := b.setTrackAllFree(t); err != nil {
				return err
			}
		}
	}
	return nil
}

func formatD81Partition(img []byte, startTrack int, trackCount int, headerTemplate []byte, label string) error {
	if startTrack < 1 || startTrack > d81Tracks {
		return newStatusErr(proto.StatusBadRequest, "invalid partition start track")
	}
	if trackCount < 3 {
		return newStatusErr(proto.StatusBadRequest, "partition too small")
	}
	if startTrack+trackCount-1 > d81Tracks {
		return newStatusErr(proto.StatusBadRequest, "partition out of bounds")
	}
	if startTrack <= int(d81DirTrack) && startTrack+trackCount-1 >= int(d81DirTrack) {
		return newStatusErr(proto.StatusBadRequest, "partition crosses system track")
	}

	// Header sector start/0
	hdr := d81ReadSector(img, startTrack, 0)
	for i := 0; i < 256; i++ {
		hdr[i] = 0
	}
	if len(headerTemplate) == 256 {
		copy(hdr, headerTemplate)
	}
	hdr[0] = uint8(startTrack)
	hdr[1] = 3
	hdr[2] = 'D'

	// Put label into bytes 4..19 (best-effort; common CBM layout).
	for i := 4; i < 20; i++ {
		hdr[i] = 0xA0
	}
	lb := []byte(strings.ToUpper(strings.TrimSpace(label)))
	if len(lb) > 16 {
		lb = lb[:16]
	}
	copy(hdr[4:20], lb)

	// First directory sector at start/3
	//
	// Note: Unlike the *root* 1581 filesystem (which reserves the whole directory
	// track), 1581 sub-partitions only reserve the minimum blocks needed.
	// A freshly created 120-block partition shows 116 blocks free.
	sec := d81ReadSector(img, startTrack, 3)
	for i := 0; i < 256; i++ {
		sec[i] = 0
	}
	sec[0] = 0
	sec[1] = 0xFF

	// BAM at start/1 and start/2: mark only this partition's tracks as free, and
	// then reserve the required system sectors on the first track.
	b, err := newD81BAMAt(img, startTrack)
	if err != nil {
		return err
	}
	for t := 1; t <= d81Tracks; t++ {
		if t < startTrack || t > startTrack+trackCount-1 {
			if err := b.setTrackAllUsed(t); err != nil {
				return err
			}
			continue
		}
		if err := b.setTrackAllFree(t); err != nil {
			return err
		}
	}
	// Reserve system sectors on the first track: header (0), BAM (1,2), first dir (3).
	for _, s := range []int{0, 1, 2, 3} {
		if err := b.markUsed(startTrack, s); err != nil {
			return err
		}
	}
	return nil
}

func populateD81Dir(img []byte, ctx d81FSContext, dir *d81TreeDir, headerTemplate []byte) error {
	b, err := newD81BAMAt(img, ctx.sysTrack)
	if err != nil {
		return err
	}

	// Create child partitions first so file allocation doesn't fragment their track ranges.
	childKeys := make([]string, 0, len(dir.Dirs))
	for k := range dir.Dirs {
		childKeys = append(childKeys, k)
	}
	sort.Strings(childKeys)

	for _, k := range childKeys {
		ch := dir.Dirs[k]
		startTrack, err := allocD81ContiguousTracks(b, ch.RequiredTracks)
		if err != nil {
			return err
		}

		// Add directory entry in parent.
		loc, freeLoc, lastDirTS, err := findD81DirSlot(img, ctx, ch.Name)
		if err != nil {
			return err
		}
		if loc.found {
			return newStatusErr(proto.StatusBadRequest, fmt.Sprintf("duplicate entry during repack: %s", ch.Name))
		}
		if !freeLoc.found {
			// No free dir slots in chain; allocate a new directory sector on the system track.
			newS, err := allocD81DirSector(img, b, ctx.sysTrack)
			if err != nil {
				return err
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

		blocks := uint16(ch.RequiredTracks * d81SectorsPerTrack)
		writeD81DirEntry(img, freeLoc, ch.Name, uint8(startTrack), 0, blocks, ch.TypeCode)

		// Format child partition filesystem.
		if err := formatD81Partition(img, startTrack, ch.RequiredTracks, headerTemplate, ch.Name); err != nil {
			return err
		}

		childCtx := d81FSContext{
			sysTrack:  startTrack,
			dirStartT: uint8(startTrack),
			dirStartS: 3,
		}
		if err := populateD81Dir(img, childCtx, ch, headerTemplate); err != nil {
			return err
		}
	}

	// Write files.
	fileKeys := make([]string, 0, len(dir.Files))
	for k := range dir.Files {
		fileKeys = append(fileKeys, k)
	}
	sort.Strings(fileKeys)

	for _, k := range fileKeys {
		f := dir.Files[k]

		loc, freeLoc, lastDirTS, err := findD81DirSlot(img, ctx, f.Name)
		if err != nil {
			return err
		}
		if loc.found {
			return newStatusErr(proto.StatusBadRequest, fmt.Sprintf("duplicate file during repack: %s", f.Name))
		}
		if !freeLoc.found {
			// No free dir slots in chain; allocate a new directory sector on the system track.
			newS, err := allocD81DirSector(img, b, ctx.sysTrack)
			if err != nil {
				return err
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

		firstT, firstS, blocks, err := writeNewD81File(img, b, f.Data)
		if err != nil {
			return err
		}
		writeD81DirEntry(img, freeLoc, f.Name, firstT, firstS, blocks, f.TypeCode)
	}

	return nil
}

func allocD81ContiguousTracks(b *d81BAM, tracksNeeded int) (int, error) {
	if tracksNeeded < 1 {
		return 0, newStatusErr(proto.StatusBadRequest, "invalid partition size")
	}

	for start := 1; start <= d81Tracks-tracksNeeded+1; start++ {
		ok := true
		for t := start; t < start+tracksNeeded; t++ {
			fc, err := b.trackFreeCount(t)
			if err != nil {
				return 0, err
			}
			if fc != d81SectorsPerTrack {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		for t := start; t < start+tracksNeeded; t++ {
			if err := b.setTrackAllUsed(t); err != nil {
				return 0, err
			}
		}
		return start, nil
	}

	return 0, newStatusErr(proto.StatusTooLarge, "disk full")
}

// repackD81ForMove rebuilds the whole image to perform a cross-partition move.
//
// This is used as a fallback when an in-place move runs out of space inside the
// destination partition. By repacking, we can shrink/grow partitions along the
// path and keep them contiguous.
func repackD81ForMove(imgPath string, origImg []byte, oldPath string, newPath string, allowOverwrite bool, perm fs.FileMode) error {
	rootHeader := make([]byte, 256)
	copy(rootHeader, d81ReadSector(origImg, int(d81DirTrack), 0))

	root, err := buildD81TreeFromImage(origImg)
	if err != nil {
		return err
	}

	if err := applyMoveToD81Tree(root, oldPath, newPath, allowOverwrite); err != nil {
		return err
	}

	if err := computeD81RepackTracks(root, d81RepackBufferTracks); err != nil {
		return err
	}

	// Build new image with the same total size as the original to preserve error-info bytes (if present).
	noErr := int(d81BytesNoErrorInfo)
	newImg := make([]byte, len(origImg))
	base := newImg
	if len(newImg) >= noErr {
		base = newImg[:noErr]
	}
	// Preserve tail bytes (error info) if present.
	if len(origImg) > noErr && len(newImg) == len(origImg) {
		copy(newImg[noErr:], origImg[noErr:])
	}

	// Format root filesystem.
	if err := formatD81Root(base, rootHeader); err != nil {
		return err
	}

	// Populate from tree.
	rootCtx := d81FSContext{
		sysTrack:  int(d81DirTrack),
		dirStartT: uint8(d81DirTrack),
		dirStartS: uint8(d81DirSector),
	}
	if err := populateD81Dir(base, rootCtx, root, rootHeader); err != nil {
		return err
	}

	if err := atomicWriteFile(imgPath, newImg, perm); err != nil {
		return newStatusErr(proto.StatusInternal, "failed to write resized image")
	}
	d81Cache.Delete(imgPath)
	return nil
}

// applyMoveToD81Tree applies a move/rename inside the in-memory directory tree.
//
// Supports moving regular files across nested partitions.
func applyMoveToD81Tree(root *d81TreeDir, oldPath string, newPath string, allowOverwrite bool) error {
	if strings.ContainsAny(oldPath, "*?") || strings.ContainsAny(newPath, "*?") {
		return newStatusErr(proto.StatusBadRequest, "wildcards are not allowed")
	}

	oldDirParts, oldLeaf, err := splitD81InnerPath(oldPath)
	if err != nil {
		return err
	}
	newDirParts, newLeaf, err := splitD81InnerPath(newPath)
	if err != nil {
		return err
	}

	// No-op.
	same := len(oldDirParts) == len(newDirParts) && oldLeaf == newLeaf
	if same {
		for i := range oldDirParts {
			if oldDirParts[i] != newDirParts[i] {
				same = false
				break
			}
		}
	}
	if same {
		return nil
	}

	// Resolve source directory.
	srcDir := root
	for _, seg := range oldDirParts {
		key := d81NameKey(seg)
		nxt := srcDir.Dirs[key]
		if nxt == nil {
			return newStatusErr(proto.StatusNotFound, fmt.Sprintf("directory not found: %s", seg))
		}
		srcDir = nxt
	}

	// Resolve destination directory.
	dstDir := root
	for _, seg := range newDirParts {
		key := d81NameKey(seg)
		nxt := dstDir.Dirs[key]
		if nxt == nil {
			return newStatusErr(proto.StatusNotFound, fmt.Sprintf("directory not found: %s", seg))
		}
		dstDir = nxt
	}

	srcKey := d81NameKey(oldLeaf)
	dstKey := d81NameKey(newLeaf)
	if srcKey == "" || dstKey == "" {
		return newStatusErr(proto.StatusBadRequest, "empty filename")
	}

	if _, isDir := srcDir.Dirs[srcKey]; isDir {
		return newStatusErr(proto.StatusIsADir, "is a directory")
	}
	srcFile := srcDir.Files[srcKey]
	if srcFile == nil {
		return newStatusErr(proto.StatusNotFound, "not found")
	}

	if _, isDir := dstDir.Dirs[dstKey]; isDir {
		return newStatusErr(proto.StatusIsADir, "destination is a directory")
	}
	if dstExisting := dstDir.Files[dstKey]; dstExisting != nil {
		if !allowOverwrite {
			return newStatusErr(proto.StatusAlreadyExists, "destination exists")
		}
		delete(dstDir.Files, dstKey)
	}

	// Remove from source.
	delete(srcDir.Files, srcKey)

	// Insert into destination (rename leaf).
	srcFile.Name = newLeaf
	dstDir.Files[dstKey] = srcFile
	return nil
}
