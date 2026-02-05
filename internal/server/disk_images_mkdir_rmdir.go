package server

import (
	"fmt"
	"path"
	"strings"
)

type diskImageKind int

const (
	diskImageNone diskImageKind = iota
	diskImageD64
	diskImageD71
	diskImageD81
)

// detectDiskImageMountRootPath checks whether p is exactly a disk image "mount root"
// (i.e., it ends with .d64/.d71/.d81 and has no inner path).
func detectDiskImageMountRootPath(p string) (diskImageKind, bool) {
	if mountPath, inner, ok := splitD64Path(p); ok && mountPath == p && inner == "" {
		return diskImageD64, true
	}
	if mountPath, inner, ok := splitD71Path(p); ok && mountPath == p && inner == "" {
		return diskImageD71, true
	}
	if mountPath, inner, ok := splitD81Path(p); ok && mountPath == p && inner == "" {
		return diskImageD81, true
	}
	return diskImageNone, false
}

// hasAnyDiskImageParent returns true if any *parent* segment (all segments except
// the last) looks like a supported disk image filename.
//
// This is used to prevent treating "images-inside-images" as host filesystem paths.
func hasAnyDiskImageParent(p string) bool {
	if p == "" || p[0] != '/' {
		return false
	}
	trim := strings.TrimPrefix(p, "/")
	if trim == "" {
		return false
	}
	segs := strings.Split(trim, "/")
	if len(segs) <= 1 {
		return false
	}
	for i := 0; i < len(segs)-1; i++ {
		seg := segs[i]
		if isD64Segment(seg) || isD71Segment(seg) || isD81Segment(seg) {
			return true
		}
	}
	return false
}

func diskImageLabelFromPath(p string) string {
	// Use filename without extension.
	base := strings.TrimSpace(path.Base(p))
	if base == "/" || base == "." {
		return "WICOS64"
	}
	if dot := strings.LastIndex(base, "."); dot > 0 {
		base = base[:dot]
	}
	base = strings.ToUpper(strings.TrimSpace(base))
	if base == "" {
		base = "WICOS64"
	}
	if len(base) > 16 {
		base = base[:16]
	}
	return base
}

func emptyDiskImageBytes(kind diskImageKind, label string) ([]byte, error) {
	switch kind {
	case diskImageD64:
		return emptyD64Bytes(label), nil
	case diskImageD71:
		return emptyD71Bytes(label), nil
	case diskImageD81:
		return emptyD81Bytes(label), nil
	default:
		return nil, fmt.Errorf("unknown disk image kind")
	}
}

func emptyD64Bytes(label string) []byte {
	// Standard 35-track 1541 layout: 683 sectors * 256 = 174848 bytes.
	sectorsPerTrack := func(track int) int {
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

	tracks := 35
	trackOffsets := make([]int, tracks+1)
	cum := 0
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		cum += sectorsPerTrack(t) * 256
	}
	img := make([]byte, cum)
	sectorOff := func(track, sector int) int {
		return trackOffsets[track] + sector*256
	}

	// BAM sector at 18/0.
	bam := img[sectorOff(18, 0) : sectorOff(18, 0)+256]
	bam[0] = 18
	bam[1] = 1
	bam[2] = 0x41 // DOS version
	bam[3] = 0x00

	// Disk name (best effort) at 0x90..0x9F (PETSCII, padded with 0xA0).
	for i := 0x90; i < 0x90+16; i++ {
		bam[i] = 0xA0
	}
	lb := []byte(strings.ToUpper(strings.TrimSpace(label)))
	if len(lb) > 16 {
		lb = lb[:16]
	}
	copy(bam[0x90:0x90+16], lb)
	// Disk ID (0xA2..0xA3) and DOS type (0xA5..0xA6).
	//
	// Some directory listing routines expect these fields and the surrounding
	// padding bytes to be initialized (0xA0). If left as 0x00, they may show up
	// as odd characters on the C64 (often '@').
	bam[0xA0] = 0xA0
	bam[0xA1] = 0xA0
	bam[0xA2] = '0'
	bam[0xA3] = '0'
	bam[0xA4] = 0xA0
	bam[0xA5] = '2'
	bam[0xA6] = 'A'
	bam[0xA7] = 0xA0
	bam[0xA8] = 0xA0
	bam[0xA9] = 0xA0
	bam[0xAA] = 0xA0

	// BAM entries per track.
	for t := 1; t <= tracks; t++ {
		base := 4 + (t-1)*4
		secs := sectorsPerTrack(t)
		var bm [3]byte
		free := 0
		for s := 0; s < secs; s++ {
			bm[s/8] |= 1 << uint(s%8)
			free++
		}
		// Reserve the full directory track like a standard formatted disk:
		//   - 18/0: BAM
		//   - 18/1..18/18: directory sectors
		// This yields the expected 664 blocks free on an empty D64.
		if t == 18 {
			for s := 0; s < secs; s++ {
				idx := s / 8
				mask := byte(1 << uint(s%8))
				if (bm[idx] & mask) != 0 {
					bm[idx] &^= mask
					free--
				}
			}
		}
		bam[base] = byte(free)
		bam[base+1] = bm[0]
		bam[base+2] = bm[1]
		bam[base+3] = bm[2]
	}

	// Directory chain on track 18: 18/1 -> 18/2 -> ... -> 18/18 -> 0/255.
	for s := 1; s <= 18; s++ {
		sec := img[sectorOff(18, s) : sectorOff(18, s)+256]
		// Link bytes.
		if s < 18 {
			sec[0] = 18
			sec[1] = byte(s + 1)
		} else {
			sec[0] = 0
			sec[1] = 0xFF
		}
	}

	return img
}

func emptyD71Bytes(label string) []byte {
	// D71 is a 1571 double-sided image: 70 tracks total, 1366 sectors.
	//
	// Important: The 1571 BAM format differs from a simple "two D64 BAMs".
	// - Track 18/0 holds the main BAM for tracks 1-35 (4 bytes per track)
	//   and also contains the *free sector counts* for tracks 36-70 at $DD-$FF.
	// - Track 53/0 holds the allocation bitmaps for tracks 36-70 (3 bytes per track, no count byte).
	// - Byte $03 in 18/0 must have bit 7 set (0x80) to indicate double-sided.
	//
	// References: VICE "emulator file formats" (D71 BAM layout) and other format docs.
	// This yields the expected 1328 blocks free on a freshly created D71.

	const sectorSize int64 = 256

	// Standard 1541/1571 sector layout per track (1..35).
	sectorsPerTrackSide := func(track int) int {
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

	tracks := 70
	// Build track offsets for a full 70-track image.
	trackOffsets := make([]int64, tracks+1) // 1-based
	var cum int64
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		tt := t
		if tt > 35 {
			tt -= 35
		}
		sec := sectorsPerTrackSide(tt)
		cum += int64(sec) * sectorSize
	}

	img := make([]byte, cum)
	sectorOff := func(track, sector int) int64 {
		if track < 1 || track > tracks {
			return -1
		}
		tt := track
		if tt > 35 {
			tt -= 35
		}
		maxSec := sectorsPerTrackSide(tt)
		if sector < 0 || sector >= maxSec {
			return -1
		}
		return trackOffsets[track] + int64(sector)*sectorSize
	}

	readSectorSlice := func(track, sector int) []byte {
		off := sectorOff(track, sector)
		if off < 0 {
			return nil
		}
		return img[off : off+sectorSize]
	}

	bam0 := readSectorSlice(18, 0)
	bam1 := readSectorSlice(53, 0)
	if bam0 == nil || bam1 == nil {
		return img
	}

	// --- Header / metadata on BAM0 (18/0) ---
	bam0[0] = 18
	bam0[1] = 1
	bam0[2] = 0x41 // DOS version
	bam0[3] = 0x80 // double-sided flag (1571 mode)

	// Disk name at $90..$9F (PETSCII padded with $A0).
	name := strings.ToUpper(label)
	if len(name) > 16 {
		name = name[:16]
	}
	for i := 0; i < 16; i++ {
		bam0[0x90+i] = 0xA0
	}
	copy(bam0[0x90:0x90+16], []byte(name))

	// Fill standard padding/ID fields.
	bam0[0xA0] = 0xA0
	bam0[0xA1] = 0xA0
	bam0[0xA2] = '0'
	bam0[0xA3] = '0'
	bam0[0xA4] = 0xA0
	bam0[0xA5] = '2'
	bam0[0xA6] = 'A'
	for i := 0xA7; i <= 0xAA; i++ {
		bam0[i] = 0xA0
	}

	// --- BAM entries for tracks 1..35 on BAM0 ($04..$8F) ---
	for t := 1; t <= 35; t++ {
		base := 4 + (t-1)*4
		secs := sectorsPerTrackSide(t)
		var bm [3]byte
		free := 0
		if t != 18 {
			for s := 0; s < secs; s++ {
				bm[s>>3] |= 1 << uint(s&7)
				free++
			}
		}
		bam0[base] = byte(free)
		bam0[base+1] = bm[0]
		bam0[base+2] = bm[1]
		bam0[base+3] = bm[2]
	}

	// --- Free sector counts for tracks 36..70 on BAM0 ($DD..$FF) ---
	for t := 36; t <= 70; t++ {
		idx := t - 36
		tt := t - 35 // 1..35
		secs := sectorsPerTrackSide(tt)
		free := secs
		if t == 53 {
			// Track 53 is the flip-side equivalent of directory track 18.
			// It is fully allocated/reserved.
			free = 0
		}
		bam0[0xDD+idx] = byte(free)
	}

	// --- BAM bitmaps for tracks 36..70 on BAM1 (53/0), 3 bytes per track starting at $00 ---
	for t := 36; t <= 70; t++ {
		idx := t - 36
		tt := t - 35 // 1..35
		secs := sectorsPerTrackSide(tt)
		var bm [3]byte
		if t != 53 {
			for s := 0; s < secs; s++ {
				bm[s>>3] |= 1 << uint(s&7)
			}
		}
		off := idx * 3
		bam1[off+0] = bm[0]
		bam1[off+1] = bm[1]
		bam1[off+2] = bm[2]
	}

	// Directory chain on track 18: sectors 1..18.
	for s := 1; s <= 18; s++ {
		sec := readSectorSlice(18, s)
		if sec == nil {
			continue
		}
		if s == 18 {
			sec[0] = 0
			sec[1] = 0xFF
		} else {
			sec[0] = 18
			sec[1] = byte(s + 1)
		}
		for i := 2; i < 256; i++ {
			sec[i] = 0x00
		}
	}

	return img
}

func emptyD81Bytes(label string) []byte {
	// Standard 1581 layout: 80 tracks * 40 sectors * 256 = 819200 bytes.
	const (
		tracks          = 80
		sectorsPerTrack = 40
		dirTrack        = 40
		dirSector       = 3
	)
	img := make([]byte, tracks*sectorsPerTrack*256)
	sectorOff := func(track, sector int) int {
		return ((track-1)*sectorsPerTrack + sector) * 256
	}

	// Header sector at 40/0
	hdr := img[sectorOff(dirTrack, 0) : sectorOff(dirTrack, 0)+256]
	for i := 0; i < 256; i++ {
		hdr[i] = 0
	}
	hdr[0] = byte(dirTrack)
	hdr[1] = byte(dirSector)
	hdr[2] = 'D'

	// Disk name best effort: bytes 4..19 padded with 0xA0.
	for i := 4; i < 20; i++ {
		hdr[i] = 0xA0
	}
	lb := []byte(strings.ToUpper(strings.TrimSpace(label)))
	if len(lb) > 16 {
		lb = lb[:16]
	}
	copy(hdr[4:20], lb)
	// The classic 1581 header also contains ID and DOS version bytes. If left as
	// 0x00, they may show up as odd characters in some directory listings.
	hdr[0x14] = 0xA0
	hdr[0x15] = 0xA0
	hdr[0x16] = '0'
	hdr[0x17] = '0'
	hdr[0x18] = 0xA0
	// DOS+disk version is "3D" (0x33, 0x44) on a standard 1581.
	hdr[0x19] = '3'
	hdr[0x1A] = 'D'
	hdr[0x1B] = 0xA0
	hdr[0x1C] = 0xA0

	// Directory sectors 40/3..39 chained.
	for s := dirSector; s < sectorsPerTrack; s++ {
		sec := img[sectorOff(dirTrack, s) : sectorOff(dirTrack, s)+256]
		for i := 0; i < 256; i++ {
			sec[i] = 0
		}
		if s < sectorsPerTrack-1 {
			sec[0] = byte(dirTrack)
			sec[1] = byte(s + 1)
		} else {
			sec[0] = 0
			sec[1] = 0xFF
		}
	}

	// BAM sectors 40/1 and 40/2. Track entries start at 0x10, 6 bytes each:
	// [free_count][40-bit bitmap little-endian (5 bytes)].
	bam1 := img[sectorOff(dirTrack, 1) : sectorOff(dirTrack, 1)+256]
	bam2 := img[sectorOff(dirTrack, 2) : sectorOff(dirTrack, 2)+256]
	for i := 0; i < 256; i++ {
		bam1[i] = 0
		bam2[i] = 0
	}
	for t := 1; t <= tracks; t++ {
		var sec []byte
		var idx int
		if t <= 40 {
			sec = bam1
			idx = t - 1
		} else {
			sec = bam2
			idx = t - 41
		}
		off := 0x10 + idx*6
		if t == dirTrack {
			// System track fully used.
			sec[off] = 0
			sec[off+1] = 0
			sec[off+2] = 0
			sec[off+3] = 0
			sec[off+4] = 0
			sec[off+5] = 0
		} else {
			sec[off] = sectorsPerTrack
			sec[off+1] = 0xFF
			sec[off+2] = 0xFF
			sec[off+3] = 0xFF
			sec[off+4] = 0xFF
			sec[off+5] = 0xFF
		}
	}
	return img
}
