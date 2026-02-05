package diskimage

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"wicos64-server/internal/proto"
)

// d71RW is a tiny helper used by the read/write helpers (delete/rename/write).
//
// The existing LoadD71()/parseD71() focuses on parsing the directory for read
// operations. For write operations we mainly need deterministic track/sector
// -> file offset translation.
type d71RW struct {
	Tracks    int
	sectorOff func(track, sector int) int64
}

// NewD71 builds a minimal layout helper for an existing D71 image.
//
// NOTE: For D71 images that include per-sector error bytes, the error bytes are
// stored after the sector data (i.e. the first N*256 bytes are still the sector
// data). This helper therefore only operates on the sector data area.
func NewD71(path string) (*d71RW, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, newStatusErr(proto.StatusNotFound, "disk image not found")
		}
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if fi.IsDir() {
		return nil, newStatusErr(proto.StatusIsADir, "disk image is a directory")
	}

	sizeBytes, tracks, err := detectD71Layout(fi.Size())
	if err != nil {
		return nil, newStatusErr(proto.StatusBadRequest, err.Error())
	}

	// Track offsets for the data area.
	trackOffsets := make([]int64, tracks+1) // 1-based
	var cum int64
	for t := 1; t <= tracks; t++ {
		trackOffsets[t] = cum
		tt := t
		if tt > 35 {
			tt -= 35
		}
		sec := sectorsOnD64Track(tt)
		if sec == 0 {
			return nil, newStatusErr(proto.StatusBadRequest, fmt.Sprintf("invalid track %d", t))
		}
		cum += int64(sec) * sectorSize
	}
	if cum != sizeBytes {
		return nil, newStatusErr(proto.StatusBadRequest, "disk image size mismatch")
	}

	sectorOff := func(track, sector int) int64 {
		if track < 1 || track > tracks {
			return -1
		}
		tt := track
		if tt > 35 {
			tt -= 35
		}
		maxSec := sectorsOnD64Track(tt)
		if maxSec == 0 {
			return -1
		}
		if sector < 0 || sector >= maxSec {
			return -1
		}
		return trackOffsets[track] + int64(sector)*sectorSize
	}

	return &d71RW{Tracks: tracks, sectorOff: sectorOff}, nil
}

// sanitizeD64Name normalizes a PETSCII-ish file name for D64/D71 root.
//
// Those images have no subdirectories; we also keep the validation strict to
// avoid surprising behavior (no wildcards here).
func sanitizeD64Name(name string) (string, error) {
	n := strings.ToUpper(strings.TrimSpace(name))
	if n == "" {
		return "", newStatusErr(proto.StatusBadRequest, "empty filename")
	}
	if strings.ContainsAny(n, "/\\") {
		return "", newStatusErr(proto.StatusNotSupported, "subdirectories are not supported in D64/D71")
	}
	if strings.ContainsAny(n, "*?") {
		return "", newStatusErr(proto.StatusBadRequest, "wildcards are not supported here")
	}
	return n, nil
}
