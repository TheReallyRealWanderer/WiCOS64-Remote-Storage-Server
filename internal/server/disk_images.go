package server

import (
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"wicos64-server/internal/diskimage"
	"wicos64-server/internal/fsops"
	"wicos64-server/internal/proto"
)

// splitD64Path checks whether p contains a ".d64" segment and splits it into:
//
//	mountPath: the path up to and including the .d64 segment
//	innerPath: the remaining path inside the image ("" means image root)
func splitD64Path(p string) (mountPath, innerPath string, ok bool) {
	if p == "" || p[0] != '/' {
		return "", "", false
	}
	trim := strings.TrimPrefix(p, "/")
	if trim == "" {
		return "", "", false
	}
	segs := strings.Split(trim, "/")
	for i, seg := range segs {
		if isD64Segment(seg) {
			mountPath = "/" + strings.Join(segs[:i+1], "/")
			if i+1 < len(segs) {
				innerPath = strings.Join(segs[i+1:], "/")
			} else {
				innerPath = ""
			}
			return mountPath, innerPath, true
		}
	}
	return "", "", false
}

// normalizeDiskImageLeafName normalizes a leaf file name used inside a mounted disk image.
//
// When the PRG fallback compatibility option is enabled, WiCOS64 directory listings may
// append ".PRG" to program files. Users then naturally refer to files with that suffix.
// CBM disk images, however, do not have a special "extension" concept. To keep the mount
// behaviour transparent and avoid names like "FOO.PRG.PRG" in listings, we strip a trailing
// ".PRG" when the fallback is enabled.
func normalizeDiskImageLeafName(name string, fallbackPRG bool) string {
	if !fallbackPRG {
		return name
	}
	u := strings.ToUpper(name)
	if strings.HasSuffix(u, ".PRG") {
		base := name[:len(name)-4]
		// Avoid returning empty string.
		if strings.TrimSpace(base) != "" {
			return base
		}
	}
	return name
}

func isD64Segment(seg string) bool {
	ext := strings.TrimSpace(filepath.Ext(seg))
	return strings.EqualFold(ext, ".d64")
}

// splitD71Path checks whether p contains a ".d71" segment and splits it into:
//   - mountPath: the path up to and including the .d71 segment
//   - innerPath: the remaining path inside the image (without a leading slash)
func splitD71Path(p string) (mountPath, innerPath string, ok bool) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", "", false
	}
	parts := strings.Split(p, "/")
	for i, seg := range parts {
		if isD71Segment(seg) {
			mountPath = strings.Join(parts[:i+1], "/")
			innerPath = strings.Join(parts[i+1:], "/")
			return mountPath, innerPath, true
		}
	}
	return "", "", false
}

func isD71Segment(seg string) bool {
	ext := strings.TrimSpace(filepath.Ext(seg))
	return strings.EqualFold(ext, ".d71")
}

// splitD81Path checks whether p contains a ".d81" segment and splits it into:
//
//	mountPath: the path up to and including the .d81 segment
//	innerPath: the remaining path inside the image ("" means image root)
func splitD81Path(p string) (mountPath, innerPath string, ok bool) {
	if p == "" || p[0] != '/' {
		return "", "", false
	}
	trim := strings.TrimPrefix(p, "/")
	if trim == "" {
		return "", "", false
	}
	segs := strings.Split(trim, "/")
	for i, seg := range segs {
		if isD81Segment(seg) {
			mountPath = "/" + strings.Join(segs[:i+1], "/")
			if i+1 < len(segs) {
				innerPath = strings.Join(segs[i+1:], "/")
			} else {
				innerPath = ""
			}
			return mountPath, innerPath, true
		}
	}
	return "", "", false
}

func isD81Segment(seg string) bool {
	ext := strings.TrimSpace(filepath.Ext(seg))
	return strings.EqualFold(ext, ".d81")
}

// resolveD81Mount validates the mount path and loads/parses the image.
func resolveD81Mount(rootAbs string, mountPath string) (imgAbs string, img *diskimage.D81, status byte, msg string) {
	abs, err := fsops.ToOSPath(rootAbs, mountPath)
	if err != nil {
		return "", nil, proto.StatusInvalidPath, err.Error()
	}
	// First ensure the path contains no symlink components.
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, proto.StatusNotFound, "image not found"
		}
		return "", nil, proto.StatusInvalidPath, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return "", nil, proto.StatusInternal, err.Error()
	}
	if !st.Exists || st.IsDir {
		return "", nil, proto.StatusNotFound, "image not found"
	}

	img, err = diskimage.LoadD81(abs)
	if err != nil {
		// Treat invalid/unsupported images as "not found" rather than "internal".
		return "", nil, proto.StatusNotFound, "invalid or unsupported .d81 image"
	}
	return abs, img, proto.StatusOK, ""
}

// resolveD81Dir resolves dirPath inside a D81 image to a directory listing.
//
// dirPath is the path *inside* the image without a leading slash.
// "" means the image root.
func resolveD81Dir(img *diskimage.D81, dirPath string) (entries []*diskimage.FileEntry, byName map[string]*diskimage.FileEntry, dirT, dirS byte, status byte, msg string) {
	if img == nil {
		return nil, nil, 0, 0, proto.StatusInternal, "nil image"
	}

	// Root directory
	curT, curS := byte(40), byte(3)
	ents, m, err := img.Dir(curT, curS)
	if err != nil {
		return nil, nil, 0, 0, proto.StatusInternal, err.Error()
	}
	if dirPath == "" {
		return ents, m, curT, curS, proto.StatusOK, ""
	}

	// Traverse subdirectories
	segs := strings.Split(strings.Trim(dirPath, "/"), "/")
	for _, seg := range segs {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		if strings.ContainsAny(seg, "*?") {
			return nil, nil, 0, 0, proto.StatusInvalidPath, "wildcards not allowed in directory path"
		}
		key := strings.ToUpper(seg)
		de, ok := m[key]
		if !ok || de == nil {
			return nil, nil, 0, 0, proto.StatusNotFound, "directory not found"
		}
		if de.Type != 6 && de.Type != 5 {
			return nil, nil, 0, 0, proto.StatusNotADir, "not a directory"
		}

		curT, curS = de.StartTrack, de.StartSector
		ents, m, err = img.Dir(curT, curS)
		if err != nil {
			return nil, nil, 0, 0, proto.StatusInternal, err.Error()
		}
	}
	return ents, m, curT, curS, proto.StatusOK, ""
}

// resolveD81Inner resolves an inner path inside a D81 image to a file entry.
// Supports wildcards (*, ?) in the last segment.
func resolveD81Inner(img *diskimage.D81, innerPath string, fallbackPRG bool) (name string, fe *diskimage.FileEntry, status byte, msg string) {
	innerPath = strings.Trim(innerPath, "/")
	if innerPath == "" {
		return "", nil, proto.StatusInvalidPath, "missing inner path"
	}

	// Split into directory path + leaf
	var dirPath, leaf string
	if i := strings.LastIndex(innerPath, "/"); i >= 0 {
		dirPath = innerPath[:i]
		leaf = innerPath[i+1:]
	} else {
		dirPath = ""
		leaf = innerPath
	}
	leaf = strings.TrimSpace(leaf)
	if leaf == "" {
		return "", nil, proto.StatusInvalidPath, "missing file name"
	}

	_, m, _, _, st, emsg := resolveD81Dir(img, dirPath)
	if st != proto.StatusOK {
		return "", nil, st, emsg
	}

	// Exact match (case-insensitive)
	if !strings.ContainsAny(leaf, "*?") {
		key := strings.ToUpper(strings.TrimSpace(leaf))
		if fe, ok := m[key]; ok && fe != nil {
			if fe.Type == 6 || fe.Type == 5 {
				return strings.ToUpper(fe.Name), nil, proto.StatusIsADir, "is a directory"
			}
			return strings.ToUpper(fe.Name), fe, proto.StatusOK, ""
		}
		// Optional ".PRG" extension fallback
		if fallbackPRG && !strings.Contains(leaf, ".") {
			cand := key + ".PRG"
			if fe, ok := m[cand]; ok && fe != nil {
				if fe.Type == 6 || fe.Type == 5 {
					return strings.ToUpper(fe.Name), nil, proto.StatusIsADir, "is a directory"
				}
				return strings.ToUpper(fe.Name), fe, proto.StatusOK, ""
			}
		}
		return "", nil, proto.StatusNotFound, "file not found"
	}

	// Wildcard match (stable)
	pat := strings.ToUpper(leaf)
	names := make([]string, 0, len(m))
	for nm := range m {
		names = append(names, nm)
	}
	sort.Strings(names)
	for _, nm := range names {
		if wildcardMatch(pat, nm) {
			fe := m[nm]
			if fe == nil {
				continue
			}
			if fe.Type == 6 || fe.Type == 5 {
				return nm, nil, proto.StatusIsADir, "is a directory"
			}
			return nm, fe, proto.StatusOK, ""
		}
	}
	return "", nil, proto.StatusNotFound, "file not found"
}

func readD81FileRange(imgAbs string, fe *diskimage.FileEntry, offset, length uint64) ([]byte, error) {
	return diskimage.ReadFileRange(imgAbs, fe, offset, length)
}

func crc32D81File(imgAbs string, fe *diskimage.FileEntry) (uint32, error) {
	f, err := os.Open(imgAbs)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	buf := make([]byte, 256)
	for _, sec := range fe.Sectors {
		_, err := f.ReadAt(buf, sec.Offset)
		if err != nil {
			return 0, err
		}
		data := buf[2 : 2+sec.DataLen]
		_, _ = h.Write(data)
	}
	return h.Sum32(), nil
}

// resolveD64Mount validates the mount path and loads/parses the image.
func resolveD64Mount(rootAbs string, mountPath string) (imgAbs string, img *diskimage.D64, status byte, msg string) {
	abs, err := fsops.ToOSPath(rootAbs, mountPath)
	if err != nil {
		return "", nil, proto.StatusInvalidPath, err.Error()
	}
	// First ensure the path contains no symlink components.
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, proto.StatusNotFound, "image not found"
		}
		return "", nil, proto.StatusInvalidPath, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return "", nil, proto.StatusInternal, err.Error()
	}
	if !st.Exists || st.IsDir {
		return "", nil, proto.StatusNotFound, "image not found"
	}

	img, err = diskimage.LoadD64(abs)
	if err != nil {
		// Treat invalid/unsupported images as "not found" rather than "internal".
		return "", nil, proto.StatusNotFound, "invalid or unsupported .d64 image"
	}
	return abs, img, proto.StatusOK, ""
}

// resolveD64Inner resolves an inner path inside a D64 image to a file entry.
// Supports wildcards (*, ?) in the last segment.
func resolveD64Inner(img *diskimage.D64, innerPath string, fallbackPRG bool) (name string, fe *diskimage.FileEntry, status byte, msg string) {
	if innerPath == "" {
		return "", nil, proto.StatusInvalidPath, "missing inner path"
	}
	// D64 has no subdirectories; treat nested segments as not found.
	if strings.Contains(innerPath, "/") {
		return "", nil, proto.StatusNotFound, "file not found"
	}

	// Exact match (case-insensitive)
	if !strings.ContainsAny(innerPath, "*?") {
		if fe, ok := img.Lookup(innerPath); ok {
			return strings.ToUpper(fe.Name), fe, proto.StatusOK, ""
		}
		// Optional ".PRG" extension fallback
		if fallbackPRG && !strings.Contains(innerPath, ".") {
			cand := innerPath + ".PRG"
			if fe, ok := img.Lookup(cand); ok {
				return strings.ToUpper(fe.Name), fe, proto.StatusOK, ""
			}
		}
		return "", nil, proto.StatusNotFound, "file not found"
	}

	// Wildcard match (stable)
	pat := strings.ToUpper(innerPath)
	names := make([]string, 0, len(img.Files))
	for _, f := range img.Files {
		names = append(names, strings.ToUpper(f.Name))
	}
	sort.Strings(names)
	for _, nm := range names {
		if wildcardMatch(pat, nm) {
			if fe, ok := img.Lookup(nm); ok {
				return nm, fe, proto.StatusOK, ""
			}
		}
	}
	return "", nil, proto.StatusNotFound, "file not found"
}

func readD64FileRange(imgAbs string, fe *diskimage.FileEntry, offset, length uint64) ([]byte, error) {
	return diskimage.ReadFileRange(imgAbs, fe, offset, length)
}

func crc32D64File(imgAbs string, fe *diskimage.FileEntry) (uint32, error) {
	f, err := os.Open(imgAbs)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	buf := make([]byte, 256)
	for _, sec := range fe.Sectors {
		_, err := f.ReadAt(buf, sec.Offset)
		if err != nil {
			return 0, err
		}
		data := buf[2 : 2+sec.DataLen]
		_, _ = h.Write(data)
	}
	return h.Sum32(), nil
}

func resolveD71Mount(rootAbs, mountPath string) (imgAbs string, img *diskimage.D71, status byte, msg string) {
	abs, err := fsops.ToOSPath(rootAbs, mountPath)
	if err != nil {
		return "", nil, proto.StatusBadPath, "invalid path"
	}
	abs = filepath.Clean(abs)

	// Ensure the image itself is not a symlink (and no symlink components
	// exist in the path).
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, proto.StatusNotFound, "image not found"
		}
		return "", nil, proto.StatusInvalidPath, err.Error()
	}

	st, err := fsops.Stat(abs)
	if err != nil {
		return "", nil, proto.StatusInternal, err.Error()
	}
	if !st.Exists || st.IsDir {
		return "", nil, proto.StatusNotFound, "image not found"
	}

	img, err = diskimage.LoadD71(abs)
	if err != nil {
		return "", nil, proto.StatusNotFound, "invalid or unsupported .d71 image"
	}
	return abs, img, proto.StatusOK, ""
}

func resolveD71Inner(img *diskimage.D71, innerPath string, fallbackPRG bool) (resolvedName string, fe *diskimage.FileEntry, status byte, msg string) {
	if innerPath == "" {
		return "", nil, proto.StatusIsADir, "is a directory"
	}
	if strings.Contains(innerPath, "/") {
		return "", nil, proto.StatusNotADir, "not a directory"
	}

	name := strings.ToUpper(innerPath)

	// Wildcards: pick the first sorted match.
	if strings.ContainsAny(name, "*?") {
		entries := img.SortedEntries()
		for _, e := range entries {
			if wildcardMatch(name, strings.ToUpper(e.Name)) {
				return strings.ToUpper(e.Name), e, proto.StatusOK, ""
			}
		}
		return "", nil, proto.StatusNotFound, "file not found"
	}

	if e, ok := img.Lookup(name); ok {
		return name, e, proto.StatusOK, ""
	}
	if fallbackPRG && !strings.Contains(innerPath, ".") {
		if e, ok := img.Lookup(name + ".PRG"); ok {
			return name + ".PRG", e, proto.StatusOK, ""
		}
	}
	return "", nil, proto.StatusNotFound, "file not found"
}

func readD71FileRange(imgAbs string, fe *diskimage.FileEntry, offset, length uint64) ([]byte, error) {
	return diskimage.ReadFileRange(imgAbs, fe, offset, length)
}

func crc32D71File(imgAbs string, fe *diskimage.FileEntry) (uint32, error) {
	f, err := os.Open(imgAbs)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	buf := make([]byte, 256)
	for _, sec := range fe.Sectors {
		_, err := f.ReadAt(buf, sec.Offset)
		if err != nil {
			return 0, err
		}
		data := buf[2 : 2+sec.DataLen]
		_, _ = h.Write(data)
	}
	return h.Sum32(), nil
}
