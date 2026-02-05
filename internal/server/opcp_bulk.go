package server

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"wicos64-server/internal/config"
	"wicos64-server/internal/diskimage"
	"wicos64-server/internal/fsops"
	"wicos64-server/internal/proto"
)

// cpBulkFS copies all matching entries from a filesystem directory into an existing destination directory.
// "Strict" behavior: dst must exist and be a directory. Only the last segment of src may contain wildcards.
func (s *Server) cpBulkFS(cfg config.Config, limits Limits, rootAbs, srcDirNorm, srcPat, dstNorm string, overwrite, recursive bool) (byte, string) {
	srcDirAbs, err := fsops.ToOSPath(rootAbs, srcDirNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	dstDirAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}

	if err := fsops.LstatNoSymlink(rootAbs, srcDirAbs, false); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	srcDirSt, err := fsops.Stat(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !srcDirSt.Exists {
		return proto.StatusNotFound, "source directory not found"
	}
	if !srcDirSt.IsDir {
		return proto.StatusNotADir, "source is not a directory"
	}

	if err := fsops.LstatNoSymlink(rootAbs, dstDirAbs, false); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	dstDirSt, err := fsops.Stat(dstDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !dstDirSt.Exists {
		return proto.StatusNotFound, "destination directory not found"
	}
	if !dstDirSt.IsDir {
		return proto.StatusNotADir, "destination is not a directory"
	}

	entries, err := os.ReadDir(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	// Deterministic order (case-insensitive).
	sort.Slice(entries, func(i, j int) bool {
		return strings.ToUpper(entries[i].Name()) < strings.ToUpper(entries[j].Name())
	})

	// Preload quota usage once (if enabled).
	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	copied := 0

	for _, de := range entries {
		name := de.Name()
		if !wildcardMatch(srcPat, strings.ToUpper(name)) {
			continue
		}

		srcAbs := filepath.Join(srcDirAbs, name)

		// Reject symlinks (and traversal) at the source entry itself.
		if err := fsops.LstatNoSymlink(rootAbs, srcAbs, false); err != nil {
			return proto.StatusInvalidPath, err.Error()
		}

		info, err := de.Info()
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		isDir := info.IsDir()
		if isDir && !recursive {
			// Strict-but-practical: skip directories unless recursive is explicitly requested.
			continue
		}

		dstAbs := filepath.Join(dstDirAbs, name)

		// Size accounting for quota checks.
		var srcTotal uint64
		var srcMax uint64
		if isDir {
			srcTotal, srcMax, err = pathSizeBytes(srcAbs)
			if err != nil {
				return proto.StatusInvalidPath, err.Error()
			}
		} else {
			srcTotal = uint64(info.Size())
			srcMax = srcTotal
		}
		if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		// Destination stat.
		dstSt, err := fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if dstSt.Exists && !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}

		// If not using trash for overwrite, compute old size (can reduce quota impact).
		var dstOldTotal uint64
		if dstSt.Exists && !trashOverwrite {
			dstOldTotal, _, err = pathSizeBytes(dstAbs)
			if err != nil {
				return proto.StatusInternal, err.Error()
			}
		}

		delta := int64(srcTotal) - int64(dstOldTotal)
		if trashOverwrite && dstSt.Exists {
			delta = int64(srcTotal)
		}

		if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
			return proto.StatusTooLarge, "quota exceeded"
		}

		// Overwrite handling.
		if dstSt.Exists {
			if trashOverwrite {
				if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
			} else {
				if err := os.RemoveAll(dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
				s.invalidateRootUsage(rootAbs)
			}
		}

		// Copy.
		if isDir {
			if err := fsops.CopyDirRecursive(srcAbs, dstAbs); err != nil {
				s.invalidateRootUsage(rootAbs)
				return proto.StatusInternal, err.Error()
			}
		} else {
			if err := fsops.CopyFile(srcAbs, dstAbs); err != nil {
				s.invalidateRootUsage(rootAbs)
				return proto.StatusInternal, err.Error()
			}
		}

		// Update cached usage as we go.
		if haveUsed && s.usage != nil {
			used = applyDeltaBytes(used, delta)
			s.setRootUsageBytes(rootAbs, used)
		}

		copied++
	}

	if copied == 0 {
		return proto.StatusNotFound, "no matching files"
	}

	return proto.StatusOK, fmt.Sprintf("copied %d item(s)", copied)
}

// cpFromD64 extracts one or more files from a mounted D64 image to the filesystem.
func (s *Server) cpFromD64(cfg config.Config, limits Limits, rootAbs, mountPath, inner, dstNorm string, overwrite, recursive bool) (byte, string) {
	if strings.Contains(inner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	if strings.ContainsAny(inner, "*?") {
		return s.cpBulkFromD64(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite)
	}
	return s.cpSingleFromD64(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite)
}

func (s *Server) cpSingleFromD64(cfg config.Config, limits Limits, rootAbs, mountPath, inner, dstNorm string, overwrite bool) (byte, string) {
	// Resolve mount & entry first (do not delete destination before we know we can read).
	imgAbs, img, st, msg := resolveD64Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}
	_, fe, st, msg := resolveD64Inner(img, inner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}

	srcTotal := uint64(fe.Size)
	srcMax := srcTotal
	if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
		return proto.StatusTooLarge, "file too large"
	}

	dstAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}

	// If dst is an existing directory, write into it using the request leaf name.
	dstSt, err := fsops.Stat(dstAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if dstSt.Exists && dstSt.IsDir {
		dstAbs = filepath.Join(dstAbs, inner)
		dstSt, err = fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	if err := fsops.LstatNoSymlink(rootAbs, dstAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.EnsureParents(dstAbs); err != nil {
		return proto.StatusInternal, err.Error()
	}

	// Quota baseline.
	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	var dstOldTotal uint64
	if dstSt.Exists && !trashOverwrite {
		dstOldTotal, _, err = pathSizeBytes(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	delta := int64(srcTotal) - int64(dstOldTotal)
	if trashOverwrite && dstSt.Exists {
		delta = int64(srcTotal)
	}

	if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
		return proto.StatusTooLarge, "quota exceeded"
	}

	// Read before touching destination.
	data, err := readD64FileRange(imgAbs, fe, 0, srcTotal)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	if dstSt.Exists {
		if !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}
		if trashOverwrite {
			if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
		} else {
			if err := os.RemoveAll(dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
			s.invalidateRootUsage(rootAbs)
		}
	}

	if err := os.WriteFile(dstAbs, data, 0o644); err != nil {
		s.invalidateRootUsage(rootAbs)
		return proto.StatusInternal, err.Error()
	}

	if haveUsed && s.usage != nil {
		used = applyDeltaBytes(used, delta)
		s.setRootUsageBytes(rootAbs, used)
	}

	return proto.StatusOK, ""
}

// ---- Common helpers for filesystem -> disk image copy paths ----

func imageDstLeaf(kind string, dstInner string, srcNorm string, fallbackPRG bool) (string, byte, string) {
	// Normalize the optional "inside image" path:
	// - allow leading/trailing slashes
	// - empty means "use source basename"
	name := strings.Trim(strings.TrimSpace(dstInner), "/")
	if name == "" {
		name = path.Base(srcNorm)
	}
	name = normalizeDiskImageLeafName(name, fallbackPRG)
	name = strings.Trim(strings.TrimSpace(name), "/")

	if name == "" {
		return "", proto.StatusBadPath, "missing destination filename"
	}
	if strings.Contains(name, "/") {
		switch strings.ToUpper(kind) {
		case "D64":
			return "", proto.StatusNotSupported, "D64 images do not support subdirectories"
		case "D71":
			return "", proto.StatusNotSupported, "D71 images do not support subdirectories"
		default:
			return "", proto.StatusNotSupported, "subdirectories are not supported"
		}
	}
	if strings.ContainsAny(name, "*?") {
		return "", proto.StatusBadRequest, "wildcards are not allowed in destination names"
	}
	return name, proto.StatusOK, ""
}

func (s *Server) statFSSourceForImageCopy(rootAbs, srcNorm string) (string, fsops.StatInfo, byte, string) {
	srcAbs, err := fsops.ToOSPath(rootAbs, srcNorm)
	if err != nil {
		return "", fsops.StatInfo{}, proto.StatusInvalidPath, err.Error()
	}
	// allowMissingLast=true so a missing file can be surfaced as NOT_FOUND (not INVALID_PATH)
	if err := fsops.LstatNoSymlink(rootAbs, srcAbs, true); err != nil {
		return "", fsops.StatInfo{}, proto.StatusInvalidPath, err.Error()
	}
	stInfo, err := fsops.Stat(srcAbs)
	if err != nil {
		return "", fsops.StatInfo{}, proto.StatusInternal, err.Error()
	}
	if !stInfo.Exists {
		return srcAbs, stInfo, proto.StatusNotFound, "source not found"
	}
	return srcAbs, stInfo, proto.StatusOK, ""
}

func (s *Server) readFSFileForImageCopy(rootAbs, srcNorm string, limits Limits) ([]byte, byte, string) {
	srcAbs, stInfo, st, msg := s.statFSSourceForImageCopy(rootAbs, srcNorm)
	if st != proto.StatusOK {
		return nil, st, msg
	}
	if stInfo.IsDir {
		return nil, proto.StatusIsADir, "source is a directory"
	}
	if limits.MaxFileBytes > 0 && stInfo.Size > limits.MaxFileBytes {
		return nil, proto.StatusTooLarge, "file too large"
	}
	data, err := os.ReadFile(srcAbs)
	if err != nil {
		return nil, proto.StatusInternal, err.Error()
	}
	return data, proto.StatusOK, ""
}

// cpToD64 copies a *single* file into a mounted .d64 image.
//
// Destination semantics:
//   - mountPath is the mount path (e.g. "/disks/blank.d64")
//   - dstInner is the inner file name (e.g. "HELLO"), or "" to mean "image root"
//     (in which case the source base name is used).
//
// NOTE: .d64 does not support subdirectories.
func (s *Server) cpToD64(cfg config.Config, limits Limits, rootAbs, mountPath, dstInner, srcNorm string, overwrite bool) (byte, string) {
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk images are read-only"
	}

	name, st, msg := imageDstLeaf("D64", dstInner, srcNorm, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}

	imgAbs, _, st, msg := resolveD64Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}

	data, st, msg := s.readFSFileForImageCopy(rootAbs, srcNorm, limits)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err := diskimage.WriteFileRangeD64(imgAbs, name, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpToD71 copies a single filesystem file into the root of a mounted .d71 image.
//
// dstInner may be empty (then the basename of src is used).
// Root directory only (D71 has no subdirectories).
func (s *Server) cpToD71(cfg config.Config, limits Limits, rootAbs, mountPath, dstInner, srcNorm string, overwrite bool) (byte, string) {
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk images are read-only"
	}

	dstName, st, msg := imageDstLeaf("D71", dstInner, srcNorm, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}

	imgAbs, _, st, msg := resolveD71Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}

	data, st, msg := s.readFSFileForImageCopy(rootAbs, srcNorm, limits)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err := diskimage.WriteFileRangeD71(imgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpToD81 copies a single filesystem file into a mounted .d81 image.
//
// Destination semantics:
//   - If dstInner is empty (mount root) OR refers to an existing directory/partition inside the image,
//     the file is copied into that directory using the source file name.
//   - Otherwise dstInner is treated as the full destination file path inside the image.
func (s *Server) cpToD81(cfg config.Config, limits Limits, rootAbs, mountPath, dstInner, srcNorm string, overwrite, recursive bool) (byte, string) {
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk images are read-only"
	}

	// Normalize optional ".PRG" suffix for compatibility when enabled.
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}

	srcLeaf := normalizeDiskImageLeafName(path.Base(srcNorm), cfg.Compat.FallbackPRGExtension)

	// Determine final destination path inside the image. For file copies this is a file path.
	// For directory copies (recursive), this represents the destination directory path.
	finalInner := strings.Trim(strings.TrimSpace(dstInner), "/")
	if finalInner == "" {
		finalInner = srcLeaf
	} else if d81InnerIsDirEntry(img, finalInner) {
		finalInner = finalInner + "/" + srcLeaf
	}
	if strings.ContainsAny(finalInner, "*?") {
		return proto.StatusBadRequest, "wildcards not allowed"
	}

	srcAbs, stInfo, st, msg := s.statFSSourceForImageCopy(rootAbs, srcNorm)
	if st != proto.StatusOK {
		return st, msg
	}
	if stInfo.IsDir {
		if !recursive {
			return proto.StatusIsADir, "source is a directory"
		}
		allowOverwrite := cfg.EnableOverwrite && overwrite
		// Import the directory tree into the image by repacking. This also creates required
		// 1581 "partition" directories and ensures they occupy contiguous tracks.
		err := diskimage.ImportDirD81(imgAbs, finalInner, srcAbs, allowOverwrite, cfg.Compat.FallbackPRGExtension, limits.MaxFileBytes)
		if err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), se.Error()
			}
			return proto.StatusInternal, err.Error()
		}
		return proto.StatusOK, ""
	}
	if limits.MaxFileBytes > 0 && stInfo.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "file too large"
	}

	data, err := os.ReadFile(srcAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD81(imgAbs, finalInner, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpBulkFSToD64 copies wildcard-matched files from a filesystem directory into the *root*
// of a mounted .d64 image.
func (s *Server) cpBulkFSToD64(cfg config.Config, limits Limits, rootAbs, srcDirNorm, pat, dstMount string, overwrite bool) (byte, string) {
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk images are read-only"
	}

	if strings.Contains(srcDirNorm, "*") || strings.Contains(srcDirNorm, "?") {
		return proto.StatusBadRequest, "wildcards are only allowed in the last path segment"
	}

	srcDirAbs, err := fsops.ToOSPath(rootAbs, srcDirNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, srcDirAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	srcSt, err := fsops.Stat(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !srcSt.Exists {
		return proto.StatusNotFound, "source directory not found"
	}
	if !srcSt.IsDir {
		return proto.StatusNotADir, "source is not a directory"
	}

	imgAbs, _, st, msg := resolveD64Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	entries, err := os.ReadDir(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite

	for _, e := range entries {
		name := e.Name()
		if !wildcardMatch(pat, strings.ToUpper(name)) {
			continue
		}
		if e.IsDir() {
			// .d64 has no directories; skip.
			continue
		}

		full := filepath.Join(srcDirAbs, name)
		st, err := fsops.Stat(full)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if st.IsDir {
			continue
		}
		if limits.MaxFileBytes > 0 && uint64(st.Size) > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		data, err := os.ReadFile(full)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		imgName := normalizeDiskImageLeafName(name, cfg.Compat.FallbackPRGExtension)
		_, err = diskimage.WriteFileRangeD64(imgAbs, imgName, 0, data, true, true, allowOverwrite)
		if err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), se.Error()
			}
			return proto.StatusInternal, err.Error()
		}
	}

	return proto.StatusOK, ""
}

// cpBulkFSToD71 copies wildcard-matched files from a filesystem directory into the *root*
// of a mounted .d71 image.
func (s *Server) cpBulkFSToD71(cfg config.Config, limits Limits, rootAbs, srcDirNorm, pat, dstMount string, overwrite bool) (byte, string) {
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk images are read-only"
	}

	if strings.Contains(srcDirNorm, "*") || strings.Contains(srcDirNorm, "?") {
		return proto.StatusBadRequest, "wildcards are only allowed in the last path segment"
	}

	srcDirAbs, err := fsops.ToOSPath(rootAbs, srcDirNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, srcDirAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	srcSt, err := fsops.Stat(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !srcSt.Exists {
		return proto.StatusNotFound, "source directory not found"
	}
	if !srcSt.IsDir {
		return proto.StatusNotADir, "source is not a directory"
	}

	imgAbs, _, st, msg := resolveD71Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	entries, err := os.ReadDir(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite

	for _, e := range entries {
		name := e.Name()
		if !wildcardMatch(pat, strings.ToUpper(name)) {
			continue
		}
		if e.IsDir() {
			// .d71 has no directories; skip.
			continue
		}

		full := filepath.Join(srcDirAbs, name)
		st, err := fsops.Stat(full)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if st.IsDir {
			continue
		}
		if limits.MaxFileBytes > 0 && uint64(st.Size) > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		data, err := os.ReadFile(full)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		imgName := normalizeDiskImageLeafName(name, cfg.Compat.FallbackPRGExtension)
		_, err = diskimage.WriteFileRangeD71(imgAbs, imgName, 0, data, true, true, allowOverwrite)
		if err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), se.Error()
			}
			return proto.StatusInternal, err.Error()
		}
	}

	return proto.StatusOK, ""
}

func (s *Server) cpBulkFSToD81(cfg config.Config, limits Limits, rootAbs, srcDirNorm, pat, dstMount string, overwrite bool) (byte, string) {
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk images are read-only"
	}

	if strings.Contains(srcDirNorm, "*") || strings.Contains(srcDirNorm, "?") {
		return proto.StatusBadRequest, "wildcards are only allowed in the last path segment"
	}

	srcDirAbs, err := fsops.ToOSPath(rootAbs, srcDirNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, srcDirAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	srcSt, err := fsops.Stat(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !srcSt.Exists {
		return proto.StatusNotFound, "source directory not found"
	}
	if !srcSt.IsDir {
		return proto.StatusNotADir, "source is not a directory"
	}

	imgAbs, _, st, msg := resolveD81Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	entries, err := os.ReadDir(srcDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite

	for _, e := range entries {
		name := e.Name()
		if !wildcardMatch(pat, strings.ToUpper(name)) {
			continue
		}
		if e.IsDir() {
			// We currently only copy files.
			continue
		}

		full := filepath.Join(srcDirAbs, name)
		st, err := fsops.Stat(full)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if st.IsDir {
			continue
		}
		if limits.MaxFileBytes > 0 && uint64(st.Size) > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		data, err := os.ReadFile(full)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		imgName := normalizeDiskImageLeafName(name, cfg.Compat.FallbackPRGExtension)
		_, err = diskimage.WriteFileRangeD81(imgAbs, imgName, 0, data, true, true, allowOverwrite)
		if err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), se.Error()
			}
			return proto.StatusInternal, err.Error()
		}
	}

	return proto.StatusOK, ""
}

func (s *Server) cpBulkFromD64(cfg config.Config, limits Limits, rootAbs, mountPath, pat, dstNorm string, overwrite bool) (byte, string) {
	dstDirAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, dstDirAbs, false); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	dstDirSt, err := fsops.Stat(dstDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !dstDirSt.Exists {
		return proto.StatusNotFound, "destination directory not found"
	}
	if !dstDirSt.IsDir {
		return proto.StatusNotADir, "destination is not a directory"
	}

	imgAbs, img, st, msg := resolveD64Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}

	files := img.SortedEntries()

	// Quota baseline.
	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	copied := 0

	for _, fe := range files {
		if fe.Type == 0 || fe.Type == 1 {
			continue
		}

		// "display name" (aligns with LS output when PRG fallback extension is enabled).
		name := strings.TrimSpace(fe.Name)
		if cfg.Compat.FallbackPRGExtension && fe.Type == 2 {
			name += ".PRG"
		}
		if !wildcardMatch(pat, strings.ToUpper(name)) {
			continue
		}

		srcTotal := uint64(fe.Size)
		srcMax := srcTotal
		if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		dstAbs := filepath.Join(dstDirAbs, name)

		// Destination stat.
		dstSt, err := fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if dstSt.Exists && !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}

		var dstOldTotal uint64
		if dstSt.Exists && !trashOverwrite {
			dstOldTotal, _, err = pathSizeBytes(dstAbs)
			if err != nil {
				return proto.StatusInternal, err.Error()
			}
		}

		delta := int64(srcTotal) - int64(dstOldTotal)
		if trashOverwrite && dstSt.Exists {
			delta = int64(srcTotal)
		}

		if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
			return proto.StatusTooLarge, "quota exceeded"
		}

		// Read before touching destination.
		data, err := readD64FileRange(imgAbs, fe, 0, srcTotal)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		if dstSt.Exists {
			if trashOverwrite {
				if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
			} else {
				if err := os.RemoveAll(dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
				s.invalidateRootUsage(rootAbs)
			}
		}

		if err := os.WriteFile(dstAbs, data, 0o644); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, err.Error()
		}

		if haveUsed && s.usage != nil {
			used = applyDeltaBytes(used, delta)
			s.setRootUsageBytes(rootAbs, used)
		}

		copied++
	}

	if copied == 0 {
		return proto.StatusNotFound, "no matching files"
	}

	return proto.StatusOK, fmt.Sprintf("copied %d item(s)", copied)
}

// cpD71ToD71 copies a file within/between mounted D71 images.
func (s *Server) cpD71ToD71(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	// Normalize optional ".PRG" suffix for compatibility when enabled.
	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	// Wildcards inside disk images are not supported for CP yet (file-only for now).
	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}

	// D71 has no subdirectories.
	if strings.Contains(srcInner, "/") {
		return proto.StatusNotFound, "not found"
	}
	if dstInner != "" && strings.Contains(dstInner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	srcImgAbs, srcImg, st, msg := resolveD71Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD71Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}

	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD71FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstName := strings.TrimSpace(dstInner)
	if dstName == "" {
		dstName = srcName
	} else {
		dstName = normalizeDiskImageLeafName(dstName, cfg.Compat.FallbackPRGExtension)
	}

	dstImgAbs, _, st, msg := resolveD71Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD71(dstImgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		if se, ok := err.(*diskimage.StatusError); ok {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// d81InnerIsDirEntry returns true if innerPath refers to an existing directory/partition
// inside the given D81 image.
//
// Unlike resolveD81Dir, this does not require the directory itself to be readable as a
// directory chain; it only checks the directory entry type in the parent directory.
// This is useful for copy semantics where the destination is allowed to be a directory
// target even if its contents are currently empty or non-standard.
func d81InnerIsDirEntry(img *diskimage.D81, innerPath string) bool {
	inner := strings.Trim(strings.TrimSpace(innerPath), "/")
	if inner == "" {
		return true
	}

	parent := ""
	leaf := inner
	if i := strings.LastIndex(inner, "/"); i >= 0 {
		parent = inner[:i]
		leaf = inner[i+1:]
	}

	_, m, _, _, st, _ := resolveD81Dir(img, parent)
	if st != proto.StatusOK {
		return false
	}
	key := strings.ToUpper(strings.TrimSpace(leaf))
	fe, ok := m[key]
	if !ok || fe == nil {
		return false
	}
	return fe.Type == 5 || fe.Type == 6
}

// d81CopyDestInner determines the final inner path for a copy target inside a D81 image.
//
// Semantics:
//   - If dstInner is empty, copy into the image root using srcName.
//   - If dstInner refers to an existing directory/partition, copy into that directory using srcName.
//   - Otherwise treat dstInner as the full destination file path inside the image.
func d81CopyDestInner(dstImg *diskimage.D81, dstInner, srcName string) (string, byte, string) {
	finalInner := strings.Trim(strings.TrimSpace(dstInner), "/")
	if finalInner == "" {
		return srcName, proto.StatusOK, ""
	}
	if d81InnerIsDirEntry(dstImg, finalInner) {
		return finalInner + "/" + srcName, proto.StatusOK, ""
	}
	return finalInner, proto.StatusOK, ""
}

// cpD64ToD64 copies a file within/between mounted D64 images.
func (s *Server) cpD64ToD64(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	// Normalize optional ".PRG" suffix for compatibility when enabled.
	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	// Wildcards inside disk images are not supported for CP yet (file-only for now).
	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}

	// D64 has no subdirectories.
	if strings.Contains(srcInner, "/") {
		return proto.StatusNotFound, "not found"
	}
	if dstInner != "" && strings.Contains(dstInner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	srcImgAbs, srcImg, st, msg := resolveD64Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD64Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD64FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstName := strings.TrimSpace(dstInner)
	if dstName == "" {
		dstName = srcName
	} else {
		dstName = normalizeDiskImageLeafName(dstName, cfg.Compat.FallbackPRGExtension)
	}

	dstImgAbs, _, st, msg := resolveD64Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD64(dstImgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD64ToD71 copies a file from a mounted D64 into a mounted D71.
func (s *Server) cpD64ToD71(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}
	if strings.Contains(srcInner, "/") {
		return proto.StatusNotFound, "not found"
	}
	if dstInner != "" && strings.Contains(dstInner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	srcImgAbs, srcImg, st, msg := resolveD64Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD64Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD64FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstName := strings.TrimSpace(dstInner)
	if dstName == "" {
		dstName = srcName
	} else {
		dstName = normalizeDiskImageLeafName(dstName, cfg.Compat.FallbackPRGExtension)
	}

	dstImgAbs, _, st, msg := resolveD71Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD71(dstImgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD64ToD81 copies a file from a mounted D64 into a mounted D81.
func (s *Server) cpD64ToD81(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}
	if strings.Contains(srcInner, "/") {
		return proto.StatusNotFound, "not found"
	}

	srcImgAbs, srcImg, st, msg := resolveD64Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD64Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD64FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstImgAbs, dstImg, st, msg := resolveD81Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	finalInner, st2, msg2 := d81CopyDestInner(dstImg, dstInner, srcName)
	if st2 != proto.StatusOK {
		return st2, msg2
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD81(dstImgAbs, finalInner, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD71ToD64 copies a file from a mounted D71 into a mounted D64.
func (s *Server) cpD71ToD64(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}
	if strings.Contains(srcInner, "/") {
		return proto.StatusNotFound, "not found"
	}
	if dstInner != "" && strings.Contains(dstInner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	srcImgAbs, srcImg, st, msg := resolveD71Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD71Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD71FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstName := strings.TrimSpace(dstInner)
	if dstName == "" {
		dstName = srcName
	} else {
		dstName = normalizeDiskImageLeafName(dstName, cfg.Compat.FallbackPRGExtension)
	}

	dstImgAbs, _, st, msg := resolveD64Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD64(dstImgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD71ToD81 copies a file from a mounted D71 into a mounted D81.
func (s *Server) cpD71ToD81(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}
	if strings.Contains(srcInner, "/") {
		return proto.StatusNotFound, "not found"
	}

	srcImgAbs, srcImg, st, msg := resolveD71Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD71Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD71FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstImgAbs, dstImg, st, msg := resolveD81Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	finalInner, st2, msg2 := d81CopyDestInner(dstImg, dstInner, srcName)
	if st2 != proto.StatusOK {
		return st2, msg2
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD81(dstImgAbs, finalInner, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD81ToD64 copies a file from a mounted D81 into a mounted D64.
func (s *Server) cpD81ToD64(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}
	if dstInner != "" && strings.Contains(dstInner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	srcImgAbs, srcImg, st, msg := resolveD81Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD81Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD81FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstName := strings.TrimSpace(dstInner)
	if dstName == "" {
		dstName = srcName
	} else {
		dstName = normalizeDiskImageLeafName(dstName, cfg.Compat.FallbackPRGExtension)
	}

	dstImgAbs, _, st, msg := resolveD64Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD64(dstImgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD81ToD71 copies a file from a mounted D81 into a mounted D71.
func (s *Server) cpD81ToD71(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}
	if dstInner != "" && strings.Contains(dstInner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	srcImgAbs, srcImg, st, msg := resolveD81Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD81Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD81FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstName := strings.TrimSpace(dstInner)
	if dstName == "" {
		dstName = srcName
	} else {
		dstName = normalizeDiskImageLeafName(dstName, cfg.Compat.FallbackPRGExtension)
	}

	dstImgAbs, _, st, msg := resolveD71Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD71(dstImgAbs, dstName, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpD81ToD81 copies a file within/between mounted D81 images.
//
// Destination semantics:
//   - If dstInner is empty (mount root) OR refers to an existing directory/partition inside the image,
//     the source file is copied into that directory using the source file name.
//   - Otherwise dstInner is treated as the full destination file path inside the image.
func (s *Server) cpD81ToD81(cfg config.Config, limits Limits, rootAbs, srcMount, srcInner, dstMount, dstInner string, overwrite bool) (byte, string) {
	if !limits.DiskImagesEnabled {
		return proto.StatusNotSupported, "disk images are disabled"
	}
	if !limits.DiskImagesWriteEnabled {
		return proto.StatusAccessDenied, "disk image writes are disabled"
	}

	// Normalize optional ".PRG" suffix for compatibility when enabled.
	srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
	dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)

	// Wildcards inside disk images are not supported for CP yet (file-only for now).
	if strings.ContainsAny(srcInner, "*?") || strings.ContainsAny(dstInner, "*?") {
		return proto.StatusNotSupported, "wildcards in disk image paths are not supported for copy"
	}

	srcImgAbs, srcImg, st, msg := resolveD81Mount(rootAbs, srcMount)
	if st != proto.StatusOK {
		return st, msg
	}
	srcName, srcFe, st, msg := resolveD81Inner(srcImg, srcInner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcFe.Size > limits.MaxFileBytes {
		return proto.StatusTooLarge, "too large"
	}

	data, err := readD81FileRange(srcImgAbs, srcFe, 0, srcFe.Size)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	dstImgAbs, dstImg, st, msg := resolveD81Mount(rootAbs, dstMount)
	if st != proto.StatusOK {
		return st, msg
	}

	// Determine final destination path inside the image.
	finalInner := strings.Trim(strings.TrimSpace(dstInner), "/")
	if finalInner == "" {
		finalInner = srcName
	} else if d81InnerIsDirEntry(dstImg, finalInner) {
		finalInner = finalInner + "/" + srcName
	}

	allowOverwrite := cfg.EnableOverwrite && overwrite
	_, err = diskimage.WriteFileRangeD81(dstImgAbs, finalInner, 0, data, true, true, allowOverwrite)
	if err != nil {
		var se *diskimage.StatusError
		if errors.As(err, &se) {
			return se.Status(), se.Error()
		}
		return proto.StatusInternal, err.Error()
	}

	return proto.StatusOK, ""
}

// cpFromD71 extracts one or more files from a mounted D71 image to the filesystem.
func (s *Server) cpFromD71(cfg config.Config, limits Limits, rootAbs, mountPath, inner, dstNorm string, overwrite, recursive bool) (byte, string) {
	if strings.Contains(inner, "/") {
		return proto.StatusNotADir, "not a directory"
	}

	if strings.ContainsAny(inner, "*?") {
		return s.cpBulkFromD71(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite)
	}
	return s.cpSingleFromD71(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite)
}

func (s *Server) cpSingleFromD71(cfg config.Config, limits Limits, rootAbs, mountPath, inner, dstNorm string, overwrite bool) (byte, string) {
	imgAbs, img, st, msg := resolveD71Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}
	_, fe, st, msg := resolveD71Inner(img, inner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}

	srcTotal := uint64(fe.Size)
	srcMax := srcTotal
	if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
		return proto.StatusTooLarge, "file too large"
	}

	dstAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	dstSt, err := fsops.Stat(dstAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if dstSt.Exists && dstSt.IsDir {
		dstAbs = filepath.Join(dstAbs, inner)
		dstSt, err = fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	if err := fsops.LstatNoSymlink(rootAbs, dstAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.EnsureParents(dstAbs); err != nil {
		return proto.StatusInternal, err.Error()
	}

	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	var dstOldTotal uint64
	if dstSt.Exists && !trashOverwrite {
		dstOldTotal, _, err = pathSizeBytes(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	delta := int64(srcTotal) - int64(dstOldTotal)
	if trashOverwrite && dstSt.Exists {
		delta = int64(srcTotal)
	}

	if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
		return proto.StatusTooLarge, "quota exceeded"
	}

	data, err := readD71FileRange(imgAbs, fe, 0, srcTotal)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	if dstSt.Exists {
		if !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}
		if trashOverwrite {
			if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
		} else {
			if err := os.RemoveAll(dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
			s.invalidateRootUsage(rootAbs)
		}
	}

	if err := os.WriteFile(dstAbs, data, 0o644); err != nil {
		s.invalidateRootUsage(rootAbs)
		return proto.StatusInternal, err.Error()
	}

	if haveUsed && s.usage != nil {
		used = applyDeltaBytes(used, delta)
		s.setRootUsageBytes(rootAbs, used)
	}

	return proto.StatusOK, ""
}

func (s *Server) cpBulkFromD71(cfg config.Config, limits Limits, rootAbs, mountPath, pat, dstNorm string, overwrite bool) (byte, string) {
	dstDirAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, dstDirAbs, false); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	dstDirSt, err := fsops.Stat(dstDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !dstDirSt.Exists {
		return proto.StatusNotFound, "destination directory not found"
	}
	if !dstDirSt.IsDir {
		return proto.StatusNotADir, "destination is not a directory"
	}

	imgAbs, img, st, msg := resolveD71Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}

	files := img.SortedEntries()

	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	copied := 0

	for _, fe := range files {
		if fe.Type == 0 || fe.Type == 1 {
			continue
		}

		name := strings.TrimSpace(fe.Name)
		if cfg.Compat.FallbackPRGExtension && fe.Type == 2 {
			name += ".PRG"
		}
		if !wildcardMatch(pat, strings.ToUpper(name)) {
			continue
		}

		srcTotal := uint64(fe.Size)
		srcMax := srcTotal
		if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		dstAbs := filepath.Join(dstDirAbs, name)

		dstSt, err := fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if dstSt.Exists && !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}

		var dstOldTotal uint64
		if dstSt.Exists && !trashOverwrite {
			dstOldTotal, _, err = pathSizeBytes(dstAbs)
			if err != nil {
				return proto.StatusInternal, err.Error()
			}
		}

		delta := int64(srcTotal) - int64(dstOldTotal)
		if trashOverwrite && dstSt.Exists {
			delta = int64(srcTotal)
		}

		if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
			return proto.StatusTooLarge, "quota exceeded"
		}

		data, err := readD71FileRange(imgAbs, fe, 0, srcTotal)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		if dstSt.Exists {
			if trashOverwrite {
				if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
			} else {
				if err := os.RemoveAll(dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
				s.invalidateRootUsage(rootAbs)
			}
		}

		if err := os.WriteFile(dstAbs, data, 0o644); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, err.Error()
		}

		if haveUsed && s.usage != nil {
			used = applyDeltaBytes(used, delta)
			s.setRootUsageBytes(rootAbs, used)
		}

		copied++
	}

	if copied == 0 {
		return proto.StatusNotFound, "no matching files"
	}

	return proto.StatusOK, fmt.Sprintf("copied %d item(s)", copied)
}

// cpFromD81 extracts one or more files from a mounted D81 image to the filesystem.
func (s *Server) cpFromD81(cfg config.Config, limits Limits, rootAbs, mountPath, inner, dstNorm string, overwrite, recursive bool) (byte, string) {
	// Wildcards are only supported in the last segment.
	_, leaf := splitDirBase("/" + inner)

	if strings.ContainsAny(leaf, "*?") {
		return s.cpBulkFromD81(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite)
	}

	// If the exact path refers to a directory/partition inside the D81, allow
	// recursive extraction when requested.
	imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}
	innerTrim := strings.Trim(inner, "/")
	if _, _, _, _, stDir, _ := resolveD81Dir(img, innerTrim); stDir == proto.StatusOK {
		if !recursive {
			return proto.StatusIsADir, "source is a directory"
		}
		return s.cpDirFromD81(cfg, limits, rootAbs, imgAbs, img, innerTrim, dstNorm, overwrite)
	}

	// Not a directory => single file.
	return s.cpSingleFromD81(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite)
}

// cpDirFromD81 recursively extracts a 1581 "partition" directory from a mounted D81
// into the filesystem.
func (s *Server) cpDirFromD81(cfg config.Config, limits Limits, rootAbs, imgAbs string, img *diskimage.D81, srcDirInner, dstNorm string, overwrite bool) (byte, string) {
	// Determine destination directory (follow the same semantics as filesystem CP):
	// if dstNorm is an existing directory, create a subdirectory with the source leaf name.
	dstAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}

	dstSt, err := fsops.Stat(dstAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if dstSt.Exists && dstSt.IsDir {
		_, leaf := splitDirBase("/" + strings.Trim(srcDirInner, "/"))
		if strings.TrimSpace(leaf) == "" {
			leaf = "DIR"
		}
		dstAbs = filepath.Join(dstAbs, leaf)
		dstSt, err = fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	if err := fsops.LstatNoSymlink(rootAbs, dstAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.EnsureParents(dstAbs); err != nil {
		return proto.StatusInternal, err.Error()
	}

	// Pre-compute source totals for quota enforcement.
	srcTotal, srcMax, st, msg := s.d81DirTotals(img, srcDirInner)
	if st != proto.StatusOK {
		return st, msg
	}
	if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
		return proto.StatusTooLarge, "file too large"
	}

	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	var dstOldTotal uint64
	if dstSt.Exists && !trashOverwrite {
		dstOldTotal, _, err = pathSizeBytes(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	delta := int64(srcTotal) - int64(dstOldTotal)
	if trashOverwrite && dstSt.Exists {
		delta = int64(srcTotal)
	}

	if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
		return proto.StatusTooLarge, "quota exceeded"
	}

	if dstSt.Exists {
		if !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}
		if trashOverwrite {
			if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
		} else {
			if err := os.RemoveAll(dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
			s.invalidateRootUsage(rootAbs)
		}
	}

	if err := os.MkdirAll(dstAbs, 0o755); err != nil {
		return proto.StatusInternal, err.Error()
	}

	if st, msg := s.extractD81DirRecursive(cfg, limits, imgAbs, img, srcDirInner, dstAbs); st != proto.StatusOK {
		s.invalidateRootUsage(rootAbs)
		return st, msg
	}

	if haveUsed && s.usage != nil {
		used = applyDeltaBytes(used, delta)
		s.setRootUsageBytes(rootAbs, used)
	}

	return proto.StatusOK, ""
}

// d81DirTotals returns the total byte size (sum of file sizes) and the maximum
// single file size in the directory subtree rooted at dirPath.
func (s *Server) d81DirTotals(img *diskimage.D81, dirPath string) (total uint64, max uint64, status byte, msg string) {
	entries, _, _, _, st, emsg := resolveD81Dir(img, dirPath)
	if st != proto.StatusOK {
		return 0, 0, st, emsg
	}
	for _, fe := range entries {
		if fe == nil {
			continue
		}
		if fe.Type == 0 || fe.Type == 1 {
			continue
		}
		name := strings.TrimSpace(fe.Name)
		if name == "" {
			continue
		}
		if strings.ContainsAny(name, "/\\") {
			return 0, 0, proto.StatusBadRequest, "invalid name in image"
		}

		if fe.Type == 5 || fe.Type == 6 {
			sub := name
			if dirPath != "" {
				sub = dirPath + "/" + name
			}
			t, m, st2, msg2 := s.d81DirTotals(img, sub)
			if st2 != proto.StatusOK {
				return 0, 0, st2, msg2
			}
			total += t
			if m > max {
				max = m
			}
			continue
		}

		sz := uint64(fe.Size)
		total += sz
		if sz > max {
			max = sz
		}
	}
	return total, max, proto.StatusOK, ""
}

func (s *Server) extractD81DirRecursive(cfg config.Config, limits Limits, imgAbs string, img *diskimage.D81, srcDirInner string, dstDirAbs string) (byte, string) {
	entries, _, _, _, st, emsg := resolveD81Dir(img, srcDirInner)
	if st != proto.StatusOK {
		return st, emsg
	}
	entries = img.SortedDirEntries(entries)

	for _, fe := range entries {
		if fe == nil {
			continue
		}
		if fe.Type == 0 || fe.Type == 1 {
			continue
		}
		baseName := strings.TrimSpace(fe.Name)
		if baseName == "" {
			continue
		}
		if strings.ContainsAny(baseName, "/\\") {
			return proto.StatusBadRequest, "invalid name in image"
		}

		if fe.Type == 5 || fe.Type == 6 {
			// Subdirectory/partition.
			subSrc := baseName
			if srcDirInner != "" {
				subSrc = srcDirInner + "/" + baseName
			}
			subDst := filepath.Join(dstDirAbs, baseName)
			if err := os.MkdirAll(subDst, 0o755); err != nil {
				return proto.StatusInternal, err.Error()
			}
			if st, msg := s.extractD81DirRecursive(cfg, limits, imgAbs, img, subSrc, subDst); st != proto.StatusOK {
				return st, msg
			}
			continue
		}

		// File.
		outName := baseName
		if cfg.Compat.FallbackPRGExtension && fe.Type == 2 {
			outName = outName + ".PRG"
		}
		if limits.MaxFileBytes > 0 && fe.Size > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		data, err := readD81FileRange(imgAbs, fe, 0, fe.Size)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		outAbs := filepath.Join(dstDirAbs, outName)
		if err := os.WriteFile(outAbs, data, 0o644); err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	return proto.StatusOK, ""
}

func (s *Server) cpSingleFromD81(cfg config.Config, limits Limits, rootAbs, mountPath, inner, dstNorm string, overwrite bool) (byte, string) {
	imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}
	_, fe, st, msg := resolveD81Inner(img, inner, cfg.Compat.FallbackPRGExtension)
	if st != proto.StatusOK {
		return st, msg
	}

	// Disallow extracting a subdirectory as a file.
	if fe.Type == 5 || fe.Type == 6 {
		return proto.StatusIsADir, "source is a directory"
	}

	srcTotal := uint64(fe.Size)
	srcMax := srcTotal
	if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
		return proto.StatusTooLarge, "file too large"
	}

	dstAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}

	dstSt, err := fsops.Stat(dstAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if dstSt.Exists && dstSt.IsDir {
		// Use request leaf as filename.
		_, leaf := splitDirBase("/" + inner)
		dstAbs = filepath.Join(dstAbs, leaf)
		dstSt, err = fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	if err := fsops.LstatNoSymlink(rootAbs, dstAbs, true); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.EnsureParents(dstAbs); err != nil {
		return proto.StatusInternal, err.Error()
	}

	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	var dstOldTotal uint64
	if dstSt.Exists && !trashOverwrite {
		dstOldTotal, _, err = pathSizeBytes(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	delta := int64(srcTotal) - int64(dstOldTotal)
	if trashOverwrite && dstSt.Exists {
		delta = int64(srcTotal)
	}

	if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
		return proto.StatusTooLarge, "quota exceeded"
	}

	data, err := readD81FileRange(imgAbs, fe, 0, srcTotal)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}

	if dstSt.Exists {
		if !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}
		if trashOverwrite {
			if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
		} else {
			if err := os.RemoveAll(dstAbs); err != nil {
				return proto.StatusInternal, err.Error()
			}
			s.invalidateRootUsage(rootAbs)
		}
	}

	if err := os.WriteFile(dstAbs, data, 0o644); err != nil {
		s.invalidateRootUsage(rootAbs)
		return proto.StatusInternal, err.Error()
	}

	if haveUsed && s.usage != nil {
		used = applyDeltaBytes(used, delta)
		s.setRootUsageBytes(rootAbs, used)
	}

	return proto.StatusOK, ""
}

func (s *Server) cpBulkFromD81(cfg config.Config, limits Limits, rootAbs, mountPath, innerWithPat, dstNorm string, overwrite bool) (byte, string) {
	dstDirAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, dstDirAbs, false); err != nil {
		return proto.StatusInvalidPath, err.Error()
	}
	dstDirSt, err := fsops.Stat(dstDirAbs)
	if err != nil {
		return proto.StatusInternal, err.Error()
	}
	if !dstDirSt.Exists {
		return proto.StatusNotFound, "destination directory not found"
	}
	if !dstDirSt.IsDir {
		return proto.StatusNotADir, "destination is not a directory"
	}

	imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
	if st != proto.StatusOK {
		return st, msg
	}

	// Split inner path into directory and leaf pattern.
	dirPart, leafPat := splitDirBase("/" + innerWithPat)
	dirPath := strings.TrimPrefix(dirPart, "/")

	entries, _, _, _, st, msg := resolveD81Dir(img, dirPath)
	if st != proto.StatusOK {
		return st, msg
	}

	// Deterministic order (same as LS).
	entries = img.SortedDirEntries(entries)

	var used uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		used, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
	}

	trashOverwrite := cfg.TrashEnabled

	copied := 0

	for _, fe := range entries {
		if fe.Type == 0 || fe.Type == 1 {
			continue
		}

		name := strings.TrimSpace(fe.Name)
		// Directories in D81:
		isDir := fe.Type == 5 || fe.Type == 6

		if cfg.Compat.FallbackPRGExtension && fe.Type == 2 {
			name += ".PRG"
		}

		if !wildcardMatch(leafPat, strings.ToUpper(name)) {
			continue
		}

		if isDir {
			// Not implemented yet: recursive directory extraction from images.
			continue
		}

		srcTotal := uint64(fe.Size)
		srcMax := srcTotal
		if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
			return proto.StatusTooLarge, "file too large"
		}

		dstAbs := filepath.Join(dstDirAbs, name)

		dstSt, err := fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}
		if dstSt.Exists && !overwrite {
			return proto.StatusAccessDenied, "destination exists"
		}

		var dstOldTotal uint64
		if dstSt.Exists && !trashOverwrite {
			dstOldTotal, _, err = pathSizeBytes(dstAbs)
			if err != nil {
				return proto.StatusInternal, err.Error()
			}
		}

		delta := int64(srcTotal) - int64(dstOldTotal)
		if trashOverwrite && dstSt.Exists {
			delta = int64(srcTotal)
		}

		if delta > 0 && haveUsed && used+uint64(delta) > limits.QuotaBytes {
			return proto.StatusTooLarge, "quota exceeded"
		}

		data, err := readD81FileRange(imgAbs, fe, 0, srcTotal)
		if err != nil {
			return proto.StatusInternal, err.Error()
		}

		if dstSt.Exists {
			if trashOverwrite {
				if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
			} else {
				if err := os.RemoveAll(dstAbs); err != nil {
					return proto.StatusInternal, err.Error()
				}
				s.invalidateRootUsage(rootAbs)
			}
		}

		if err := os.WriteFile(dstAbs, data, 0o644); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, err.Error()
		}

		if haveUsed && s.usage != nil {
			used = applyDeltaBytes(used, delta)
			s.setRootUsageBytes(rootAbs, used)
		}

		copied++
	}

	if copied == 0 {
		return proto.StatusNotFound, "no matching files"
	}

	return proto.StatusOK, fmt.Sprintf("copied %d item(s)", copied)
}
