package fsops

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrSymlinkNotAllowed = errors.New("symlink not allowed")

// ToOSPath converts a normalized WiCOS64 path (starting with '/') into an on-disk path
// inside root. It performs a lexical sandbox check (no '..') and ensures the resulting
// path stays within root.
//
// WiCOS64 paths are case-insensitive (the server canonicalizes to upper-case). On
// case-sensitive filesystems we therefore resolve each existing path segment in a
// case-insensitive way so that pre-existing files with different casing remain
// accessible.
func ToOSPath(rootAbs string, normalized string) (string, error) {
	cleanRoot := filepath.Clean(rootAbs)
	if normalized == "" || normalized == "/" {
		return cleanRoot, nil
	}
	// Defensive: normalize should always start with '/'.
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}

	rel := strings.TrimPrefix(normalized, "/")
	segs := strings.Split(rel, "/")
	cur := cleanRoot

	for i, seg := range segs {
		if seg == "" || seg == "." {
			continue
		}
		// Try to resolve this segment by scanning the existing directory entries.
		entries, err := os.ReadDir(cur)
		if err != nil {
			// Cannot read/list this directory (missing, permissions, ...). Fall back to
			// the lexical join for the remaining path.
			rest := filepath.FromSlash(strings.Join(segs[i:], "/"))
			p := filepath.Join(cur, rest)
			return ensureWithinRoot(cleanRoot, p)
		}

		best := ""
		for _, e := range entries {
			name := e.Name()
			if strings.EqualFold(name, seg) {
				if best == "" || name < best {
					best = name
				}
			}
		}

		if best == "" {
			// Segment doesn't exist (yet). Stop resolving and join the rest verbatim.
			rest := filepath.FromSlash(strings.Join(segs[i:], "/"))
			p := filepath.Join(cur, rest)
			return ensureWithinRoot(cleanRoot, p)
		}

		next := filepath.Join(cur, best)
		fi, err := os.Lstat(next)
		if err != nil {
			// Entry vanished between ReadDir and Lstat. Fall back to lexical join.
			rest := filepath.FromSlash(strings.Join(segs[i:], "/"))
			p := filepath.Join(cur, rest)
			return ensureWithinRoot(cleanRoot, p)
		}

		// Never follow symlinks during resolution.
		if fi.Mode()&os.ModeSymlink != 0 {
			// Do not traverse deeper; return a path that still contains the symlink.
			rest := filepath.FromSlash(strings.Join(segs[i+1:], "/"))
			p := next
			if rest != "" {
				p = filepath.Join(next, rest)
			}
			return ensureWithinRoot(cleanRoot, p)
		}

		// If we need to traverse further, this must be a directory.
		if i < len(segs)-1 && !fi.IsDir() {
			rest := filepath.FromSlash(strings.Join(segs[i+1:], "/"))
			p := next
			if rest != "" {
				p = filepath.Join(next, rest)
			}
			return ensureWithinRoot(cleanRoot, p)
		}

		cur = next
	}

	return ensureWithinRoot(cleanRoot, cur)
}

func ensureWithinRoot(cleanRoot, p string) (string, error) {
	cleanP := filepath.Clean(p)
	relCheck, err := filepath.Rel(cleanRoot, cleanP)
	if err != nil {
		return "", err
	}
	if relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root")
	}
	return cleanP, nil
}

// LstatNoSymlink walks from root to absPath (inclusive where it exists) and rejects any symlink.
// This prevents symlink escapes out of the sandbox.
// For creation paths, allowMissingLast can be set so that the last component may not exist yet.
func LstatNoSymlink(rootAbs, absPath string, allowMissingLast bool) error {
	cleanRoot := filepath.Clean(rootAbs)
	cleanP := filepath.Clean(absPath)
	rel, err := filepath.Rel(cleanRoot, cleanP)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	parts := strings.Split(rel, string(filepath.Separator))
	cur := cleanRoot
	for i, part := range parts {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			if allowMissingLast && i == len(parts)-1 && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return ErrSymlinkNotAllowed
		}
	}
	return nil
}

type StatInfo struct {
	Exists    bool
	IsDir     bool
	Size      uint64
	MTimeUnix uint32
}

func Stat(absPath string) (StatInfo, error) {
	fi, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return StatInfo{Exists: false}, nil
		}
		return StatInfo{}, err
	}
	mtime := uint32(0)
	if !fi.ModTime().IsZero() {
		mtime = uint32(fi.ModTime().Unix())
	}
	size := uint64(0)
	if !fi.IsDir() {
		size = uint64(fi.Size())
	}
	return StatInfo{Exists: true, IsDir: fi.IsDir(), Size: size, MTimeUnix: mtime}, nil
}

// EnsureDir ensures a directory exists.
func EnsureDir(p string) error {
	return os.MkdirAll(p, 0o755)
}

// EnsureParents ensures the parent directory of p exists.
func EnsureParents(p string) error {
	parent := filepath.Dir(p)
	return os.MkdirAll(parent, 0o755)
}

// CopyFile copies a file from src to dst (overwriting dst). It creates parent directories.
func CopyFile(src, dst string) error {
	if err := EnsureParents(dst); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	// Best-effort flush.
	_ = out.Sync()

	// Try to preserve modtime for nicer UX (not required by spec).
	if fi, err := os.Stat(src); err == nil {
		_ = os.Chtimes(dst, time.Now(), fi.ModTime())
	}
	return nil
}

// CopyDirRecursive copies a directory tree from srcDir to dstDir.
// It creates dstDir if missing, and copies files. Symlinks are not followed (they are rejected).
func CopyDirRecursive(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		src := filepath.Join(srcDir, e.Name())
		dst := filepath.Join(dstDir, e.Name())
		info, err := e.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink not allowed")
		}
		if info.IsDir() {
			if err := CopyDirRecursive(src, dst); err != nil {
				return err
			}
			continue
		}
		if err := CopyFile(src, dst); err != nil {
			return err
		}
	}
	return nil
}
