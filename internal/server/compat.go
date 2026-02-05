package server

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"wicos64-server/internal/config"
	"wicos64-server/internal/fsops"
)

func hasWildcard(s string) bool {
	return strings.ContainsAny(s, "*?")
}

func splitDirBase(normPath string) (dir, base string) {
	// normPath is normalized and always begins with '/'.
	i := strings.LastIndex(normPath, "/")
	if i <= 0 {
		// "/FOO" or "FOO" (defensive) -> dir="/"
		return "/", strings.TrimPrefix(normPath, "/")
	}
	dir = normPath[:i]
	if dir == "" {
		dir = "/"
	}
	base = normPath[i+1:]
	return dir, base
}

// wildcardMatch implements a tiny glob matcher:
//   - '*' matches any sequence (incl. empty)
//   - '?' matches exactly one character
//
// No other special characters are supported.
func wildcardMatch(pattern, s string) bool {
	// Fast path: exact
	if pattern == s {
		return true
	}

	p := 0
	i := 0
	star := -1
	match := 0
	for i < len(s) {
		if p < len(pattern) && (pattern[p] == '?' || pattern[p] == s[i]) {
			p++
			i++
			continue
		}
		if p < len(pattern) && pattern[p] == '*' {
			star = p
			match = i
			p++
			continue
		}
		if star != -1 {
			p = star + 1
			match++
			i = match
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

func prgFallbackCandidate(normPath string) bool {
	if normPath == "" || normPath == "/" {
		return false
	}
	// Only consider the last path segment.
	name := normPath
	if i := strings.LastIndex(normPath, "/"); i >= 0 {
		name = normPath[i+1:]
	}
	if name == "" {
		return false
	}
	// If the name already contains a dot, assume it already has an extension.
	if strings.Contains(name, ".") {
		return false
	}
	return true
}

// resolveReadPathWithCompat resolves a normalized/canonical WiCOS64 path to an
// OS path and optionally applies compatibility helpers:
//
//   - wildcard_load: If the final path segment contains '*' or '?', it is
//     resolved to the first matching file in that directory.
//   - fallback_prg_extension: If the exact path is missing and the name has no
//     '.', it additionally tries "<name>.PRG".
//
// It also performs the no-symlink check and returns the error from that check.
func resolveReadPathWithCompat(cfg config.Config, rootAbs, normPath string) (abs string, usedCompat bool, err error) {
	// 1) Optional wildcard resolution (final segment only)
	if cfg.Compat.WildcardLoad {
		dirNorm, namePat := splitDirBase(normPath)
		if hasWildcard(namePat) {
			// Safety: do not allow wildcards in directory segments.
			if hasWildcard(dirNorm) {
				return "", false, errors.New("wildcards are only allowed in the final path segment")
			}

			dirAbs, err2 := fsops.ToOSPath(rootAbs, dirNorm)
			if err2 != nil {
				return "", false, err2
			}
			if err3 := fsops.LstatNoSymlink(rootAbs, dirAbs, false); err3 != nil {
				return "", false, err3
			}

			entries, err4 := os.ReadDir(dirAbs)
			if err4 != nil {
				return "", false, err4
			}
			sort.Slice(entries, func(i, j int) bool {
				return strings.ToUpper(entries[i].Name()) < strings.ToUpper(entries[j].Name())
			})

			patU := strings.ToUpper(namePat)
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				if !wildcardMatch(patU, strings.ToUpper(name)) {
					continue
				}

				candAbs := filepath.Join(dirAbs, name)
				if err5 := fsops.LstatNoSymlink(rootAbs, candAbs, false); err5 != nil {
					// Ignore symlinks and keep searching.
					if errors.Is(err5, fsops.ErrSymlinkNotAllowed) {
						continue
					}
					return "", true, err5
				}
				return candAbs, true, nil
			}
			return "", true, fs.ErrNotExist
		}
	}

	// 2) Exact path
	abs, err = fsops.ToOSPath(rootAbs, normPath)
	if err != nil {
		return "", false, err
	}
	err = fsops.LstatNoSymlink(rootAbs, abs, false)
	if err == nil {
		return abs, false, nil
	}

	// 3) Optional <name>.PRG fallback
	if errors.Is(err, fs.ErrNotExist) && cfg.Compat.FallbackPRGExtension && prgFallbackCandidate(normPath) {
		altNorm := normPath + ".PRG"
		altAbs, err2 := fsops.ToOSPath(rootAbs, altNorm)
		if err2 == nil {
			if err3 := fsops.LstatNoSymlink(rootAbs, altAbs, false); err3 == nil {
				return altAbs, true, nil
			}
		}
	}

	return abs, false, err
}
