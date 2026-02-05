package pathutil

import (
	"fmt"
	"path"
	"strings"
)

// Normalize validates and normalizes a WiCOS64 path according to v0.2.x:
// - separator '/'
// - collapses '//' -> '/'
// - removes '/./'
// - '/dir' and '/dir/' are identical (trailing slash removed except root)
// - forbids '..' segments
// - enforces ASCII 0x20..0x7E and rejects DEL (0x7F) and NUL
//
// It returns the normalized path ALWAYS starting with '/'.
func Normalize(raw string, maxPath, maxName uint16) (string, error) {
	return normalizeImpl(raw, maxPath, maxName, false)
}

// NormalizeAllowWildcards is identical to Normalize, except that it allows the
// wildcard characters '*' and '?' in path segments.
//
// This exists solely for compatibility features like wildcard LOAD/READ, where
// the path segment represents a *pattern* rather than an actual filename.
func NormalizeAllowWildcards(raw string, maxPath, maxName uint16) (string, error) {
	return normalizeImpl(raw, maxPath, maxName, true)
}

func normalizeImpl(raw string, maxPath, maxName uint16, allowWildcards bool) (string, error) {
	// Empty means root.
	if raw == "" {
		return "/", nil
	}

	// Must use forward slashes only.
	if strings.Contains(raw, "\\") {
		return "", fmt.Errorf("backslash not allowed")
	}

	// Validate ASCII range and reject chars that are problematic on Windows.
	// Spec says ASCII 0x20..0x7E. We additionally reject characters not allowed on Windows filenames.
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c == 0 {
			return "", fmt.Errorf("NUL not allowed")
		}
		if c < 0x20 || c == 0x7F {
			return "", fmt.Errorf("control/DEL not allowed")
		}
		switch c {
		case ':', '"', '<', '>', '|':
			return "", fmt.Errorf("invalid character 0x%02x", c)
		case '*', '?':
			if !allowWildcards {
				return "", fmt.Errorf("invalid character 0x%02x", c)
			}
		}
	}

	if uint16(len(raw)) > maxPath {
		return "", fmt.Errorf("path length %d exceeds %d", len(raw), maxPath)
	}

	// Ensure leading slash.
	p := raw
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	// path.Clean will:
	// - resolve '//' and '/./'
	// - but it also resolves '..' which we must FORBID, so we check segments first.
	segs := strings.Split(p, "/")
	for _, s := range segs {
		if s == ".." {
			return "", fmt.Errorf(".. segment not allowed")
		}
		if s == "" {
			continue // root / multiple slashes – will be normalized
		}
		if uint16(len(s)) > maxName {
			return "", fmt.Errorf("segment too long (%d>%d)", len(s), maxName)
		}
		// Windows forbids names ending in dot/space.
		if strings.HasSuffix(s, " ") || strings.HasSuffix(s, ".") {
			return "", fmt.Errorf("segment may not end with space or dot")
		}
		// Windows reserved device names (case-insensitive). We keep the list small.
		up := strings.ToUpper(s)
		switch up {
		case "CON", "PRN", "AUX", "NUL",
			"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
			"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
			return "", fmt.Errorf("reserved name not allowed")
		}
	}

	p = path.Clean(p)
	if p == "." {
		p = "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}

	// Remove trailing slash except for root.
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		p = strings.TrimRight(p, "/")
		if p == "" {
			p = "/"
		}
	}

	// Re-check length after normalization.
	if uint16(len(p)) > maxPath {
		return "", fmt.Errorf("normalized path length %d exceeds %d", len(p), maxPath)
	}
	return p, nil
}

// Canonicalize returns a canonical representation for case-insensitive matching.
// For v0.2.x we use ASCII uppercase folding.
func Canonicalize(p string) string {
	return strings.ToUpper(p)
}
