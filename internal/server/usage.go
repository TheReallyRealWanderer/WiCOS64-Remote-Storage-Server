package server

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type usageEntry struct {
	bytes uint64
	at    time.Time
}

// usageCache caches per-root directory usage (sum of regular file sizes) to make quota checks cheap.
//
// NOTE: This is best-effort: if the cache becomes inconsistent (e.g. due to external changes),
// it expires quickly and will be recomputed.
type usageCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]usageEntry
}

func newUsageCache(ttl time.Duration) *usageCache {
	if ttl <= 0 {
		ttl = 3 * time.Second
	}
	return &usageCache{ttl: ttl, m: make(map[string]usageEntry)}
}

func (c *usageCache) getFresh(rootAbs string) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[rootAbs]
	if !ok {
		return 0, false
	}
	if c.ttl > 0 && time.Since(e.at) > c.ttl {
		delete(c.m, rootAbs)
		return 0, false
	}
	return e.bytes, true
}

func (c *usageCache) set(rootAbs string, bytes uint64) {
	c.mu.Lock()
	c.m[rootAbs] = usageEntry{bytes: bytes, at: time.Now()}
	c.mu.Unlock()
}

func (c *usageCache) invalidate(rootAbs string) {
	c.mu.Lock()
	delete(c.m, rootAbs)
	c.mu.Unlock()
}

// getOrScanRootUsage returns the cached usage for rootAbs, or scans the tree if missing/stale.
func (s *Server) getOrScanRootUsage(rootAbs string) (uint64, error) {
	if s.usage != nil {
		if b, ok := s.usage.getFresh(rootAbs); ok {
			return b, nil
		}
	}
	used, _, err := pathSizeBytes(rootAbs)
	if err != nil {
		return 0, err
	}
	if s.usage != nil {
		s.usage.set(rootAbs, used)
	}
	return used, nil
}

func (s *Server) setRootUsage(rootAbs string, used uint64) {
	if s.usage != nil {
		s.usage.set(rootAbs, used)
	}
}

func (s *Server) invalidateRootUsage(rootAbs string) {
	if s.usage != nil {
		s.usage.invalidate(rootAbs)
	}
}

// rootUsageBytes returns the current used bytes under rootAbs.
// It is a thin wrapper kept for backwards compatibility with earlier refactors.
func (s *Server) rootUsageBytes(rootAbs string) (uint64, error) {
	return s.getOrScanRootUsage(rootAbs)
}

// setRootUsageBytes updates the cached used bytes for rootAbs.
// It is a thin wrapper kept for backwards compatibility with earlier refactors.
func (s *Server) setRootUsageBytes(rootAbs string, bytes uint64) {
	s.setRootUsage(rootAbs, bytes)
}

// applyDeltaBytes applies a signed delta to an unsigned used-byte counter.
func applyDeltaBytes(used uint64, delta int64) uint64 {
	if delta >= 0 {
		return used + uint64(delta)
	}
	d := uint64(-delta)
	if d >= used {
		return 0
	}
	return used - d
}

// pathSizeBytes returns (totalBytes, maxFileBytes) for absPath.
// It sums regular file sizes; directories themselves do not contribute.
// Symlinks are rejected.
func pathSizeBytes(absPath string) (uint64, uint64, error) {
	fi, err := os.Lstat(absPath)
	if err != nil {
		return 0, 0, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return 0, 0, fmt.Errorf("symlink not allowed")
	}
	if fi.IsDir() {
		return dirTreeSize(absPath)
	}
	if fi.Mode().IsRegular() {
		sz := uint64(fi.Size())
		return sz, sz, nil
	}
	return 0, 0, fmt.Errorf("unsupported file type")
}

func dirTreeSize(dir string) (uint64, uint64, error) {
	var total uint64
	var maxFile uint64

	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Skip the root dir itself.
		if p == dir {
			return nil
		}
		// Reject symlinks.
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink not allowed")
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported file type")
		}
		sz := uint64(info.Size())
		total += sz
		if sz > maxFile {
			maxFile = sz
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return total, maxFile, nil
}
