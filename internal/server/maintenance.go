package server

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"wicos64-server/internal/config"
)

// CleanupReport describes what a tmp-cleanup run did.
type CleanupReport struct {
	RootAbs      string `json:"root_abs"`
	TmpDirAbs    string `json:"tmp_dir_abs"`
	DeletedFiles int    `json:"deleted_files"`
	DeletedDirs  int    `json:"deleted_dirs"`
	FreedBytes   uint64 `json:"freed_bytes"`
	DurationMs   int64  `json:"duration_ms"`
	Error        string `json:"error,omitempty"`
}

// TrashCleanupReport describes what a trash-cleanup run did.
//
// Trash cleanup removes old entries under the configured TrashDir (default: .TRASH).
// Each trashed item lives under a top-level ID directory (see makeTrashID()).
type TrashCleanupReport struct {
	RootAbs      string `json:"root_abs"`
	TrashDirAbs  string `json:"trash_dir_abs"`
	DeletedFiles int    `json:"deleted_files"`
	DeletedDirs  int    `json:"deleted_dirs"`
	FreedBytes   uint64 `json:"freed_bytes"`
	DurationMs   int64  `json:"duration_ms"`
	Error        string `json:"error,omitempty"`
}

// runTrashCleanupOnce runs trash cleanup for all configured roots.
func (s *Server) runTrashCleanupOnce(cfg config.Config) []TrashCleanupReport {
	// Per config warning: if trash is disabled, cleanup is intentionally a no-op.
	if !cfg.TrashEnabled || !cfg.TrashCleanupEnabled {
		return nil
	}
	roots := resolveRootsForMaintenance(cfg)
	if len(roots) == 0 {
		return nil
	}
	out := make([]TrashCleanupReport, 0, len(roots))
	for _, rootAbs := range roots {
		rep := cleanupTrashForRoot(cfg, rootAbs)
		out = append(out, rep)
		if rep.DeletedFiles > 0 || rep.DeletedDirs > 0 {
			// Usage has changed; safest is to invalidate.
			s.invalidateRootUsage(rootAbs)
		}
	}
	return out
}

func parseTrashIDTime(name string) (time.Time, bool) {
	// Expected format: 20060102T150405Z-<hex>
	parts := strings.SplitN(name, "-", 2)
	if len(parts) == 0 || parts[0] == "" {
		return time.Time{}, false
	}
	t, err := time.Parse("20060102T150405Z", parts[0])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func countTreeSize(root string) (files, dirs int, bytes uint64, err error) {
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		// Never follow / allow symlinks.
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink not allowed")
		}
		if d.IsDir() {
			dirs++
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		// Only regular files.
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported file type")
		}
		files++
		bytes += uint64(info.Size())
		return nil
	})
	return
}

func cleanupTrashForRoot(cfg config.Config, rootAbs string) TrashCleanupReport {
	start := time.Now()
	trashDir := strings.TrimSpace(cfg.TrashDir)
	if trashDir == "" {
		trashDir = ".TRASH"
	}
	trashAbs := filepath.Join(rootAbs, trashDir)
	rep := TrashCleanupReport{RootAbs: rootAbs, TrashDirAbs: trashAbs}

	// If root doesn't exist yet, skip silently.
	if _, err := os.Stat(rootAbs); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	// Trash dir missing is not an error.
	fi, err := os.Stat(trashAbs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}
	if !fi.IsDir() {
		rep.Error = "trash path exists but is not a directory"
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	maxAge := time.Duration(cfg.TrashCleanupMaxAgeSec) * time.Second
	if maxAge <= 0 {
		maxAge = 7 * 24 * time.Hour
	}
	cutoff := time.Now().Add(-maxAge)

	entries, err := os.ReadDir(trashAbs)
	if err != nil {
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	for _, e := range entries {
		name := e.Name()
		child := filepath.Join(trashAbs, name)
		info, err := e.Info()
		if err != nil {
			rep.Error = err.Error()
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		if info.Mode()&os.ModeSymlink != 0 {
			rep.Error = "symlink not allowed"
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}

		// Determine the age of this trash entry.
		t, ok := parseTrashIDTime(name)
		if !ok {
			// Fallback: use the entry's modtime.
			t = info.ModTime()
		}
		if t.After(cutoff) {
			continue
		}

		files, dirs, bytes, err := countTreeSize(child)
		if err != nil {
			rep.Error = err.Error()
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		if err := os.RemoveAll(child); err != nil {
			rep.Error = err.Error()
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		rep.DeletedFiles += files
		rep.DeletedDirs += dirs
		rep.FreedBytes += bytes
	}

	if cfg.TrashCleanupDeleteEmptyDirs {
		// Prune empty dirs in reverse depth order.
		var dirs []string
		err = filepath.WalkDir(trashAbs, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if p == trashAbs {
				return nil
			}
			if d.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink not allowed")
			}
			if d.IsDir() {
				dirs = append(dirs, p)
			}
			return nil
		})
		if err != nil {
			rep.Error = err.Error()
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		for i := len(dirs) - 1; i >= 0; i-- {
			p := dirs[i]
			_ = os.Remove(p)
			if _, err := os.Stat(p); errors.Is(err, fs.ErrNotExist) {
				rep.DeletedDirs++
			}
		}
	}

	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

// SelfTestReport is a quick health check for filesystem basics.
type SelfTestReport struct {
	RootAbs    string `json:"root_abs"`
	TmpDirAbs  string `json:"tmp_dir_abs"`
	ReadOnly   bool   `json:"read_only"`
	OK         bool   `json:"ok"`
	Details    string `json:"details"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

func resolveRootsForMaintenance(cfg config.Config) []string {
	// We dedupe because multiple tokens might point to the same root.
	seen := make(map[string]struct{})
	add := func(p string) {
		if p == "" {
			return
		}
		abs := p
		if !filepath.IsAbs(abs) {
			abs = filepath.Clean(abs)
		}
		if _, ok := seen[abs]; ok {
			return
		}
		seen[abs] = struct{}{}
	}

	if len(cfg.Tokens) > 0 {
		for _, t := range cfg.Tokens {
			root := t.Root
			if root == "" {
				root = cfg.BasePath
			} else if !filepath.IsAbs(root) {
				root = filepath.Join(cfg.BasePath, root)
			}
			add(root)
		}
		out := make([]string, 0, len(seen))
		for p := range seen {
			out = append(out, p)
		}
		return out
	}

	if len(cfg.TokenRoots) > 0 {
		for _, r := range cfg.TokenRoots {
			root := r
			if root == "" {
				continue
			}
			if !filepath.IsAbs(root) {
				root = filepath.Join(cfg.BasePath, root)
			}
			add(root)
		}
		// If Token is also set, BasePath might still be used by clients in single-token setups.
		if cfg.Token != "" {
			add(cfg.BasePath)
		}
		out := make([]string, 0, len(seen))
		for p := range seen {
			out = append(out, p)
		}
		return out
	}

	add(cfg.BasePath)
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	return out
}

// runTmpCleanupOnce runs cleanup for all configured roots.
func (s *Server) runTmpCleanupOnce(cfg config.Config) []CleanupReport {
	roots := resolveRootsForMaintenance(cfg)
	if len(roots) == 0 {
		return nil
	}
	out := make([]CleanupReport, 0, len(roots))
	for _, rootAbs := range roots {
		rep := cleanupTmpForRoot(cfg, rootAbs)
		out = append(out, rep)
		if rep.DeletedFiles > 0 || rep.DeletedDirs > 0 {
			// Usage has changed; safest is to invalidate.
			s.invalidateRootUsage(rootAbs)
		}
	}
	return out
}

func cleanupTmpForRoot(cfg config.Config, rootAbs string) CleanupReport {
	start := time.Now()
	tmpDir := filepath.Join(rootAbs, ".TMP")
	rep := CleanupReport{RootAbs: rootAbs, TmpDirAbs: tmpDir}

	// If root doesn't exist yet, skip silently.
	if _, err := os.Stat(rootAbs); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	// tmp dir missing is not an error.
	fi, err := os.Stat(tmpDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			rep.DurationMs = time.Since(start).Milliseconds()
			return rep
		}
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}
	if !fi.IsDir() {
		rep.Error = "tmp path exists but is not a directory"
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	maxAge := time.Duration(cfg.TmpCleanupMaxAgeSec) * time.Second
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	cutoff := time.Now().Add(-maxAge)

	// We delete files on the fly, then optionally attempt to prune empty dirs.
	var dirs []string
	err = filepath.WalkDir(tmpDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == tmpDir {
			return nil
		}
		// Never follow / allow symlinks.
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink not allowed")
		}
		if d.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		// Only regular files.
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported file type")
		}
		if info.ModTime().After(cutoff) {
			return nil
		}
		sz := uint64(info.Size())
		if err := os.Remove(p); err != nil {
			return err
		}
		rep.DeletedFiles++
		rep.FreedBytes += sz
		return nil
	})
	if err != nil {
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	if cfg.TmpCleanupDeleteEmptyDirs {
		// Remove empty dirs in reverse depth order.
		for i := len(dirs) - 1; i >= 0; i-- {
			p := dirs[i]
			_ = os.Remove(p)
			// We don't count failures (directory might not be empty).
			if _, err := os.Stat(p); errors.Is(err, fs.ErrNotExist) {
				rep.DeletedDirs++
			}
		}
	}

	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

// runSelfTestOnce runs a filesystem self-test for all configured roots.
func (s *Server) runSelfTestOnce(cfg config.Config) []SelfTestReport {
	roots := resolveRootsForMaintenance(cfg)
	if len(roots) == 0 {
		return nil
	}
	out := make([]SelfTestReport, 0, len(roots))
	for _, rootAbs := range roots {
		out = append(out, selfTestForRoot(cfg, rootAbs))
	}
	return out
}

func selfTestForRoot(cfg config.Config, rootAbs string) SelfTestReport {
	start := time.Now()
	tmpDir := filepath.Join(rootAbs, ".TMP")
	rep := SelfTestReport{RootAbs: rootAbs, TmpDirAbs: tmpDir, ReadOnly: cfg.GlobalReadOnly}

	if err := config.EnsureRoot(rootAbs); err != nil {
		rep.OK = false
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}
	_ = os.MkdirAll(tmpDir, 0o755)

	// In read-only mode we only validate that directories exist.
	if cfg.GlobalReadOnly {
		rep.OK = true
		rep.Details = "read-only: root and .TMP ok"
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	// Write -> read -> delete a small file.
	name := fmt.Sprintf("SELFTEST-%d.BIN", time.Now().UnixNano())
	p := filepath.Join(tmpDir, name)
	payload := []byte("WiCOS64 selftest\n")

	if err := os.WriteFile(p, payload, 0o644); err != nil {
		rep.OK = false
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}
	b, err := os.ReadFile(p)
	if err != nil {
		_ = os.Remove(p)
		rep.OK = false
		rep.Error = err.Error()
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}
	_ = os.Remove(p)

	if string(b) != string(payload) {
		rep.OK = false
		rep.Details = "roundtrip mismatch"
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}

	rep.OK = true
	rep.Details = "write/read/delete ok"
	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

func (s *Server) startMaintenanceLoop() {
	// TMP cleanup loop
	go func() {
		// Small initial delay so the server is fully up.
		time.Sleep(2 * time.Second)
		for {
			cfg := s.getCfg()
			if !cfg.TmpCleanupEnabled {
				// Sleep a bit, then re-check config.
				time.Sleep(10 * time.Second)
				continue
			}
			interval := time.Duration(cfg.TmpCleanupIntervalSec) * time.Second
			if interval <= 0 {
				interval = 15 * time.Minute
			}
			if interval < 10*time.Second {
				interval = 10 * time.Second
			}

			_ = s.runTmpCleanupOnce(cfg)
			time.Sleep(interval)
		}
	}()

	// Trash cleanup loop
	go func() {
		// Small initial delay so the server is fully up.
		time.Sleep(2 * time.Second)
		for {
			cfg := s.getCfg()
			if !cfg.TrashEnabled || !cfg.TrashCleanupEnabled {
				// Sleep a bit, then re-check config.
				time.Sleep(10 * time.Second)
				continue
			}
			interval := time.Duration(cfg.TrashCleanupIntervalSec) * time.Second
			if interval <= 0 {
				interval = 6 * time.Hour
			}
			if interval < 60*time.Second {
				interval = 60 * time.Second
			}

			_ = s.runTrashCleanupOnce(cfg)
			time.Sleep(interval)
		}
	}()
}
