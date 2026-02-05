package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"wicos64-server/internal/config"
)

// shouldUseTrash returns true if trash is enabled and the target path is eligible.
//
// We purposely do NOT trash anything inside the trash dir itself or inside .TMP.
// That allows users/admins to permanently delete items by deleting from trash,
// and prevents infinite nesting.
func shouldUseTrash(cfg config.Config, rootAbs, targetAbs string) bool {
	if !cfg.TrashEnabled {
		return false
	}
	trashDir := strings.TrimSpace(cfg.TrashDir)
	if trashDir == "" {
		trashDir = ".TRASH"
	}
	if isTopLevelDir(rootAbs, targetAbs, trashDir) {
		return false
	}
	if isTopLevelDir(rootAbs, targetAbs, ".TMP") {
		return false
	}
	return true
}

func isTopLevelDir(rootAbs, targetAbs, dir string) bool {
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil {
		return false
	}
	rel = filepath.ToSlash(rel)
	if rel == "." || rel == "" {
		return false
	}
	if strings.HasPrefix(rel, "../") || rel == ".." {
		return false
	}
	first := rel
	if i := strings.IndexByte(rel, '/'); i >= 0 {
		first = rel[:i]
	}
	return strings.EqualFold(first, dir)
}

func makeTrashID() string {
	now := time.Now().UTC()
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("%s-%s", now.Format("20060102T150405Z"), hex.EncodeToString(buf))
}

// moveToTrash renames targetAbs into <rootAbs>/<trashDir>/<id>/<relative-path>.
func (s *Server) moveToTrash(cfg config.Config, rootAbs, targetAbs string) (string, error) {
	trashDir := strings.TrimSpace(cfg.TrashDir)
	if trashDir == "" {
		trashDir = ".TRASH"
	}
	base := filepath.Join(rootAbs, trashDir)

	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == "" || strings.HasPrefix(rel, "..") {
		// Safety: never move outside root.
		rel = filepath.Base(targetAbs)
	}

	for i := 0; i < 5; i++ {
		id := makeTrashID()
		dstAbs := filepath.Join(base, id, rel)
		if err := os.MkdirAll(filepath.Dir(dstAbs), 0o755); err != nil {
			return "", err
		}
		if err := os.Rename(targetAbs, dstAbs); err != nil {
			if os.IsExist(err) {
				continue
			}
			return "", err
		}
		return dstAbs, nil
	}
	return "", fmt.Errorf("failed to move to trash: too many name collisions")
}
