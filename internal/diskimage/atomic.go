package diskimage

import (
	"os"
	"path/filepath"
)

// writeFileAtomic writes data to path atomically (best effort).
// It creates a temp file in the same directory and renames it over the target.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".wicos64-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Ignore chmod errors on platforms that don't support it well.
	_ = os.Chmod(tmpName, perm)

	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	ok = true
	return nil
}
