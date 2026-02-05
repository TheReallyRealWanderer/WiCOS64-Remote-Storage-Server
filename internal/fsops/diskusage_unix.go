//go:build !windows

package fsops

import "syscall"

// DiskUsage returns total and free bytes for the filesystem containing the given path.
// used bytes can be derived as total-free.
func DiskUsage(path string) (total uint64, free uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(st.Bsize)
	total = uint64(st.Blocks) * bs
	free = uint64(st.Bavail) * bs
	return total, free, nil
}
