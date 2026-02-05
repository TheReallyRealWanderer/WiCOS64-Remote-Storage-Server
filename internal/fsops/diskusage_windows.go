//go:build windows

package fsops

import (
	"syscall"
	"unsafe"
)

// DiskUsage returns total and free bytes for the filesystem containing the given path.
// used bytes can be derived as total-free.
func DiskUsage(path string) (total uint64, free uint64, err error) {
	// WinAPI: BOOL GetDiskFreeSpaceExW(...)
	k32 := syscall.NewLazyDLL("kernel32.dll")
	proc := k32.NewProc("GetDiskFreeSpaceExW")

	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}

	var freeBytesAvailable uint64
	var totalNumberOfBytes uint64
	var totalNumberOfFreeBytes uint64

	r1, _, e1 := proc.Call(
		uintptr(unsafe.Pointer(p)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)
	if r1 == 0 {
		if e1 != nil {
			return 0, 0, e1
		}
		return 0, 0, syscall.EINVAL
	}
	return totalNumberOfBytes, totalNumberOfFreeBytes, nil
}
