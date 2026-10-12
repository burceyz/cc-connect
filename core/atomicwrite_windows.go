//go:build windows

package core

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

var moveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func durableReplaceFile(source, destination string) error {
	from, err := windowsReplacementPath(source)
	if err != nil {
		return err
	}
	to, err := windowsReplacementPath(destination)
	if err != nil {
		return err
	}
	if err := moveFileExW.Find(); err != nil {
		return err
	}
	// A read-only directory handle cannot be flushed on Windows. Use the
	// documented write-through move instead, after the file itself is synced.
	// COPY_ALLOWED is deliberately absent: both paths are in one directory.
	// https://learn.microsoft.com/windows/win32/api/winbase/nf-winbase-movefileexw
	const replaceExisting, writeThrough = 0x1, 0x8
	ok, _, callErr := moveFileExW.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), replaceExisting|writeThrough)
	if ok == 0 {
		return &os.LinkError{Op: "rename", Old: source, New: destination, Err: callErr}
	}
	return nil
}

// MoveFileExW needs extended absolute paths when long-path opt-in is absent.
// Preserve device paths, and use the UNC form for network shares.
func windowsReplacementPath(path string) (*uint16, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if len(abs) >= 248 && !strings.HasPrefix(abs, `\\?\`) && !strings.HasPrefix(abs, `\\.\`) {
		if strings.HasPrefix(abs, `\\`) {
			abs = `\\?\UNC\` + abs[2:]
		} else {
			abs = `\\?\` + abs
		}
	}
	return syscall.UTF16PtrFromString(abs)
}
