//go:build windows

package snapshots

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

var (
	modkernel32       = syscall.NewLazyDLL("kernel32.dll")
	procGetVolumeInfo = modkernel32.NewProc("GetVolumeInformationW")
)

func getFsType(path string) (string, string, error) {
	vol := filepath.VolumeName(path)
	if vol == "" {
		return "", "", fmt.Errorf("could not determine volume for path: %s", path)
	}

	rootPath := vol + "\\"
	rootPtr, err := syscall.UTF16PtrFromString(rootPath)
	if err != nil {
		return "", "", err
	}

	fsNameBuf := make([]uint16, syscall.MAX_PATH+1)

	r1, _, ctxErr := procGetVolumeInfo.Call(
		uintptr(unsafe.Pointer(rootPtr)),
		0,
		0,
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&fsNameBuf[0])),
		uintptr(len(fsNameBuf)),
	)

	if r1 == 0 {
		return "", "", fmt.Errorf("GetVolumeInformationW failed: %w", ctxErr)
	}

	return strings.ToLower(syscall.UTF16ToString(fsNameBuf)), rootPath, nil
}
