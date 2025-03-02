//go:build windows

package vssfs

import (
	"fmt"
	"os"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func mapWinError(err error) error {
	switch err {
	case windows.ERROR_FILE_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_PATH_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_ACCESS_DENIED:
		return os.ErrPermission
	default:
		return &os.PathError{
			Op:   "access",
			Path: "",
			Err:  err,
		}
	}
}

func fileIDToKey(info *winio.FileIDInfo) string {
	return fmt.Sprintf("%d_%x", info.VolumeSerialNumber, info.FileID)
}
