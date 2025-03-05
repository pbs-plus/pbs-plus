//go:build windows

package vssfs

import (
	"os"

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
