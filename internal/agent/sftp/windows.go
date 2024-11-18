//go:build windows

package sftp

import (
	"os"

	"golang.org/x/sys/windows"
)

func invalidAttributes(path string) (bool, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false, err
	}

	// Get file attributes
	attributes, err := windows.GetFileAttributes(p)
	if err != nil {
		return false, os.NewSyscallError("GetFileAttributes", err)
	}

	if attributes&windows.FILE_ATTRIBUTE_TEMPORARY != 0 {
		return true, nil
	}

	if attributes&windows.FILE_ATTRIBUTE_RECALL_ON_OPEN != 0 {
		return true, nil
	}

	if attributes&windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS != 0 {
		return true, nil
	}

	if attributes&windows.FILE_ATTRIBUTE_VIRTUAL != 0 {
		return true, nil
	}

	if attributes&windows.FILE_ATTRIBUTE_OFFLINE != 0 {
		return true, nil
	}

	return false, nil
}
