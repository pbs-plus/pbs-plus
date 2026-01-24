//go:build windows

package pxar

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

func getFilesystemCapabilities(path string) filesystemCapabilities {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return filesystemCapabilities{}
	}

	rootPath := filepath.VolumeName(absPath) + string(filepath.Separator)

	cap := filesystemCapabilities{}

	rootPathPtr := windows.StringToUTF16Ptr(rootPath)
	var fsFlags uint32
	var fsName [windows.MAX_PATH + 1]uint16

	err = windows.GetVolumeInformation(
		rootPathPtr,
		nil, 0, // Volume name buffer
		nil,      // Serial number
		nil,      // Max component length
		&fsFlags, // Filesystem flags
		&fsName[0], uint32(len(fsName)),
	)

	if err == nil {
		fs := windows.UTF16ToString(fsName[:])

		cap.supportsACLs = (fsFlags & windows.FILE_PERSISTENT_ACLS) != 0
		cap.supportsPersistentACLs = cap.supportsACLs
		cap.supportsXAttrs = true

		switch fs {
		case "FAT", "FAT32", "exFAT":
			cap.supportsACLs = false
			cap.supportsPersistentACLs = false
			cap.prefersSequentialOps = true
		}
	}

	return cap
}
