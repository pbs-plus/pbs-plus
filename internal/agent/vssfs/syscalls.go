//go:build windows

package vssfs

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

func stat(path string) (*VSSFileInfo, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPtr, &findData)
	if err != nil {
		return nil, err
	}
	defer windows.FindClose(handle)

	name := windows.UTF16ToString(findData.FileName[:])

	info := createFileInfoFromFindData(name, &findData)

	return info, nil
}

func readDir(dir string) (ReadDirEntries, error) {
	searchPath := filepath.Join(dir, "*")
	var findData windows.Win32finddata
	handle, err := FindFirstFileEx(searchPath, &findData)
	if err != nil {
		return nil, err
	}
	defer windows.FindClose(handle)

	var entries []*VSSFileInfo
	for {
		name := windows.UTF16ToString(findData.FileName[:])
		if name != "." && name != ".." {
			if !skipPathWithAttributes(findData.FileAttributes) {
				info := createFileInfoFromFindData(name, &findData)
				entries = append(entries, info)
			}
		}
		if err := windows.FindNextFile(handle, &findData); err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break
			}
			return nil, err
		}
	}
	return entries, nil
}
