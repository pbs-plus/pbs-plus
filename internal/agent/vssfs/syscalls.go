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

func readDir(dir string) ([]*VSSFileInfo, error) {
	searchPath := filepath.Join(dir, "*")
	searchPathPtr, err := windows.UTF16PtrFromString(searchPath)
	if err != nil {
		return nil, err
	}
	var findData windows.Win32finddata
	// You may substitute FindFirstFileEx here if that is your normal method.
	handle, err := windows.FindFirstFile(searchPathPtr, &findData)
	if err != nil {
		return nil, err
	}
	defer windows.FindClose(handle)

	var entries []*VSSFileInfo
	for {
		name := windows.UTF16ToString(findData.FileName[:])
		if name != "." && name != ".." {
			if !skipPathWithAttributes(findData.FileAttributes) {
				entry := createFileInfoFromFindData(name, &findData)
				entries = append(entries, entry)
			}
		}
		err = windows.FindNextFile(handle, &findData)
		if err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break
			}
			return nil, err
		}
	}
	return entries, nil
}
