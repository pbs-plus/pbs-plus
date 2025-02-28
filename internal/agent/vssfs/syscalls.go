//go:build windows

package vssfs

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

// Optimized stat with invalidation on first access
func (fc *FSCache) stat(path string) (*VSSFileInfo, error) {
	// Check cache first
	if entry, cached := fc.getStatCache(path); entry != nil && cached {
		return entry, nil
	}

	// Not in cache, perform actual stat operation
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, mapWinError(err, path)
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPtr, &findData)
	if err != nil {
		return nil, mapWinError(err, path)
	}
	defer windows.FindClose(handle)

	name := windows.UTF16ToString(findData.FileName[:])
	info := createFileInfoFromFindData(name, &findData)

	// Cache the new entry
	fc.addStatEntry(path, info)

	return info, nil
}

// Process a directory and cache its contents
func (fc *FSCache) readDir(dirPath string) (ReadDirEntries, error) {
	// Check cache first
	if entry, cached := fc.getDirCache(dirPath); entry != nil && cached {
		return entry, nil
	}

	// Read directory
	searchPath := filepath.Join(dirPath, "*")
	var findData windows.Win32finddata
	handle, err := FindFirstFileEx(searchPath, &findData)
	if err != nil {
		return nil, mapWinError(err, dirPath)
	}
	defer windows.FindClose(handle)

	var paths []string
	var toReturn ReadDirEntries
	for {
		name := windows.UTF16ToString(findData.FileName[:])

		// Skip . and .. entries
		if name != "." && name != ".." {
			if !skipPathWithAttributes(findData.FileAttributes) {
				fullPath := filepath.Join(dirPath, name)
				info := createFileInfoFromFindData(name, &findData)

				fc.addStatEntry(fullPath, info)
				paths = append(paths, fullPath)
				toReturn = append(toReturn, info)
			}
		}

		if err := windows.FindNextFile(handle, &findData); err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break
			}
			return nil, mapWinError(err, dirPath)
		}
	}

	// Store in directory cache
	dirEntry := &dirCacheEntry{
		paths: paths,
	}
	fc.dirCache.Store(dirPath, dirEntry)

	return toReturn, nil
}
