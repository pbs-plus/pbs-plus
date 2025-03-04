//go:build windows

package vssfs

import (
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

const (
	excludedAttrs = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
)

func (s *VSSFSServer) readDirBulk(dirPath string) ([]byte, error) {
	// Open the directory
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read all entries
	rawEntries, err := f.ReadDir(-1)
	if err != nil {
		return nil, err
	}

	// Filter entries based on attributes
	entries := make(ReadDirEntries, 0, len(rawEntries))
	for _, entry := range rawEntries {
		// Get detailed file info
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Get sys info for Windows-specific attributes
		if sys, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
			// Skip if file has any of the excluded attributes
			if sys.FileAttributes&excludedAttrs != 0 {
				continue
			}
		}

		entries = append(entries, VSSDirEntry{
			Name: []byte(entry.Name()),
			Mode: uint32(entry.Type()),
		})
	}

	return entries.MarshalMsg(nil)
}
