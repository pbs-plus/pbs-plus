//go:build windows

package vssfs

import (
	"golang.org/x/sys/windows"
)

var invalidAttributes = []uint32{
	windows.FILE_ATTRIBUTE_TEMPORARY,
	windows.FILE_ATTRIBUTE_RECALL_ON_OPEN,
	windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS,
	windows.FILE_ATTRIBUTE_VIRTUAL,
	windows.FILE_ATTRIBUTE_OFFLINE,
	windows.FILE_ATTRIBUTE_REPARSE_POINT,
}

func skipPathWithAttributes(attrs uint32) bool {
	return hasInvalidAttributes(attrs)
}

func hasInvalidAttributes(attrs uint32) bool {
	for _, attr := range invalidAttributes {
		if attrs&attr != 0 {
			return true
		}
	}
	return false
}

func fastHash(s string) uint64 {
	// FNV-1a hash constants
	const offset64 = uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)

	// Initialize hash with offset
	hash := offset64

	// Iterate through string bytes
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}

	return hash
}

// getFileIDWindows computes the hash without creating an intermediate uppercase string
func getFileIDWindows(path string, findData *windows.Win32finddata) uint64 {
	// Extract components from Win32finddata
	fileSize := uint64(findData.FileSizeHigh)<<32 | uint64(findData.FileSizeLow)
	creationTime := uint64(findData.CreationTime.HighDateTime)<<32 |
		uint64(findData.CreationTime.LowDateTime)

	// Generate filename component
	nameComponent := fastHash(path)

	// Start with creation time (but reserve some bits)
	id := creationTime & 0xFFFFFFFFFF000000

	// Mix in filename component in a non-overlapping way
	id |= (nameComponent & 0xFFFFFF)

	// Mix in file size bits
	id ^= (fileSize & 0xFFFF) << 24

	// Mix in some file attributes
	attrs := uint64(findData.FileAttributes)
	relevantAttrs := attrs & (windows.FILE_ATTRIBUTE_DIRECTORY |
		windows.FILE_ATTRIBUTE_COMPRESSED |
		windows.FILE_ATTRIBUTE_ENCRYPTED |
		windows.FILE_ATTRIBUTE_SPARSE_FILE)

	id ^= (relevantAttrs << 40)

	return id
}
