//go:build windows

package vssfs

import (
	"strings"
	"unicode"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
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

func skipPathWithAttributes(path string, attrs uint32, snapshot *snapshots.WinVSSSnapshot, exclusions []*pattern.GlobPattern) bool {
	pathWithoutSnap := strings.TrimPrefix(path, snapshot.SnapshotPath)

	// Manually trim leading backslash to avoid TrimPrefix overhead
	if len(pathWithoutSnap) > 0 && pathWithoutSnap[0] == '\\' {
		pathWithoutSnap = pathWithoutSnap[1:]
	}

	normalizedPath := normalizePath(pathWithoutSnap)

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	if exclusions != nil {
		for _, excl := range exclusions {
			if excl.Match(normalizedPath) {
				return true
			}
		}
	}

	return hasInvalidAttributes(attrs)
}

// normalizePath processes the path in a single pass to:
// 1. Replace backslashes with forward slashes
// 2. Convert all characters to uppercase
func normalizePath(s string) string {
	var builder strings.Builder
	builder.Grow(len(s)) // Pre-allocate to minimize reallocations

	for _, c := range s {
		if c == '\\' {
			builder.WriteByte('/')
		} else {
			builder.WriteRune(unicode.ToUpper(c))
		}
	}
	return builder.String()
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
