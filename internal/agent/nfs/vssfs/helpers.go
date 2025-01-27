//go:build windows

package vssfs

import (
	"strings"
	"unicode"
	"unicode/utf16"

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

// getFileIDWindows computes the hash without creating an intermediate uppercase string
func getFileIDWindows(filename string, findData *windows.Win32finddata) uint64 {
	// Extract components from Win32finddata
	fileSize := uint64(findData.FileSizeHigh)<<32 | uint64(findData.FileSizeLow)
	creationTime := uint64(findData.CreationTime.HighDateTime)<<32 |
		uint64(findData.CreationTime.LowDateTime)

	// Get filename as UTF16
	filenameUTF16 := utf16.Encode([]rune(filename))

	// Generate filename component
	nameComponent := uint64FromStr(filenameUTF16)

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

func uint64FromStr(str []uint16) uint64 {
	if len(str) == 0 {
		return 0
	}

	// Take up to first 4 characters and last 4 characters
	// This captures both prefix and suffix while being efficient
	var mix uint64

	// Mix in first chars (up to 4)
	for i := 0; i < len(str) && i < 4; i++ {
		mix = (mix << 8) | uint64(str[i]&0xFF)
	}

	// Mix in last chars (up to 4)
	if len(str) > 4 {
		for i := max(len(str)-4, 4); i < len(str); i++ {
			mix = (mix << 4) | uint64(str[i]&0x0F)
		}
	}

	// Mix in length for additional uniqueness
	mix = (mix << 8) | uint64(len(str)&0xFF)

	return mix
}
