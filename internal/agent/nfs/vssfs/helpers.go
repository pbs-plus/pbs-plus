//go:build windows

package vssfs

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
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
func getFileIDWindows(path string) uint64 {
	h := xxhash.New()
	buf := make([]byte, 1) // Reusable buffer for single-byte writes
	for i := 0; i < len(path); {
		c := path[i]
		if c < utf8.RuneSelf { // ASCII optimization
			// Convert to uppercase and write
			if 'a' <= c && c <= 'z' {
				c -= 'a' - 'A'
			}
			buf[0] = c
			h.Write(buf)
			i++
		} else { // Unicode path
			r, size := utf8.DecodeRuneInString(path[i:])
			upper := unicode.ToUpper(r)
			var runeBuf [utf8.UTFMax]byte
			n := utf8.EncodeRune(runeBuf[:], upper)
			h.Write(runeBuf[:n])
			i += size
		}
	}
	return h.Sum64()
}
