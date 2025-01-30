package vssfs

import (
	"path/filepath"
	"strings"

	"github.com/zeebo/xxh3"
)

func generateFullPathID(path string) uint64 {
	hash := xxh3.HashString(path)
	depth := uint64(getPathDepth(path))
	length := uint64(len(path))
	ext := getExtensionHash(path)

	return (depth&0x3f)<<58 | // 6 bits depth (max 63)
		(length&0x3ff)<<48 | // 10 bits length (max 1023)
		(uint64(ext)&0xff)<<40 | // 8 bits extension
		(hash>>24)&0xffffffffff // 40 bits hash (upper 40 of 64)
}

func quickMatch(id uint64, path string) bool {
	// Check length first (cheapest operation)
	pathLen := uint64(len(path))
	if (id >> 48 & 0x3ff) != pathLen {
		return false
	}

	// Then check depth
	depth := uint64(getPathDepth(path))
	if (id >> 58) != depth {
		return false
	}

	// Finally check partial hash (adjust bit positions to match generateFullPathID)
	hash := xxh3.HashString(path)
	return (id & 0xffffffffff) == ((hash >> 24) & 0xffffffffff)
}

func getPathDepth(path string) int {
	// Count both types of separators for cross-platform support
	return strings.Count(path, "/") + strings.Count(path, "\\")
}

func getParentPath(path string) string {
	parent := filepath.Dir(path)
	if parent == "." || parent == path {
		return ""
	}
	return parent
}

func getFileName(path string) string {
	return filepath.Base(path)
}

func getExtensionHash(path string) uint8 {
	ext := filepath.Ext(path)
	if ext == "" {
		return 0
	}
	if ext[0] == '.' {
		ext = ext[1:] // Remove leading dot
	}
	h := xxh3.HashString(ext)
	return uint8(h>>56) ^ uint8(h>>48) ^ uint8(h>>40) ^ uint8(h>>32)
}
