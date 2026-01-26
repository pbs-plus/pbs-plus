package utils

import (
	"path/filepath"
	"strings"
	"unicode"
)

func IsValid(path string) bool {
	// Check if path is not empty
	if path == "" {
		return false
	}

	cleanPath := filepath.Clean(path)

	if strings.HasPrefix(cleanPath, "..") || strings.Contains(cleanPath, string(filepath.Separator)+"..") {
		return false
	}

	if strings.Contains(path, "\x00") {
		return false
	}

	return true
}

func IsValidPathString(path string) bool {
	if path == "" {
		return true
	}

	if strings.Contains(path, "//") {
		return false
	}

	for _, r := range path {
		if r == 0 || !unicode.IsPrint(r) {
			return false
		}
	}

	return true
}
