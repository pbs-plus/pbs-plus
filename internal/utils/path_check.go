package utils

import (
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

const safeDir = "/home/user/"

func IsValid(path string) bool {
	// Check if path is not empty
	if path == "" {
		return false
	}

	// Resolve the input path with respect to the safe directory
	absPath, err := filepath.Abs(filepath.Join(safeDir, path))
	if err != nil || !strings.HasPrefix(absPath, safeDir) {
		return false
	}

	// Check if the path exists
	_, err = os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		return false
	}

	// Path exists, return true and no error
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
