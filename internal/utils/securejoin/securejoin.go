package securejoin

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrNotAllowed   = errors.New("path component not allowed")
	ErrSymlinkLoop  = errors.New("symbolic link loop detected")
	ErrInvalidDrive = errors.New("invalid drive specification")
)

// isWindowsPath checks if the path starts with a drive letter or UNC path
func isWindowsPath(path string) bool {
	if len(path) < 2 {
		return false
	}
	// Check for drive letter (C:)
	if len(path) >= 2 && path[1] == ':' {
		return true
	}
	// Check for UNC (\\server\share)
	if strings.HasPrefix(path, "\\\\") || strings.HasPrefix(path, "//") {
		return true
	}
	return false
}

// normalizeWindowsPath converts all slashes to backslashes and handles UNC paths
func normalizeWindowsPath(path string) string {
	// Convert forward slashes to backslashes
	path = strings.ReplaceAll(path, "/", "\\")

	// Handle double backslashes at start (UNC paths)
	if strings.HasPrefix(path, "\\\\") {
		return path
	}

	return path
}

// SecureJoin performs a secure join of a base directory and untrusted input path
func SecureJoin(baseDir, unsafePath string) (string, error) {
	// Normalize paths for Windows
	baseDir = normalizeWindowsPath(baseDir)
	unsafePath = normalizeWindowsPath(unsafePath)

	// Ensure base directory is absolute
	if !filepath.IsAbs(baseDir) {
		return "", errors.New("base directory must be absolute")
	}

	// Handle drive letter in unsafe path
	if isWindowsPath(unsafePath) {
		return "", ErrInvalidDrive
	}

	// Remove leading separators from unsafe path
	unsafePath = strings.TrimPrefix(unsafePath, "\\")

	// Split path into components
	parts := strings.Split(unsafePath, "\\")

	// Start with absolute base directory
	result := baseDir

	// Track visited paths to detect symlink loops
	visited := make(map[string]bool)

	for _, part := range parts {
		// Skip empty components and current directory
		if part == "" || part == "." {
			continue
		}

		// Prevent directory traversal
		if part == ".." {
			return "", ErrNotAllowed
		}

		// Join next component
		nextPath := filepath.Join(result, part)

		// Evaluate any symbolic links
		evalPath, err := filepath.EvalSymlinks(nextPath)
		if err != nil {
			if os.IsNotExist(err) {
				// If path doesn't exist yet, just use the joined path
				result = nextPath
				continue
			}
			return "", err
		}

		// Normalize evaluated path
		evalPath = normalizeWindowsPath(evalPath)

		// Check for symlink loops
		if visited[evalPath] {
			return "", ErrSymlinkLoop
		}
		visited[evalPath] = true

		// Ensure path is still under base directory
		if !strings.HasPrefix(strings.ToLower(evalPath),
			strings.ToLower(baseDir)) {
			return "", ErrNotAllowed
		}

		result = evalPath
	}

	return result, nil
}
