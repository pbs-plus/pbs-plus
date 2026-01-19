package snapshots

import (
	"fmt"
	"path/filepath"
)

func detectFilesystem(path string) (string, string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("failed to get absolute path: %w", err)
	}

	return getFsType(absPath)
}
