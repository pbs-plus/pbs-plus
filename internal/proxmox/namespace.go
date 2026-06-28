package proxmox

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func EnsureNamespacePath(datastorePath, namespace string) error {
	if namespace == "" {
		return nil
	}

	parts := strings.Split(namespace, "/")
	fullPath := datastorePath

	for _, ns := range parts {
		fullPath = filepath.Join(fullPath, "ns", ns)
		if err := os.MkdirAll(fullPath, 0o755); err != nil {
			return fmt.Errorf("create namespace dir %q: %w", fullPath, err)
		}
		if err := os.Chown(fullPath, 34, 34); err != nil {
			return fmt.Errorf("chown namespace dir %q: %w", fullPath, err)
		}
	}

	return nil
}
