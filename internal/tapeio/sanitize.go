package tapeio

import (
	"path/filepath"
	"strings"

	_ "github.com/pbs-plus/go-mtf/besetmap"
)

func lastNameSegment(relPath string) (name string, depth int) {
	lastSep := -1
	for i := 0; i < len(relPath); i++ {
		if relPath[i] == '/' {
			depth++
			lastSep = i
		}
	}
	return relPath[lastSep+1:], depth
}

func sanitizeName(name string) string {
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, "\\", "/")
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" || name == "." || name == ".." {
		name = "_"
	}
	return name
}

// sanitizePath replaces path-unsafe characters so a backup ID can be used

// sanitizePath replaces path-unsafe characters so a backup ID can be used
func sanitizePath(s string) string {
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, ":", "_")
	if s == "" {
		s = "_"
	}
	return s
}
