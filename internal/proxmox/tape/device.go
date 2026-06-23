package tape

import (
	"os"
	"strings"
)

// ResolveDevice converts a -sg device path to its -nst non-rewind tape
// device equivalent, if it exists.
func ResolveDevice(path string) string {
	if path == "" {
		return path
	}
	if before, ok := strings.CutSuffix(path, "-sg"); ok {
		nstPath := before + "-nst"
		if _, err := os.Stat(nstPath); err == nil {
			return nstPath
		}
	}
	return path
}
