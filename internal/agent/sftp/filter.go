//go:build windows

package sftp

import (
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
)

func (h *SftpHandler) skipFile(path string) bool {
	stat, err := os.Lstat(path)
	if err != nil {
		return true
	}

	if !stat.IsDir() {
		// skip probably opened files
		if h.Snapshot.TimeStarted.Sub(stat.ModTime()) <= time.Minute {
			return true
		}

		d := stat.Sys().(*syscall.Win32FileAttributeData)
		cTime := time.Unix(0, d.CreationTime.Nanoseconds())

		if stat.ModTime().Before(cTime) {
			return true
		}
	}

	snapSplit := strings.Split(h.Snapshot.SnapshotPath, "\\")
	snapRoot := strings.Join(snapSplit[:len(snapSplit)-1], "\\")

	pathWithoutSnap := strings.TrimPrefix(path, snapRoot)
	normalizedPath := strings.ToUpper(strings.TrimPrefix(pathWithoutSnap, "\\"))

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	for _, regex := range cache.ExcludedPathRegexes {
		if regex.MatchString(normalizedPath) {
			return true
		}
	}

	invalidAttr, err := invalidAttributes(path)
	if err != nil || invalidAttr {
		return true
	}

	return false
}
