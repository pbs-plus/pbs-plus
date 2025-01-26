//go:build windows

package vssfs

import (
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	"golang.org/x/sys/windows"
)

func skipPathWithAttributes(path string, attrs uint32, snapshot *snapshots.WinVSSSnapshot, exclusions []*pattern.GlobPattern) bool {
	pathWithoutSnap := strings.TrimPrefix(path, snapshot.SnapshotPath)
	normalizedPath := strings.ToUpper(strings.TrimPrefix(pathWithoutSnap, "\\"))

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	for _, excl := range exclusions {
		if excl.Match(normalizedPath) {
			return true
		}
	}

	return hasInvalidAttributes(attrs)
}

func hasInvalidAttributes(attrs uint32) bool {
	invalidAttributes := map[uint32]string{
		windows.FILE_ATTRIBUTE_TEMPORARY:             "TEMPORARY",
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN:        "RECALL_ON_OPEN",
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS: "RECALL_ON_DATA_ACCESS",
		windows.FILE_ATTRIBUTE_VIRTUAL:               "VIRTUAL",
		windows.FILE_ATTRIBUTE_OFFLINE:               "OFFLINE",
		windows.FILE_ATTRIBUTE_REPARSE_POINT:         "REPARSE_POINT",
	}

	for attr := range invalidAttributes {
		if attrs&attr != 0 {
			return true
		}
	}
	return false
}

func getFileIDWindows(path string) uint64 {
	path = strings.ToUpper(path)
	hash := xxhash.Sum64String(path)
	return hash
}
