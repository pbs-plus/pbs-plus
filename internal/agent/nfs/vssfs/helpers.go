//go:build windows

package vssfs

import (
	"os"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	"golang.org/x/sys/windows"
)

func skipPath(path string, snapshot *snapshots.WinVSSSnapshot, exclusions *pattern.Matcher) bool {
	pathWithoutSnap := strings.TrimPrefix(path, snapshot.SnapshotPath)
	normalizedPath := strings.ToUpper(strings.TrimPrefix(pathWithoutSnap, "\\"))

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	if matched, _ := exclusions.Match(normalizedPath); matched {
		return true
	}

	invalid, err := hasInvalidAttributes(path)
	if err != nil || invalid {
		return true
	}

	return false
}

func hasInvalidAttributes(path string) (bool, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false, err
	}

	attributes, err := windows.GetFileAttributes(p)
	if err != nil {
		return false, os.NewSyscallError("GetFileAttributes", err)
	}

	invalidAttributes := map[uint32]string{
		windows.FILE_ATTRIBUTE_TEMPORARY:             "TEMPORARY",
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN:        "RECALL_ON_OPEN",
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS: "RECALL_ON_DATA_ACCESS",
		windows.FILE_ATTRIBUTE_VIRTUAL:               "VIRTUAL",
		windows.FILE_ATTRIBUTE_OFFLINE:               "OFFLINE",
		windows.FILE_ATTRIBUTE_REPARSE_POINT:         "REPARSE_POINT",
	}

	for attr := range invalidAttributes {
		if attributes&attr != 0 {
			return true, nil
		}
	}
	return false, nil
}

func getFileIDWindows(path string) uint64 {
	path = strings.ToUpper(path)
	hash := xxhash.Sum64String(path)
	return hash
}
