//go:build windows

package vssfs

import (
	"os"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"golang.org/x/sys/windows"
)

func skipFile(path string, snapshot *snapshots.WinVSSSnapshot) bool {
	pathWithoutSnap := strings.TrimPrefix(path, snapshot.SnapshotPath)
	normalizedPath := strings.ToUpper(strings.TrimPrefix(pathWithoutSnap, "\\"))

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	for _, regex := range cache.ExcludedPathRegexes {
		if regex.MatchString(normalizedPath) {
			return true
		}
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

	// Check for invalid Windows file attributes
	invalidAttributes := []uint32{
		windows.FILE_ATTRIBUTE_TEMPORARY,
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN,
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS,
		windows.FILE_ATTRIBUTE_VIRTUAL,
		windows.FILE_ATTRIBUTE_OFFLINE,
		windows.FILE_ATTRIBUTE_REPARSE_POINT,
	}

	for _, attr := range invalidAttributes {
		if attributes&attr != 0 {
			return true, nil
		}
	}

	return false, nil
}
