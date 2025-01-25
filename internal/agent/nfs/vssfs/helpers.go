//go:build windows

package vssfs

import (
	"os"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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
		syslog.L.Infof("Skipping due to exclusion matching: %s", path)
		return true
	}

	invalid, err := hasInvalidAttributes(path)
	if err != nil || invalid {
		syslog.L.Infof("Skipping due to invalid attributes: %s", path)
		return true
	}

	return false
}

func hasInvalidAttributes(path string) (bool, error) {
	invalid := false

	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		invalid = false
		return invalid, err
	}

	attributes, err := windows.GetFileAttributes(p)
	if err != nil {
		invalid = false
		return invalid, os.NewSyscallError("GetFileAttributes", err)
	}

	// Check for invalid Windows file attributes
	invalidAttributes := []uint32{
		windows.FILE_ATTRIBUTE_TEMPORARY,
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN,
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS,
		windows.FILE_ATTRIBUTE_VIRTUAL,
		windows.FILE_ATTRIBUTE_OFFLINE,
	}
	for _, attr := range invalidAttributes {
		if attributes&attr != 0 {
			invalid = true
			return invalid, nil
		}
	}

	return invalid, nil
}

func getFileIDWindows(path string, fi *windows.ByHandleFileInformation) (uint64, uint32, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|
			windows.FILE_FLAG_OPEN_REPARSE_POINT|
			windows.FILE_FLAG_POSIX_SEMANTICS, // Added for better POSIX compliance
		0,
	)
	if err != nil {
		return 0, 0, err
	}
	defer windows.CloseHandle(handle)

	if err := windows.GetFileInformationByHandle(handle, fi); err != nil {
		return 0, 0, err
	}

	// Combine all available file information for maximum uniqueness
	fileIndex := uint64(fi.FileIndexHigh)<<32 | uint64(fi.FileIndexLow)
	stableID := uint64(fi.VolumeSerialNumber)<<48 | // Use 16 bits from volume serial
		(fileIndex & 0xFFFFFFFFFFFF) // Use 48 bits from file index high and low

	return stableID, fi.NumberOfLinks, nil
}

// Use existing syscall data when possible
func computeIDFromExisting(fi *windows.ByHandleFileInformation) (uint64, uint32) {
	fileIndex := uint64(fi.FileIndexHigh)<<32 | uint64(fi.FileIndexLow)
	return uint64(fi.VolumeSerialNumber)<<48 | (fileIndex & 0xFFFFFFFFFFFF), fi.NumberOfLinks
}
