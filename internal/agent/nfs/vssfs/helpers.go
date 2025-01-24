//go:build windows

package vssfs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"golang.org/x/sys/windows"
)

type attrCacheEntry struct {
	expiry  time.Time
	invalid bool
}

var (
	attrCache sync.Map // map[string]attrCacheEntry
	cacheTTL  = 5 * time.Minute
)

func skipPath(path string, snapshot *snapshots.WinVSSSnapshot, exclusions []*regexp.Regexp) bool {
	pathWithoutSnap := strings.TrimPrefix(path, snapshot.SnapshotPath)
	normalizedPath := strings.ToUpper(strings.TrimPrefix(pathWithoutSnap, "\\"))

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	baseName := filepath.Base(normalizedPath)
	for _, regex := range exclusions {
		if regex.MatchString(baseName) {
			return true
		}
	}

	for _, regex := range exclusions {
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
	if entry, ok := attrCache.Load(path); ok {
		if time.Now().Before(entry.(attrCacheEntry).expiry) {
			return entry.(attrCacheEntry).invalid, nil
		}
	}

	invalid := false

	defer func() {
		attrCache.Store(path, attrCacheEntry{
			expiry:  time.Now().Add(cacheTTL),
			invalid: invalid,
		})
	}()

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
		windows.FILE_ATTRIBUTE_REPARSE_POINT,
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

	// Single system call with optimal flags
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|
			windows.FILE_FLAG_OPEN_REPARSE_POINT|
			windows.FILE_FLAG_SEQUENTIAL_SCAN,
		0,
	)
	if err != nil {
		return 0, 0, err
	}
	defer windows.CloseHandle(handle)

	if err := windows.GetFileInformationByHandle(handle, fi); err != nil {
		return 0, 0, err
	}

	// Compute stable ID from retrieved information
	fileIndex := uint64(fi.FileIndexHigh)<<32 | uint64(fi.FileIndexLow)
	stableID := (uint64(fi.VolumeSerialNumber) << 32) | (fileIndex & 0xFFFFFFFF)

	return stableID, fi.NumberOfLinks, nil
}

// Use existing syscall data when possible
func computeIDFromExisting(fi *windows.ByHandleFileInformation) (uint64, uint32) {
	fileIndex := uint64(fi.FileIndexHigh)<<32 | uint64(fi.FileIndexLow)
	return (uint64(fi.VolumeSerialNumber) << 32) | (fileIndex & 0xFFFFFFFF), fi.NumberOfLinks
}
