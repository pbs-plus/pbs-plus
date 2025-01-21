//go:build windows
// +build windows

package nfs

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"golang.org/x/sys/windows"
)

type CustomFileInfo struct {
	os.FileInfo
	filePath   string
	snapshotId string
}

func (f *CustomFileInfo) Size() int64 {
	metadataSize := f.FileInfo.Size()
	scanFile := false

	// Check if file matches partial file patterns
	for _, regex := range cache.PartialFilePathRegexes {
		if regex.MatchString(f.filePath) {
			scanFile = true
			break
		}
	}

	if !scanFile {
		return metadataSize
	}

	// Check size cache
	if snapSizes, ok := cache.SizeCache.Load(f.snapshotId); ok {
		if cachedSize, ok := snapSizes.(map[string]int64)[f.filePath]; ok {
			return cachedSize
		}
	}

	// Compute actual file size
	file, err := os.Open(f.filePath)
	if err != nil {
		return 0
	}
	defer file.Close()

	byteCount, err := io.Copy(io.Discard, file)
	if err != nil {
		return 0
	}

	// Cache the computed size
	snapSizes, _ := cache.SizeCache.LoadOrStore(f.snapshotId, map[string]int64{})
	snapSizes.(map[string]int64)[f.filePath] = byteCount

	return byteCount
}

func skipFile(path string, snapshot *snapshots.WinVSSSnapshot) bool {
	stat, err := os.Lstat(path)
	if err != nil {
		return true
	}

	if !stat.IsDir() {
		// Skip recently modified files
		if snapshot.TimeStarted.Sub(stat.ModTime()) <= time.Minute {
			return true
		}
	}

	// Check path against exclusion patterns
	snapSplit := strings.Split(snapshot.SnapshotPath, "\\")
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
