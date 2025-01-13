//go:build windows
// +build windows

package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mxk/go-vss"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
)

var (
	ErrSnapshotTimeout  = errors.New("timeout waiting for in-progress snapshot")
	ErrSnapshotCreation = errors.New("failed to create snapshot")
	ErrInvalidSnapshot  = errors.New("invalid snapshot")
)

// WinVSSSnapshot represents a Windows Volume Shadow Copy snapshot
type WinVSSSnapshot struct {
	SnapshotPath string    `json:"path"`
	Id           string    `json:"vss_id"`
	TimeStarted  time.Time `json:"time_started"`
	closed       atomic.Bool
}

// getVSSFolder returns the path to the VSS working directory
func getVSSFolder() (string, error) {
	tmpDir := os.TempDir()
	configBasePath := filepath.Join(tmpDir, "pbs-plus-vss")

	if err := os.MkdirAll(configBasePath, 0750); err != nil {
		return "", fmt.Errorf("failed to create VSS directory %q: %w", configBasePath, err)
	}

	return configBasePath, nil
}

// Snapshot creates a new VSS snapshot for the specified drive
func Snapshot(driveLetter string) (*WinVSSSnapshot, error) {
	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))

	vssFolder, err := getVSSFolder()
	if err != nil {
		return nil, fmt.Errorf("error getting VSS folder: %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, driveLetter)
	timeStarted := time.Now()

	// Clean up any existing snapshot before creating new one
	cleanupExistingSnapshot(snapshotPath)

	// Create snapshot with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := createSnapshotWithRetry(ctx, snapshotPath, volName); err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return nil, fmt.Errorf("snapshot creation failed: %w", err)
	}

	// Validate snapshot
	sc, err := vss.Get(snapshotPath)
	if err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return nil, fmt.Errorf("snapshot validation failed: %w", err)
	}

	snapshot := &WinVSSSnapshot{
		SnapshotPath: snapshotPath,
		Id:           sc.ID,
		TimeStarted:  timeStarted,
	}

	// Initialize caches
	cache.ExcludedPathRegexes = cache.CompileExcludedPaths()
	cache.PartialFilePathRegexes = cache.CompilePartialFileList()

	return snapshot, nil
}

// createSnapshotWithRetry attempts to create a snapshot with retries on conflicts
func createSnapshotWithRetry(ctx context.Context, snapshotPath, volName string) error {
	const retryInterval = time.Second

	for {
		if err := vss.CreateLink(snapshotPath, volName); err == nil {
			return nil
		} else if !strings.Contains(err.Error(), "shadow copy operation is already in progress") {
			return fmt.Errorf("%w: %v", ErrSnapshotCreation, err)
		}

		select {
		case <-ctx.Done():
			return ErrSnapshotTimeout
		case <-time.After(retryInterval):
			continue
		}
	}
}

// cleanupExistingSnapshot removes any existing snapshot and its path
func cleanupExistingSnapshot(path string) {
	_ = vss.Remove(path)
	_ = os.Remove(path)
}

// Close cleans up the snapshot and associated resources
func (s *WinVSSSnapshot) Close() {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return
	}

	if fileMap, ok := cache.SizeCache.Load(s.Id); ok {
		clear(fileMap.(map[string]int64))
		cache.SizeCache.Delete(s.Id)
	}

	cleanupExistingSnapshot(s.SnapshotPath)
}
