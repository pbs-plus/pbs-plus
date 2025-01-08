//go:build windows
// +build windows

package snapshots

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mxk/go-vss"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
)

type WinVSSSnapshot struct {
	SnapshotPath string    `json:"path"`
	Id           string    `json:"vss_id"`
	TimeStarted  time.Time `json:"time_started"`
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func getVSSFolder() (string, error) {
	tmpDir := os.TempDir()
	configBasePath := filepath.Join(tmpDir, "pbs-plus-vss")
	err := os.MkdirAll(configBasePath, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("getVSSFolder: failed to create directory \"%s\" -> %w", configBasePath, err)
	}
	return configBasePath, nil
}

func Snapshot(driveLetter string) (*WinVSSSnapshot, error) {
	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))
	vssFolder, err := getVSSFolder()
	if err != nil {
		return nil, fmt.Errorf("Snapshot: error getting app data folder -> %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, driveLetter)
	timeStarted := time.Now()

	if err := cleanupExistingSnapshot(snapshotPath); err != nil {
		return nil, fmt.Errorf("Snapshot: cleanup failed -> %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err = createSnapshotWithRetry(ctx, snapshotPath, volName)
	if err != nil {
		return nil, fmt.Errorf("Snapshot: creation failed -> %w", err)
	}

	sc, err := vss.Get(snapshotPath)
	if err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return nil, fmt.Errorf("Snapshot: validation failed -> %w", err)
	}

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	snapshot := &WinVSSSnapshot{
		SnapshotPath: snapshotPath,
		Id:           sc.ID,
		TimeStarted:  timeStarted,
		cancel:       monitorCancel,
	}

	snapshot.wg.Add(1)
	go func() {
		defer snapshot.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				if _, err := vss.Get(snapshotPath); err != nil {
					// Attempt to recreate the snapshot if it's invalid
					if err := vss.CreateLink(snapshotPath, volName); err != nil {
						// If recreation fails, cancel the context to signal failure
						monitorCancel()
						return
					}
				}
			}
		}
	}()

	cache.ExcludedPathRegexes = cache.CompileExcludedPaths()
	cache.PartialFilePathRegexes = cache.CompilePartialFileList()

	return snapshot, nil
}

func createSnapshotWithRetry(ctx context.Context, snapshotPath, volName string) error {
	for {
		err := vss.CreateLink(snapshotPath, volName)
		if err == nil {
			return nil
		}

		if !strings.Contains(err.Error(), "shadow copy operation is already in progress") {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for in-progress snapshot")
		case <-time.After(time.Second):
			continue
		}
	}
}

func cleanupExistingSnapshot(path string) error {
	if err := vss.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (instance *WinVSSSnapshot) Close() {
	if instance == nil {
		return
	}

	if instance.cancel != nil {
		instance.cancel()
		instance.wg.Wait()
	}

	if fileMap, ok := cache.SizeCache.Load(instance.Id); ok {
		clear(fileMap.(map[string]int64))
		cache.SizeCache.Delete(instance.Id)
	}

	cleanupExistingSnapshot(instance.SnapshotPath)
}

