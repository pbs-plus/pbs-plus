//go:build windows
// +build windows

package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mxk/go-vss"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
)

func getVSSFolder() (string, error) {
	tmpDir := os.TempDir()

	configBasePath := filepath.Join(tmpDir, "pbs-plus-vss")

	err := os.MkdirAll(configBasePath, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("getVSSFolder: failed to create directory \"%s\" -> %w", configBasePath, err)
	}

	return configBasePath, nil
}

func Snapshot(driveLetter string, forceNew bool) (*WinVSSSnapshot, error) {
	knownSnaps := &KnownSnapshots{registry: "KnownSnaps"}
	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))

	// Check if there's an existing valid snapshot
	vssFolder, err := getVSSFolder()
	if err != nil {
		return nil, fmt.Errorf("Snapshot: error getting app data folder -> %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, driveLetter)
	if knownSnap, err := knownSnaps.Get(snapshotPath); err == nil {
		if _, err := vss.Get(snapshotPath); err == nil {
			if time.Since(knownSnap.GetTimestamp()) < 15*time.Minute && !forceNew {
				return knownSnap, nil
			}
		}

		knownSnap.Close()
		_ = vss.Remove(snapshotPath) // Clean up old snapshot link if expired
		_ = os.Remove(snapshotPath)
	}

	timeStarted := time.Now()
	// Attempt to create a new snapshot
	err = vss.CreateLink(snapshotPath, volName)
	if err != nil {
		if strings.Contains(err.Error(), "shadow copy operation is already in progress") {
			// Wait for the in-progress shadow copy operation to complete
			for {
				if _, err := vss.Get(snapshotPath); err == nil {
					break
				}
			}
		} else if strings.Contains(err.Error(), "already exists") {
			_ = vss.Remove(snapshotPath)
			_ = os.Remove(snapshotPath)

			timeStarted = time.Now()
			err = vss.CreateLink(snapshotPath, volName)
			if err != nil {
				return nil, fmt.Errorf("Snapshot: error creating snapshot (%s to %s) -> %w", volName, snapshotPath, err)
			}
		} else {
			return nil, fmt.Errorf("Snapshot: error creating snapshot (%s to %s) -> %w", volName, snapshotPath, err)
		}
	}

	// Retrieve snapshot details and save the new snapshot
	sc, err := vss.Get(snapshotPath)
	if err != nil {
		_ = vss.Remove(snapshotPath)
		_ = os.Remove(snapshotPath)
		return nil, fmt.Errorf("Snapshot: error getting snapshot details -> %w", err)
	}

	newSnapshot := &WinVSSSnapshot{
		SnapshotPath: snapshotPath,
		LastAccessed: time.Now(),
		Id:           sc.ID,
		TimeStarted:  timeStarted,
	}
	knownSnaps.Save(newSnapshot)

	cache.ExcludedPathRegexes = cache.CompileExcludedPaths()
	cache.PartialFilePathRegexes = cache.CompilePartialFileList()

	return newSnapshot, nil
}

func (instance *WinVSSSnapshot) GetTimestamp() time.Time {
	if instance.LastAccessed.IsZero() {
		knownSnaps := &KnownSnapshots{
			registry: "KnownSnaps",
		}

		snap, err := knownSnaps.Get(instance.SnapshotPath)
		if err == nil {
			instance.LastAccessed = snap.LastAccessed
			return snap.LastAccessed
		}
	}

	return instance.LastAccessed
}

func (instance *WinVSSSnapshot) UpdateTimestamp() {
	knownSnaps := &KnownSnapshots{
		registry: "KnownSnaps",
	}

	instance.LastAccessed = time.Now()

	knownSnaps.Save(instance)
}

func (instance *WinVSSSnapshot) Close() {
	if fileMap, ok := cache.SizeCache.Load(instance.Id); ok {
		clear(fileMap.(map[string]int64))
		cache.SizeCache.Delete(instance.Id)
	}

	_ = vss.Remove(instance.Id)
	_ = os.Remove(instance.SnapshotPath)

	knownSnaps := &KnownSnapshots{
		registry: "KnownSnaps",
	}

	knownSnaps.Delete(instance.SnapshotPath)
}

func CloseAllSnapshots() {
	knownSnaps := &KnownSnapshots{
		registry: "KnownSnaps",
	}

	if knownSnapshots, err := knownSnaps.GetAll(); err == nil {
		for _, snapshot := range knownSnapshots {
			snapshot.Close()
		}
	}
}

func GarbageCollect() {
	knownSnaps := &KnownSnapshots{
		registry: "KnownSnaps",
	}

	if knownSnapshots, err := knownSnaps.GetAll(); err == nil {
		for _, snapshot := range knownSnapshots {
			if knownSnap, err := knownSnaps.Get(snapshot.Id); err == nil {
				if time.Since(knownSnap.GetTimestamp()) >= 30*time.Minute {
					knownSnap.Close()
				}
			}
		}
	}
}
