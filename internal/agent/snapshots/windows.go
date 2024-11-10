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
)

func getVSSFolder() (string, error) {
	tmpDir := os.TempDir()

	configBasePath := filepath.Join(tmpDir, "pbs-plus")

	err := os.MkdirAll(configBasePath, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("getVSSFolder: failed to create directory \"%s\" -> %w", configBasePath, err)
	}

	return configBasePath, nil
}

func Snapshot(driveLetter string) (*WinVSSSnapshot, error) {
	knownSnaps := &KnownSnapshots{
		registry: "KnownSnaps",
	}

	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))

	vssFolder, err := getVSSFolder()
	if err != nil {
		return nil, fmt.Errorf("Snapshot: error getting app data folder -> %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, "VSS", driveLetter)
	err = vss.CreateLink(snapshotPath, volName)
	if err != nil {
		if strings.Contains(err.Error(), "shadow copy operation is already in progress") {
			for {
				if _, err := vss.Get(snapshotPath); err == nil {
					break
				}
			}
		} else {
			if strings.Contains(err.Error(), "already exists") {
				if _, err := vss.Get(snapshotPath); err == nil {
					if knownSnap, err := knownSnaps.Get(snapshotPath); err == nil {
						if knownSnap.SnapshotPath == snapshotPath && time.Since(knownSnap.LastAccessed) < (15*time.Minute) {
							return knownSnap, nil
						} else if time.Since(knownSnap.LastAccessed) >= (15 * time.Minute) {
							knownSnap.Close()
						}
					}
					_ = vss.Remove(snapshotPath)
				}

				_ = os.Remove(snapshotPath)

				if err := vss.CreateLink(snapshotPath, volName); err != nil {
					return nil, fmt.Errorf("Snapshot: error creating snapshot (%s to %s) -> %w", volName, snapshotPath, err)
				}
			} else {
				return nil, fmt.Errorf("Snapshot: error creating snapshot (%s to %s) -> %w", volName, snapshotPath, err)
			}
		}
	}

	sc, err := vss.Get(snapshotPath)
	if err != nil {
		_ = vss.Remove(snapshotPath)
		return nil, fmt.Errorf("Snapshot: error getting snapshot details -> %w", err)
	}

	newSnapshot := WinVSSSnapshot{
		SnapshotPath: snapshotPath,
		LastAccessed: time.Now(),
		Id:           sc.ID,
	}

	knownSnaps.Save(&newSnapshot)

	return &newSnapshot, nil
}

func (instance *WinVSSSnapshot) UpdateTimestamp() {
	knownSnaps := &KnownSnapshots{
		registry: "KnownSnaps",
	}

	instance.LastAccessed = time.Now()

	knownSnaps.Save(instance)
}

func (instance *WinVSSSnapshot) Close() {
	_ = vss.Remove(instance.Id)

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
