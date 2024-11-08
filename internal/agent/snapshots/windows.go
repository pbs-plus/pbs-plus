//go:build windows
// +build windows

package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mxk/go-vss"
)

type WinVSSSnapshot struct {
	SnapshotPath string
	Id           string
	LastAccessed time.Time
}

var KnownSnapshots []*WinVSSSnapshot
var SnapLock sync.Mutex

func getAppDataFolder() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("getAppDataFolder: failed to get user config directory -> %w", err)
	}

	configBasePath := filepath.Join(configDir, "proxmox-agent")

	err = os.MkdirAll(configBasePath, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("getAppDataFolder: failed to create directory \"%s\" -> %w", configBasePath, err)
	}

	return configBasePath, nil
}

func Snapshot(driveLetter string) (*WinVSSSnapshot, error) {
	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))

	appDataFolder, err := getAppDataFolder()
	if err != nil {
		return nil, fmt.Errorf("Snapshot: error getting app data folder -> %w", err)
	}

	snapshotPath := filepath.Join(appDataFolder, "VSS", driveLetter)
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
					if KnownSnapshots != nil {
						for _, knownSnap := range KnownSnapshots {
							if knownSnap.SnapshotPath == snapshotPath && time.Since(knownSnap.LastAccessed) < (15*time.Minute) {
								return knownSnap, nil
							} else if time.Since(knownSnap.LastAccessed) >= (15 * time.Minute) {
								knownSnap.Close()
								if knownSnap.SnapshotPath == snapshotPath {
									break
								}
							}
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

	SnapLock.Lock()
	if KnownSnapshots == nil {
		KnownSnapshots = make([]*WinVSSSnapshot, 0)
	}

	KnownSnapshots = append(KnownSnapshots, &newSnapshot)
	SnapLock.Unlock()

	return &newSnapshot, nil
}

func (instance *WinVSSSnapshot) Close() {
	SnapLock.Lock()
	defer SnapLock.Unlock()

	_ = vss.Remove(instance.Id)

	if KnownSnapshots != nil {
		newKnownSnapshots := []*WinVSSSnapshot{}
		for _, snapshot := range KnownSnapshots {
			if snapshot.Id != instance.Id {
				newKnownSnapshots = append(newKnownSnapshots, snapshot)
			}
		}

		KnownSnapshots = newKnownSnapshots
	}

	return
}

func CloseAllSnapshots() {
	if KnownSnapshots != nil {
		for _, snapshot := range KnownSnapshots {
			snapshot.Close()
		}
	}
}
