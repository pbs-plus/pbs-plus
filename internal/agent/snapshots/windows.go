//go:build windows
// +build windows

package snapshots

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mxk/go-vss"
)

type WinVSSSnapshot struct {
	SnapshotPath   string
	Id             string
	Used           bool
	LastUsedUpdate time.Time
}

var CurrentSnapshots []*WinVSSSnapshot

func symlinkSnapshot(symlinkPath string, id string, deviceObjectPath string) (string, error) {
	snapshotSymLinkFolder := symlinkPath + "\\" + id + "\\"

	snapshotSymLinkFolder = filepath.Clean(snapshotSymLinkFolder)
	os.RemoveAll(snapshotSymLinkFolder)
	if err := os.MkdirAll(snapshotSymLinkFolder, 0700); err != nil {
		return "", fmt.Errorf("symlinkSnapshot: failed to create snapshot symlink folder for snapshot: %s -> %w", id, err)
	}

	os.Remove(snapshotSymLinkFolder)

	fmt.Println("Symlink from: ", deviceObjectPath, " to: ", snapshotSymLinkFolder)

	if err := os.Symlink(deviceObjectPath, snapshotSymLinkFolder); err != nil {
		return "", fmt.Errorf("symlinkSnapshot: failed to create symlink from: %s to: %s -> %w", deviceObjectPath, snapshotSymLinkFolder, err)
	}

	return snapshotSymLinkFolder, nil
}

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

func Snapshot(path string) (*WinVSSSnapshot, error) {
	volName := filepath.VolumeName(fmt.Sprintf("%s:\\", path))
	volName += "\\"

	appDataFolder, err := getAppDataFolder()
	if err != nil {
		return nil, fmt.Errorf("Snapshot: error getting app data folder -> %w", err)
	}

	snapshotPath := filepath.Join(appDataFolder, "VSS", path)

	err = vss.CreateLink(snapshotPath, volName)
	if err != nil {
		return nil, fmt.Errorf("Snapshot: error creating snapshot -> %w", err)
	}

	sc, err := vss.Get(snapshotPath)
	if err != nil {
		_ = vss.Remove(snapshotPath)
		return nil, fmt.Errorf("Snapshot: error getting snapshot details -> %w", err)
	}

	newSnapshot := WinVSSSnapshot{
		SnapshotPath:   snapshotPath,
		Used:           false,
		LastUsedUpdate: time.Now(),
		Id:             sc.ID,
	}

	go newSnapshot.closeOnStale()

	if CurrentSnapshots == nil {
		CurrentSnapshots = make([]*WinVSSSnapshot, 0)
		CurrentSnapshots = append(CurrentSnapshots, &newSnapshot)
	}

	return &newSnapshot, nil
}

func (instance *WinVSSSnapshot) closeOnStale() {
	ctx, cancel := context.WithCancel(context.Background())

	for {
		select {
		case <-ctx.Done():
			if instance != nil {
				_ = instance.Close()
			}
		default:
			time.Sleep(time.Minute)

			if instance == nil {
				cancel()
			}

			if !instance.Used {
				if time.Since(instance.LastUsedUpdate) > 2*time.Minute {
					cancel()
				}
			}
		}
	}
}

func (instance *WinVSSSnapshot) Close() error {
	err := vss.Remove(instance.Id)
	if err != nil {
		return fmt.Errorf("Close: error deleting snapshot %s -> %w", instance.SnapshotPath, err)
	}

	if CurrentSnapshots != nil {
		newCurrentSnapshots := []*WinVSSSnapshot{}
		for _, snapshot := range CurrentSnapshots {
			if snapshot.Id != instance.Id {
				newCurrentSnapshots = append(newCurrentSnapshots, snapshot)
			}
		}

		CurrentSnapshots = newCurrentSnapshots
	}

	return nil
}

func CloseAllSnapshots() {
	if CurrentSnapshots != nil {
		for _, snapshot := range CurrentSnapshots {
			_ = snapshot.Close()
		}
	}
}
