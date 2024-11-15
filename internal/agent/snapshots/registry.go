//go:build windows
// +build windows

package snapshots

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/windows/registry"
)

type WinVSSSnapshot struct {
	SnapshotPath string    `json:"path"`
	Id           string    `json:"vss_id"`
	LastAccessed time.Time `json:"last_accessed"`
	TimeStarted  time.Time `json:"time_started"`
}

type KnownSnapshots struct {
	registry string

	sync.Mutex
}

func (k *KnownSnapshots) Get(path string) (*WinVSSSnapshot, error) {
	k.Lock()
	defer k.Unlock()

	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus`, registry.ALL_ACCESS)
	if err != nil {
		return nil, fmt.Errorf("Get: error getting registry key -> %w", err)
	}
	defer key.Close()

	if knownSnapshotsRaw, _, err := key.GetStringValue(k.registry); err == nil { // Unmarshal JSON into the configuration struct
		knownSnapshots := []WinVSSSnapshot{}
		if err := json.Unmarshal([]byte(knownSnapshotsRaw), &knownSnapshots); err != nil {
			_ = key.DeleteValue(k.registry)
			return nil, fmt.Errorf("Get: error getting known snapshots registry key -> %w", err)
		}

		for _, knownSnap := range knownSnapshots {
			if knownSnap.SnapshotPath == path {
				return &knownSnap, nil
			}
		}
	}

	return nil, fmt.Errorf("Get: error getting known snapshots registry key -> %w", err)
}

func (k *KnownSnapshots) GetAll() ([]WinVSSSnapshot, error) {
	k.Lock()
	defer k.Unlock()

	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus`, registry.ALL_ACCESS)
	if err != nil {
		return nil, fmt.Errorf("GetAll: error getting registry key -> %w", err)
	}
	defer key.Close()

	if knownSnapshotsRaw, _, err := key.GetStringValue(k.registry); err == nil { // Unmarshal JSON into the configuration struct
		knownSnapshots := []WinVSSSnapshot{}
		if err := json.Unmarshal([]byte(knownSnapshotsRaw), &knownSnapshots); err != nil {
			_ = key.DeleteValue(k.registry)
			return nil, fmt.Errorf("GetAll: error getting known snapshots registry key -> %w", err)
		}

		return knownSnapshots, nil
	}

	return nil, fmt.Errorf("GetAll: error getting known snapshots registry key -> %w", err)
}

func (k *KnownSnapshots) Delete(path string) error {
	k.Lock()
	defer k.Unlock()

	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus`, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("Delete: error getting registry key -> %w", err)
	}
	defer key.Close()

	if knownSnapshotsRaw, _, err := key.GetStringValue(k.registry); err == nil { // Unmarshal JSON into the configuration struct
		knownSnapshots := []WinVSSSnapshot{}
		if err := json.Unmarshal([]byte(knownSnapshotsRaw), &knownSnapshots); err != nil {
			_ = key.DeleteValue(k.registry)
			return nil
		}

		newKnownSnapshots := []WinVSSSnapshot{}
		for _, knownSnap := range knownSnapshots {
			if knownSnap.SnapshotPath != path {
				newKnownSnapshots = append(newKnownSnapshots, knownSnap)
			}
		}

		if newKnownSnapshotsRaw, err := json.Marshal(newKnownSnapshots); err == nil {
			err = key.SetStringValue(k.registry, string(newKnownSnapshotsRaw))
			if err != nil {
				return fmt.Errorf("Delete: failed to set new array json -> %w", err)
			}
			return nil
		} else {
			return fmt.Errorf("Delete: failed to marshal json -> %w", err)
		}
	}

	return fmt.Errorf("Delete: error getting known snapshots registry key -> %w", err)
}

func (k *KnownSnapshots) Save(snap *WinVSSSnapshot) error {
	k.Lock()
	defer k.Unlock()

	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus`, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("Save: error getting registry key -> %w", err)
	}
	defer key.Close()

	knownSnapshots := []WinVSSSnapshot{*snap}

	// if reg exists
	if knownSnapshotsRaw, _, err := key.GetStringValue(k.registry); err == nil { // Unmarshal JSON into the configuration struct
		var regSnapshots []WinVSSSnapshot
		if err := json.Unmarshal([]byte(knownSnapshotsRaw), &regSnapshots); err == nil {
			for _, regSnap := range regSnapshots {
				if regSnap.SnapshotPath != snap.SnapshotPath {
					knownSnapshots = append(knownSnapshots, regSnap)
				}
			}
		}
	}

	if newKnownSnapshotsRaw, err := json.Marshal(knownSnapshots); err == nil {
		err = key.SetStringValue(k.registry, string(newKnownSnapshotsRaw))
		if err != nil {
			return fmt.Errorf("Save: failed to set new array json -> %w", err)
		}
		return nil

	} else {
		return fmt.Errorf("Save: failed to marshal json -> %w", err)
	}
}
