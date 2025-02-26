//go:build windows
// +build windows

package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mxk/go-vss"
)

var (
	ErrSnapshotTimeout  = errors.New("timeout waiting for in-progress snapshot")
	ErrSnapshotCreation = errors.New("failed to create snapshot")
	ErrInvalidSnapshot  = errors.New("invalid snapshot")
)

type WinVSSSnapshot struct {
	SnapshotPath string    `json:"path"`
	Id           string    `json:"vss_id"`
	TimeStarted  time.Time `json:"time_started"`
	DriveLetter  string
	closed       atomic.Bool
}

func getVSSFolder() (string, error) {
	tmpDir := os.TempDir()
	configBasePath := filepath.Join(tmpDir, "pbs-plus-vss")
	if err := os.MkdirAll(configBasePath, 0750); err != nil {
		return "", fmt.Errorf("failed to create VSS directory %q: %w", configBasePath, err)
	}
	return configBasePath, nil
}

// Snapshot creates a new VSS snapshot for the specified drive
func Snapshot(jobId string, driveLetter string) (*WinVSSSnapshot, error) {
	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))
	vssFolder, err := getVSSFolder()
	if err != nil {
		return nil, fmt.Errorf("error getting VSS folder: %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, driveLetter)
	timeStarted := time.Now()

	cleanupExistingSnapshot(snapshotPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := createSnapshotWithRetry(ctx, snapshotPath, volName); err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return nil, fmt.Errorf("snapshot creation failed: %w", err)
	}

	sc, err := vss.Get(snapshotPath)
	if err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return nil, fmt.Errorf("snapshot validation failed: %w", err)
	}

	snapshot := &WinVSSSnapshot{
		SnapshotPath: snapshotPath,
		Id:           sc.ID,
		TimeStarted:  timeStarted,
		DriveLetter:  driveLetter,
	}

	return snapshot, nil
}

// reregisterVSSWriters attempts to restart VSS services when needed
func reregisterVSSWriters() error {
	services := []string{
		"Winmgmt", // Windows Management Instrumentation
		"VSS",     // Volume Shadow Copy
		"swprv",   // Microsoft Software Shadow Copy Provider
	}

	for _, svc := range services {
		if err := exec.Command("net", "stop", svc).Run(); err != nil {
			return fmt.Errorf("failed to stop service %s: %w", svc, err)
		}
	}

	for i := len(services) - 1; i >= 0; i-- {
		if err := exec.Command("net", "start", services[i]).Run(); err != nil {
			return fmt.Errorf("failed to start service %s: %w", services[i], err)
		}
	}

	return nil
}

func createSnapshotWithRetry(ctx context.Context, snapshotPath, volName string) error {
	const retryInterval = time.Second
	var lastError error

	for attempts := 0; attempts < 2; attempts++ {
		for {
			if err := vss.CreateLink(snapshotPath, volName); err == nil {
				return nil
			} else if !strings.Contains(err.Error(), "shadow copy operation is already in progress") {
				lastError = err
				// If this is our first attempt and it's a VSS-related error,
				// try re-registering writers
				if attempts == 0 && (strings.Contains(err.Error(), "VSS") ||
					strings.Contains(err.Error(), "shadow copy")) {
					fmt.Println("VSS error detected, attempting to re-register writers...")
					if reregErr := reregisterVSSWriters(); reregErr != nil {
						fmt.Printf("Warning: failed to re-register VSS writers: %v\n", reregErr)
					}
					// Break inner loop to start fresh after re-registration
					break
				}
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

	return fmt.Errorf("%w: %v", ErrSnapshotCreation, lastError)
}

func cleanupExistingSnapshot(path string) {
	if sc, err := vss.Get(path); err == nil {
		_ = vss.Remove(sc.ID)
	}

	_ = os.Remove(path)
}

func (s *WinVSSSnapshot) Close() {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return
	}

	_ = vss.Remove(s.Id)
	_ = os.Remove(s.SnapshotPath)
}
