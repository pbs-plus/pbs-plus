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
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/cache"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

// Snapshot creates a new VSS snapshot for the specified drive
func Snapshot(driveLetter string) (*WinVSSSnapshot, error) {
	volName := filepath.VolumeName(fmt.Sprintf("%s:", driveLetter))
	timeStarted := time.Now()

	// Create snapshot with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	snapshotId, err := createSnapshotWithRetry(ctx, volName)
	if err != nil {
		return nil, fmt.Errorf("snapshot creation failed: %w", err)
	}

	// Validate snapshot
	sc, err := vss.Get(snapshotId)
	if err != nil {
		_ = vss.Remove(snapshotId)
		return nil, fmt.Errorf("snapshot validation failed: %w", err)
	}

	snapshot := &WinVSSSnapshot{
		SnapshotPath: sc.DeviceObject, // Use the DeviceObject path directly
		Id:           sc.ID,
		TimeStarted:  timeStarted,
	}

	// Initialize caches
	cache.ExcludedPathRegexes = cache.CompileExcludedPaths()
	cache.PartialFilePathRegexes = cache.CompilePartialFileList()

	return snapshot, nil
}

func stopService(name string) error {
	cmd := exec.Command("net", "stop", name)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func startService(name string) error {
	cmd := exec.Command("net", "start", name)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func reregisterVSSWriters() error {
	services := []string{
		"Winmgmt", // Windows Management Instrumentation
		"VSS",     // Volume Shadow Copy
		"swprv",   // Microsoft Software Shadow Copy Provider
	}

	for _, svc := range services {
		if err := stopService(svc); err != nil {
			return fmt.Errorf("failed to stop service %s: %w", svc, err)
		}
	}

	for i := len(services) - 1; i >= 0; i-- {
		if err := startService(services[i]); err != nil {
			return fmt.Errorf("failed to start service %s: %w", services[i], err)
		}
	}

	return nil
}

func createSnapshotWithRetry(ctx context.Context, volName string) (string, error) {
	const retryInterval = time.Second
	var lastError error

	for attempts := 0; attempts < 2; attempts++ {
		for {
			id, err := vss.Create(volName)
			if err == nil {
				return id, nil
			}

			lastError = err
			if !strings.Contains(err.Error(), "shadow copy operation is already in progress") {
				// If this is our first attempt and it's a VSS-related error,
				// try re-registering writers
				if attempts == 0 && (strings.Contains(err.Error(), "VSS") ||
					strings.Contains(err.Error(), "shadow copy")) {
					syslog.L.Error("VSS error detected, attempting to re-register writers...")
					if reregErr := reregisterVSSWriters(); reregErr != nil {
						syslog.L.Warnf("Warning: failed to re-register VSS writers: %v\n", reregErr)
					}
					// Break inner loop to start fresh after re-registration
					break
				}
				return "", fmt.Errorf("%w: %v", ErrSnapshotCreation, err)
			}

			select {
			case <-ctx.Done():
				return "", ErrSnapshotTimeout
			case <-time.After(retryInterval):
				continue
			}
		}
	}

	return "", fmt.Errorf("%w: %v", ErrSnapshotCreation, lastError)
}

func (s *WinVSSSnapshot) Close() {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return
	}
	if fileMap, ok := cache.SizeCache.Load(s.Id); ok {
		clear(fileMap.(map[string]int64))
		cache.SizeCache.Delete(s.Id)
	}
	_ = vss.Remove(s.Id)
}
