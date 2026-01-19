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
	"time"

	"github.com/mxk/go-vss"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type NtfsSnapshotHandler struct{}

func (w *NtfsSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	if sourcePath == "" {
		return Snapshot{}, errors.New("empty source path")
	}

	driveLetter := sourcePath[:1]
	volName := fmt.Sprintf("%s:", driveLetter)

	vssFolder, err := getVSSFolder()
	if err != nil {
		return Snapshot{}, fmt.Errorf("error getting VSS folder: %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, jobId)
	timeStarted := time.Now()

	cleanupExistingSnapshot(snapshotPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := createSnapshotWithRetry(ctx, snapshotPath, volName); err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return Snapshot{}, fmt.Errorf("snapshot creation failed: %w", err)
	}

	_, err = vss.Get(snapshotPath)
	if err != nil {
		cleanupExistingSnapshot(snapshotPath)
		return Snapshot{}, fmt.Errorf("snapshot validation failed: %w", err)
	}

	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     w,
	}, nil
}

func (w *NtfsSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	if err := vss.Remove(snapshot.Path); err != nil {
		return fmt.Errorf("failed to delete VSS snapshot: %w", err)
	}

	if vssFolder, err := getVSSFolder(); err == nil {
		if strings.HasPrefix(snapshot.Path, vssFolder) {
			_ = os.Remove(snapshot.Path)
		}
	}

	return nil
}

func (w *NtfsSnapshotHandler) IsSupported(sourcePath string) bool {
	return true
}

func getVSSFolder() (string, error) {
	tmpDir := os.TempDir()
	configBasePath := filepath.Join(tmpDir, "pbs-plus-vss")
	if err := os.MkdirAll(configBasePath, 0750); err != nil {
		return "", fmt.Errorf("failed to create VSS directory %q: %w", configBasePath, err)
	}
	return configBasePath, nil
}

func reregisterVSSWriters() error {
	services := []string{
		"Winmgmt",
		"VSS",
		"swprv",
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
				if attempts == 0 && (strings.Contains(err.Error(), "VSS") ||
					strings.Contains(err.Error(), "shadow copy")) {
					syslog.L.Error(err).WithMessage("vss error detected, attempting to re-register").Write()
					if reregErr := reregisterVSSWriters(); reregErr != nil {
						syslog.L.Error(reregErr).WithMessage("failed to re-register VSS writers")
					}
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

	if vssFolder, err := getVSSFolder(); err == nil {
		if strings.HasPrefix(path, vssFolder) {
			_ = os.Remove(path)
		}
	}
}
