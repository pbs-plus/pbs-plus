//go:build windows

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

// VSSProvider creates snapshots using Windows Volume Shadow Copy Service.
type VSSProvider struct{}

func (p *VSSProvider) Name() string { return "vss" }

func (p *VSSProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	if sourcePath == "" {
		return Snapshot{}, errors.New("empty source path")
	}

	driveLetter := sourcePath[:1]
	volName := fmt.Sprintf("%s:", driveLetter)

	vssFolder, err := vssFolderPath()
	if err != nil {
		return Snapshot{}, fmt.Errorf("vss: get folder: %w", err)
	}

	snapshotPath := filepath.Join(vssFolder, jobID)
	timeStarted := time.Now()

	// Cleanup existing snapshot.
	cleanupExistingVSS(snapshotPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := createVSSWithRetry(ctx, snapshotPath, volName); err != nil {
		cleanupExistingVSS(snapshotPath)
		return Snapshot{}, fmt.Errorf("vss: snapshot creation failed: %w", err)
	}

	if _, err := vss.Get(snapshotPath); err != nil {
		cleanupExistingVSS(snapshotPath)
		return Snapshot{}, fmt.Errorf("vss: validation failed: %w", err)
	}

	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     p,
	}, nil
}

func (p *VSSProvider) DeleteSnapshot(snapshot Snapshot) error {
	if err := vss.Remove(snapshot.Path); err != nil {
		return fmt.Errorf("vss: remove failed: %w", err)
	}
	if vssFolder, err := vssFolderPath(); err == nil {
		if strings.HasPrefix(snapshot.Path, vssFolder) {
			_ = os.Remove(snapshot.Path)
		}
	}
	return nil
}

func (p *VSSProvider) IsSupported(sourcePath string) bool { return true }

func vssFolderPath() (string, error) {
	tmpDir := os.TempDir()
	base := filepath.Join(tmpDir, "pbs-plus-vss")
	if err := os.MkdirAll(base, 0750); err != nil {
		return "", fmt.Errorf("vss: create dir %q: %w", base, err)
	}
	return base, nil
}

func cleanupExistingVSS(path string) {
	if sc, err := vss.Get(path); err == nil {
		_ = vss.Remove(sc.ID)
	}
	if vssFolder, err := vssFolderPath(); err == nil {
		if strings.HasPrefix(path, vssFolder) {
			_ = os.Remove(path)
		}
	}
}

func createVSSWithRetry(ctx context.Context, snapshotPath, volName string) error {
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
						syslog.L.Error(reregErr).WithMessage("failed to re-register VSS writers").Write()
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

func reregisterVSSWriters() error {
	services := []string{"Winmgmt", "VSS", "swprv"}
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
