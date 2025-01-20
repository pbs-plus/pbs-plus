//go:build windows

package snapshots

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
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
	internal     *VSSSnapshot // Internal COM-based snapshot
}

// Snapshot creates a new VSS snapshot for the specified drive
func Snapshot(driveLetter string) (*WinVSSSnapshot, error) {
	volName := fmt.Sprintf("%s:", driveLetter)
	timeStarted := time.Now()

	// Initialize VSS
	if err := InitializeVSS(); err != nil {
		return nil, fmt.Errorf("failed to initialize VSS: %w", err)
	}

	// Create snapshot with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create internal COM-based snapshot with retry
	internalSnapshot, err := createSnapshotWithRetry(ctx, volName)
	if err != nil {
		return nil, err
	}

	// Get the device path for the snapshot
	path, err := internalSnapshot.GetSnapshotPath()
	if err != nil {
		internalSnapshot.Close()
		return nil, fmt.Errorf("failed to get snapshot path: %w", err)
	}

	snapshot := &WinVSSSnapshot{
		SnapshotPath: path,
		Id:           internalSnapshot.SnapshotID.String(),
		TimeStarted:  timeStarted,
		internal:     internalSnapshot,
	}

	return snapshot, nil
}

// createSnapshotWithRetry attempts to create a snapshot with retries on conflicts
func createSnapshotWithRetry(ctx context.Context, volName string) (*VSSSnapshot, error) {
	const retryInterval = time.Second

	for {
		// Try to create snapshot
		internalSnapshot, err := CreateSnapshot(volName)
		if err == nil {
			return internalSnapshot, nil
		}

		// If error is not "shadow copy operation is already in progress", return error
		if !isSnapshotInProgressError(err) {
			return nil, fmt.Errorf("%w: %v", ErrSnapshotCreation, err)
		}

		// Wait and retry
		select {
		case <-ctx.Done():
			return nil, ErrSnapshotTimeout
		case <-time.After(retryInterval):
			continue
		}
	}
}

// isSnapshotInProgressError checks if the error indicates a snapshot is in progress
func isSnapshotInProgressError(err error) bool {
	return err != nil && err.Error() == "shadow copy operation is already in progress"
}

// Close cleans up the snapshot and associated resources
func (s *WinVSSSnapshot) Close() {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return
	}

	if s.internal != nil {
		s.internal.Close()
		s.internal = nil
	}
}
