//go:build linux

package snapshots

import (
	"context"
	"errors"
)

type NtfsSnapshotHandler struct{}

func (w *NtfsSnapshotHandler) CreateSnapshot(ctx context.Context, jobId string, sourcePath string) (Snapshot, error) {
	return Snapshot{}, errors.New("unsupported")
}

func (w *NtfsSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	return nil
}

func (w *NtfsSnapshotHandler) IsSupported(sourcePath string) bool {
	return false
}
