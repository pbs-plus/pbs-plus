//go:build !linux

package snapshots

import (
	"errors"
)

type BlksnapSnapshotHandler struct {
}

func (b *BlksnapSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	return Snapshot{}, errors.New("unsupported")
}

func (b *BlksnapSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	return nil
}

func (b *BlksnapSnapshotHandler) IsSupported(sourcePath string) bool {
	return false
}
