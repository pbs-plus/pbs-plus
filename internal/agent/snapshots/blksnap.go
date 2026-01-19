//go:build !linux

package snapshots

import (
	"errors"
)

type KernelSnapshotHandler struct {
}

func (b *KernelSnapshotHandler) CreateSnapshot(jobId string, sourcePath string) (Snapshot, error) {
	return Snapshot{}, errors.New("unsupported")
}

func (b *KernelSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	return nil
}

func (b *KernelSnapshotHandler) IsSupported(sourcePath string) bool {
	return false
}

func NewKernelSnapshotHandler() *KernelSnapshotHandler {
	return nil
}
