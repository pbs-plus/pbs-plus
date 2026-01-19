//go:build linux

package veeamsnap

import (
	"fmt"
	"unsafe"
)

// Snapshot represents a veeamsnap snapshot
type Snapshot struct {
	ID      uint64
	control *Control
}

// CreateSnapshot creates a new snapshot
func (c *Control) CreateSnapshot(snapshotID uint64, devIDs []DevID) (*Snapshot, error) {
	if len(devIDs) == 0 {
		return nil, fmt.Errorf("device ID list cannot be empty")
	}

	req := SnapshotCreate{
		SnapshotID: snapshotID,
		Count:      uint32(len(devIDs)),
		DevIDSet:   uint64(uintptr(unsafe.Pointer(&devIDs[0]))),
	}

	if err := ioctl(c.fd.Fd(), IOCTL_SNAPSHOT_CREATE, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	return &Snapshot{
		ID:      snapshotID,
		control: c,
	}, nil
}

// OpenSnapshot opens an existing snapshot
func (c *Control) OpenSnapshot(snapshotID uint64) *Snapshot {
	return &Snapshot{
		ID:      snapshotID,
		control: c,
	}
}

// Destroy destroys the snapshot
func (s *Snapshot) Destroy() error {
	if err := ioctl(s.control.fd.Fd(), IOCTL_SNAPSHOT_DESTROY, uintptr(unsafe.Pointer(&s.ID))); err != nil {
		return fmt.Errorf("failed to destroy snapshot: %w", err)
	}
	return nil
}

// GetErrno gets the error status for a device in the snapshot
func (s *Snapshot) GetErrno(major, minor int32) (int32, error) {
	req := SnapshotErrno{
		DevID: DevID{
			Major: major,
			Minor: minor,
		},
	}

	if err := ioctl(s.control.fd.Fd(), IOCTL_SNAPSHOT_ERRNO, uintptr(unsafe.Pointer(&req))); err != nil {
		return 0, fmt.Errorf("failed to get snapshot errno: %w", err)
	}

	return req.ErrCode, nil
}

// CollectSnapshotImages collects snapshot image information
func (c *Control) CollectSnapshotImages(maxCount int32) ([]ImageInfo, error) {
	if maxCount == 0 {
		maxCount = MaxTrackingDeviceCount
	}

	images := make([]ImageInfo, maxCount)

	req := CollectSnapshotImages{
		Count:     maxCount,
		ImageInfo: uint64(uintptr(unsafe.Pointer(&images[0]))),
	}

	if err := ioctl(c.fd.Fd(), IOCTL_COLLECT_SNAPSHOT_IMAGES, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to collect snapshot images: %w", err)
	}

	return images[:req.Count], nil
}
