//go:build linux

package veeamsnap

import (
	"fmt"
	"unsafe"
)

// Snapstore manages snapshot storage
type Snapstore struct {
	ID      UUID
	control *Control
}

// CreateSnapstore creates a new snapstore
func (c *Control) CreateSnapstore(id UUID, snapstoreDevID DevID, devIDs []DevID) (*Snapstore, error) {
	if len(devIDs) == 0 {
		return nil, fmt.Errorf("device ID list cannot be empty")
	}

	req := SnapstoreCreate{
		ID:             id,
		SnapstoreDevID: snapstoreDevID,
		Count:          uint32(len(devIDs)),
		DevIDSet:       uint64(uintptr(unsafe.Pointer(&devIDs[0]))),
	}

	if err := ioctl(c.fd.Fd(), IOCTL_SNAPSTORE_CREATE, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to create snapstore: %w", err)
	}

	return &Snapstore{
		ID:      id,
		control: c,
	}, nil
}

// OpenSnapstore opens an existing snapstore
func (c *Control) OpenSnapstore(id UUID) *Snapstore {
	return &Snapstore{
		ID:      id,
		control: c,
	}
}

// AddFile adds file ranges to the snapstore
func (s *Snapstore) AddFile(ranges []Range) error {
	if len(ranges) == 0 {
		return nil
	}

	req := SnapstoreFileAdd{
		ID:         s.ID,
		RangeCount: uint32(len(ranges)),
		Ranges:     uint64(uintptr(unsafe.Pointer(&ranges[0]))),
	}

	return ioctl(s.control.fd.Fd(), IOCTL_SNAPSTORE_FILE, uintptr(unsafe.Pointer(&req)))
}

// AddFileMultidev adds file ranges to the snapstore for multidev support
func (s *Snapstore) AddFileMultidev(devID DevID, ranges []Range) error {
	if len(ranges) == 0 {
		return nil
	}

	req := SnapstoreFileAddMultidev{
		ID:         s.ID,
		DevID:      devID,
		RangeCount: uint32(len(ranges)),
		Ranges:     uint64(uintptr(unsafe.Pointer(&ranges[0]))),
	}

	return ioctl(s.control.fd.Fd(), IOCTL_SNAPSTORE_FILE_MULTIDEV, uintptr(unsafe.Pointer(&req)))
}

// SetMemoryLimit sets the memory limit for the snapstore
func (s *Snapstore) SetMemoryLimit(size uint64) error {
	req := SnapstoreMemoryLimit{
		ID:   s.ID,
		Size: size,
	}

	return ioctl(s.control.fd.Fd(), IOCTL_SNAPSTORE_MEMORY, uintptr(unsafe.Pointer(&req)))
}

// Cleanup cleans up the snapstore
func (s *Snapstore) Cleanup() (uint64, error) {
	req := SnapstoreCleanup{
		ID: s.ID,
	}

	if err := ioctl(s.control.fd.Fd(), IOCTL_SNAPSTORE_CLEANUP, uintptr(unsafe.Pointer(&req))); err != nil {
		return 0, fmt.Errorf("failed to cleanup snapstore: %w", err)
	}

	return req.FilledBytes, nil
}
