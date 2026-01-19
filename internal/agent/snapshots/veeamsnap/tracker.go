//go:build linux

package veeamsnap

import (
	"fmt"
	"unsafe"
)

// Tracker manages CBT tracking for a device
type Tracker struct {
	control *Control
}

// NewTracker creates a new tracker instance
func NewTracker(control *Control) *Tracker {
	return &Tracker{control: control}
}

// AddTracking adds a device to CBT tracking
func (t *Tracker) AddTracking(major, minor int32) error {
	devID := DevID{
		Major: major,
		Minor: minor,
	}
	return ioctl(t.control.fd.Fd(), IOCTL_TRACKING_ADD, uintptr(unsafe.Pointer(&devID)))
}

// RemoveTracking removes a device from CBT tracking
func (t *Tracker) RemoveTracking(major, minor int32) error {
	devID := DevID{
		Major: major,
		Minor: minor,
	}
	return ioctl(t.control.fd.Fd(), IOCTL_TRACKING_REMOVE, uintptr(unsafe.Pointer(&devID)))
}

// CollectTracking collects CBT info for all tracked devices
func (t *Tracker) CollectTracking(maxCount uint32) ([]CbtInfo, error) {
	if maxCount == 0 {
		maxCount = MaxTrackingDeviceCount
	}

	cbtInfos := make([]CbtInfo, maxCount)

	req := TrackingCollect{
		Count:   maxCount,
		CbtInfo: uint64(uintptr(unsafe.Pointer(&cbtInfos[0]))),
	}

	if err := ioctl(t.control.fd.Fd(), IOCTL_TRACKING_COLLECT, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to collect tracking: %w", err)
	}

	return cbtInfos[:req.Count], nil
}

// SetTrackingBlockSize sets the CBT block size
func (t *Tracker) SetTrackingBlockSize(blockSize uint32) error {
	return ioctl(t.control.fd.Fd(), IOCTL_TRACKING_BLOCK_SIZE, uintptr(unsafe.Pointer(&blockSize)))
}

// ReadCbtBitmap reads the CBT bitmap for a device
func (t *Tracker) ReadCbtBitmap(major, minor int32, offset, length uint32) ([]byte, error) {
	buffer := make([]byte, length)

	req := TrackingReadCbtBitmap{
		DevID: DevID{
			Major: major,
			Minor: minor,
		},
		Offset: offset,
		Length: length,
		Buff:   uint64(uintptr(unsafe.Pointer(&buffer[0]))),
	}

	if err := ioctl(t.control.fd.Fd(), IOCTL_TRACKING_READ_CBT_BITMAP, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to read CBT bitmap: %w", err)
	}

	return buffer, nil
}

// MarkDirtyBlocks marks blocks as dirty in the CBT
func (t *Tracker) MarkDirtyBlocks(imageMajor, imageMinor int32, ranges []BlockRange) error {
	if len(ranges) == 0 {
		return nil
	}

	req := TrackingMarkDirtyBlocks{
		ImageDevID: DevID{
			Major: imageMajor,
			Minor: imageMinor,
		},
		Count:       uint32(len(ranges)),
		DirtyBlocks: uint64(uintptr(unsafe.Pointer(&ranges[0]))),
	}

	return ioctl(t.control.fd.Fd(), IOCTL_TRACKING_MARK_DIRTY_BLOCKS, uintptr(unsafe.Pointer(&req)))
}
