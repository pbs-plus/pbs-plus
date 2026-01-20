//go:build linux

package blksnap

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

type Control struct {
	fd *os.File
}

func OpenControl() (*Control, error) {
	fd, err := os.OpenFile(BlksnapControlDevice, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open control device: %w", err)
	}

	return &Control{fd: fd}, nil
}

func (c *Control) Close() error {
	if c.fd != nil {
		return c.fd.Close()
	}
	return nil
}

func (c *Control) GetVersion() (*Version, error) {
	var version Version
	if err := ioctl(c.fd.Fd(), IOCTL_BLKSNAP_VERSION, uintptr(unsafe.Pointer(&version))); err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	return &version, nil
}

func (c *Control) GetMod() (*Mod, error) {
	var m Mod
	if err := ioctl(c.fd.Fd(), IOCTL_BLKSNAP_MOD, uintptr(unsafe.Pointer(&m))); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *Control) SetLog(path string, level int32, tzMinutes int32) error {
	p, err := unix.BytePtrFromString(path)
	if err != nil {
		return err
	}
	req := SetLog{
		TzMinuteswest: tzMinutes,
		Level:         level,
		FilepathSize:  uint32(len(path)),
		Filepath:      uint64(uintptr(unsafe.Pointer(p))),
	}
	return ioctl(c.fd.Fd(), IOCTL_BLKSNAP_SETLOG, uintptr(unsafe.Pointer(&req)))
}

type Snapshot struct {
	ID      UUID
	control *Control
}

func (c *Control) CreateSnapshot(filename string, limitSectors uint64) (*Snapshot, error) {
	p, err := unix.BytePtrFromString(filename)
	if err != nil {
		return nil, err
	}
	req := snapshotCreate{
		DiffStorageLimitSect: limitSectors,
		DiffStorageFilename:  uint64(uintptr(unsafe.Pointer(p))),
	}
	if err := ioctl(c.fd.Fd(), IOCTL_BLKSNAP_SNAPSHOT_CREATE, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, err
	}
	return &Snapshot{ID: req.ID, control: c}, nil
}

func (c *Control) OpenSnapshot(id UUID) *Snapshot {
	return &Snapshot{
		ID:      id,
		control: c,
	}
}

func (s *Snapshot) Take() error {
	if err := ioctl(s.control.fd.Fd(), IOCTL_BLKSNAP_SNAPSHOT_TAKE, uintptr(unsafe.Pointer(&s.ID))); err != nil {
		return fmt.Errorf("failed to take snapshot: %w", err)
	}
	return nil
}

func (s *Snapshot) Destroy() error {
	if err := ioctl(s.control.fd.Fd(), IOCTL_BLKSNAP_SNAPSHOT_DESTROY, uintptr(unsafe.Pointer(&s.ID))); err != nil {
		return fmt.Errorf("failed to destroy snapshot: %w", err)
	}
	return nil
}

func (s *Snapshot) AppendStorage(devPath string, ranges []Range) error {
	p, err := unix.BytePtrFromString(devPath)
	if err != nil {
		return err
	}
	req := snapshotAppendStorage{
		ID:      s.ID,
		Devpath: uint64(uintptr(unsafe.Pointer(p))),
		Count:   uint32(len(ranges)),
		Ranges:  uint64(uintptr(unsafe.Pointer(&ranges[0]))),
	}
	return ioctl(s.control.fd.Fd(), IOCTL_BLKSNAP_SNAPSHOT_APPEND_STORAGE, uintptr(unsafe.Pointer(&req)))
}

func (c *Control) CollectSnapshots(maxCount uint32) ([]UUID, error) {
	if maxCount == 0 {
		maxCount = 256 // reasonable default
	}

	ids := make([]UUID, maxCount)

	req := snapshotCollect{
		Count: maxCount,
		IDs:   uint64(uintptr(unsafe.Pointer(&ids[0]))),
	}

	if err := ioctl(c.fd.Fd(), IOCTL_BLKSNAP_SNAPSHOT_COLLECT, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to collect snapshots: %w", err)
	}

	return ids[:req.Count], nil
}

func (c *Control) WaitEvent(timeoutMs uint32) (*Event, error) {
	var req snapshotEvent
	req.TimeoutMs = timeoutMs

	if err := ioctl(c.fd.Fd(), IOCTL_BLKSNAP_SNAPSHOT_WAIT_EVENT, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, fmt.Errorf("failed to wait for event: %w", err)
	}

	event := &Event{
		ID:        req.ID,
		TimeoutMs: req.TimeoutMs,
		Code:      req.Code,
		TimeLabel: req.TimeLabel,
		data:      req.Data,
	}

	return event, nil
}
