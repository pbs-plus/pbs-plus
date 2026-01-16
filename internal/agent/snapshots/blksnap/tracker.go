//go:build linux

package blksnap

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

type Tracker struct {
	device string
	fd     *os.File
}

func NewTracker(devicePath string) (*Tracker, error) {
	fd, err := os.OpenFile(devicePath, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open device %s: %w", devicePath, err)
	}

	return &Tracker{
		device: devicePath,
		fd:     fd,
	}, nil
}

func (t *Tracker) Close() error {
	if t.fd != nil {
		return t.fd.Close()
	}
	return nil
}

func (t *Tracker) Attach() error {
	var attach blkfilterAttach
	copy(attach.Name[:], "blksnap")

	return ioctl(t.fd.Fd(), BLKFILTER_ATTACH, uintptr(unsafe.Pointer(&attach)))
}

func (t *Tracker) Detach() error {
	var detach blkfilterDetach
	copy(detach.Name[:], "blksnap")

	return ioctl(t.fd.Fd(), BLKFILTER_DETACH, uintptr(unsafe.Pointer(&detach)))
}

func (t *Tracker) GetCbtInfo() (*CbtInfo, error) {
	var info CbtInfo

	ctl := blkfilterCtl{
		Cmd:    BLKFILTER_CTL_BLKSNAP_CBTINFO,
		Optlen: uint32(unsafe.Sizeof(info)),
		Opt:    uint64(uintptr(unsafe.Pointer(&info))),
	}
	copy(ctl.Name[:], "blksnap")

	if err := ioctl(t.fd.Fd(), BLKFILTER_CTL, uintptr(unsafe.Pointer(&ctl))); err != nil {
		return nil, fmt.Errorf("failed to get CBT info: %w", err)
	}

	return &info, nil
}

func (t *Tracker) ReadCbtMap(offset, length uint32) ([]byte, error) {
	buffer := make([]byte, length)

	req := cbtMap{
		Offset: offset,
		Length: length,
		Buffer: uint64(uintptr(unsafe.Pointer(&buffer[0]))),
	}

	ctl := blkfilterCtl{
		Cmd:    BLKFILTER_CTL_BLKSNAP_CBTMAP,
		Optlen: uint32(unsafe.Sizeof(req)),
		Opt:    uint64(uintptr(unsafe.Pointer(&req))),
	}
	copy(ctl.Name[:], "blksnap")

	if err := ioctl(t.fd.Fd(), BLKFILTER_CTL, uintptr(unsafe.Pointer(&ctl))); err != nil {
		return nil, fmt.Errorf("failed to read CBT map: %w", err)
	}

	return buffer, nil
}

func (t *Tracker) MarkDirtyBlocks(ranges []Range) error {
	if len(ranges) == 0 {
		return nil
	}

	req := cbtDirty{
		Count:        uint32(len(ranges)),
		DirtySectors: uint64(uintptr(unsafe.Pointer(&ranges[0]))),
	}

	ctl := blkfilterCtl{
		Cmd:    BLKFILTER_CTL_BLKSNAP_CBTDIRTY,
		Optlen: uint32(unsafe.Sizeof(req)),
		Opt:    uint64(uintptr(unsafe.Pointer(&req))),
	}
	copy(ctl.Name[:], "blksnap")

	return ioctl(t.fd.Fd(), BLKFILTER_CTL, uintptr(unsafe.Pointer(&ctl)))
}

func (t *Tracker) AddToSnapshot(snapshotID UUID) error {
	req := snapshotAdd{
		ID: snapshotID,
	}

	ctl := blkfilterCtl{
		Cmd:    BLKFILTER_CTL_BLKSNAP_SNAPSHOTADD,
		Optlen: uint32(unsafe.Sizeof(req)),
		Opt:    uint64(uintptr(unsafe.Pointer(&req))),
	}
	copy(ctl.Name[:], "blksnap")

	return ioctl(t.fd.Fd(), BLKFILTER_CTL, uintptr(unsafe.Pointer(&ctl)))
}

func (t *Tracker) GetSnapshotInfo() (*SnapshotInfo, error) {
	var info SnapshotInfo

	ctl := blkfilterCtl{
		Cmd:    BLKFILTER_CTL_BLKSNAP_SNAPSHOTINFO,
		Optlen: uint32(unsafe.Sizeof(info)),
		Opt:    uint64(uintptr(unsafe.Pointer(&info))),
	}
	copy(ctl.Name[:], "blksnap")

	if err := ioctl(t.fd.Fd(), BLKFILTER_CTL, uintptr(unsafe.Pointer(&ctl))); err != nil {
		return nil, fmt.Errorf("failed to get snapshot info: %w", err)
	}

	return &info, nil
}
