//go:build linux

package blksnap

import (
	"fmt"
	"unsafe"
)

const (
	BlksnapControlDevice = "/dev/blksnap-control"
	BlksnapImageName     = "blksnap"
	BlkfilterNameLength  = 32
	ImageDiskNameLen     = 32

	SectorShift = 9
	SectorSize  = 1 << SectorShift

	UUIDSize = 16
)

const (
	_IOC_NRBITS   = 8
	_IOC_TYPEBITS = 8
	_IOC_SIZEBITS = 14
	_IOC_DIRBITS  = 2

	_IOC_NRSHIFT   = 0
	_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
	_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
	_IOC_DIRSHIFT  = _IOC_SIZESHIFT + _IOC_SIZEBITS

	_IOC_NONE  = 0
	_IOC_WRITE = 1
	_IOC_READ  = 2
)

func _IOC(dir, typ, nr, size uintptr) uintptr {
	return (dir << _IOC_DIRSHIFT) |
		(typ << _IOC_TYPESHIFT) |
		(nr << _IOC_NRSHIFT) |
		(size << _IOC_SIZESHIFT)
}

func _IOR(typ, nr, size uintptr) uintptr {
	return _IOC(_IOC_READ, typ, nr, size)
}

func _IOW(typ, nr, size uintptr) uintptr {
	return _IOC(_IOC_WRITE, typ, nr, size)
}

func _IOWR(typ, nr, size uintptr) uintptr {
	return _IOC(_IOC_READ|_IOC_WRITE, typ, nr, size)
}

const (
	blksnapMagic = 'V'
	blockMagic   = 0x12
)

var (
	BLKFILTER_ATTACH = _IOWR(blockMagic, 142, unsafe.Sizeof(blkfilterAttach{}))
	BLKFILTER_DETACH = _IOWR(blockMagic, 143, unsafe.Sizeof(blkfilterDetach{}))
	BLKFILTER_CTL    = _IOWR(blockMagic, 144, unsafe.Sizeof(blkfilterCtl{}))
)

var (
	IOCTL_BLKSNAP_VERSION             = _IOR(blksnapMagic, 0, unsafe.Sizeof(Version{}))
	IOCTL_BLKSNAP_SNAPSHOT_CREATE     = _IOWR(blksnapMagic, 1, unsafe.Sizeof(snapshotCreate{}))
	IOCTL_BLKSNAP_SNAPSHOT_DESTROY    = _IOW(blksnapMagic, 2, unsafe.Sizeof(UUID{}))
	IOCTL_BLKSNAP_SNAPSHOT_TAKE       = _IOW(blksnapMagic, 3, unsafe.Sizeof(UUID{}))
	IOCTL_BLKSNAP_SNAPSHOT_COLLECT    = _IOR(blksnapMagic, 4, unsafe.Sizeof(snapshotCollect{}))
	IOCTL_BLKSNAP_SNAPSHOT_WAIT_EVENT = _IOR(blksnapMagic, 5, unsafe.Sizeof(snapshotEvent{}))
)

const (
	BLKFILTER_CTL_BLKSNAP_CBTINFO      = 0
	BLKFILTER_CTL_BLKSNAP_CBTMAP       = 1
	BLKFILTER_CTL_BLKSNAP_CBTDIRTY     = 2
	BLKFILTER_CTL_BLKSNAP_SNAPSHOTADD  = 3
	BLKFILTER_CTL_BLKSNAP_SNAPSHOTINFO = 4
)

const (
	BlksnapEventCodeCorrupted = 0
	BlksnapEventCodeNoSpace   = 1
)

type Version struct {
	Major    uint16
	Minor    uint16
	Revision uint16
	Build    uint16
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", v.Major, v.Minor, v.Revision, v.Build)
}

type UUID [UUIDSize]byte

func (u UUID) String() string {
	return fmt.Sprintf(
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		u[0], u[1], u[2], u[3],
		u[4], u[5],
		u[6], u[7],
		u[8], u[9],
		u[10], u[11], u[12], u[13], u[14], u[15],
	)
}

func ParseUUID(s string) (UUID, error) {
	var u UUID
	_, err := fmt.Sscanf(s,
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		&u[0], &u[1], &u[2], &u[3],
		&u[4], &u[5],
		&u[6], &u[7],
		&u[8], &u[9],
		&u[10], &u[11], &u[12], &u[13], &u[14], &u[15],
	)
	return u, err
}

type Range struct {
	Offset uint64 // Offset from beginning in sectors
	Count  uint64 // Number of sectors
}

type CbtInfo struct {
	DeviceCapacity uint64
	BlockSize      uint32
	BlockCount     uint32
	GenerationID   UUID
	ChangesNumber  uint8
}

type SnapshotInfo struct {
	ErrorCode int32
	Image     [ImageDiskNameLen]byte
}

func (s *SnapshotInfo) ImageName() string {
	for i, b := range s.Image {
		if b == 0 {
			return string(s.Image[:i])
		}
	}
	return string(s.Image[:])
}

type EventCorrupted struct {
	DevIDMj uint32 // Major device ID
	DevIDMn uint32 // Minor device ID
	ErrCode int32  // Error code
}

type EventNoSpace struct {
	RequestedNrSect uint64 // Requested number of sectors
}

type Event struct {
	ID        UUID
	TimeoutMs uint32
	Code      uint32
	TimeLabel int64
	// Raw data buffer (union of event types)
	data [4096 - 32]byte
}

func (e *Event) GetCorrupted() EventCorrupted {
	return EventCorrupted{
		DevIDMj: *(*uint32)(unsafe.Pointer(&e.data[0])),
		DevIDMn: *(*uint32)(unsafe.Pointer(&e.data[4])),
		ErrCode: *(*int32)(unsafe.Pointer(&e.data[8])),
	}
}

func (e *Event) GetNoSpace() EventNoSpace {
	return EventNoSpace{
		RequestedNrSect: *(*uint64)(unsafe.Pointer(&e.data[0])),
	}
}

type blkfilterAttach struct {
	Name   [BlkfilterNameLength]byte
	Opt    uint64
	Optlen uint32
}

type blkfilterDetach struct {
	Name [BlkfilterNameLength]byte
}

type blkfilterCtl struct {
	Name   [BlkfilterNameLength]byte
	Cmd    uint32
	Optlen uint32
	Opt    uint64
}

type snapshotCreate struct {
	DiffStorageLimitSect uint64
	DiffStorageFilename  uint64 // pointer to string
	ID                   UUID
}

type snapshotCollect struct {
	Count uint32
	_     uint32 // padding
	IDs   uint64 // pointer to array of UUIDs
}

type snapshotEvent struct {
	ID        UUID
	TimeoutMs uint32
	Code      uint32
	TimeLabel int64
	Data      [4096 - 32]byte
}

type cbtMap struct {
	Offset uint32
	Length uint32
	Buffer uint64 // pointer
}

type cbtDirty struct {
	Count        uint32
	_            uint32 // padding
	DirtySectors uint64 // pointer to blksnap_sectors array
}

type snapshotAdd struct {
	ID UUID
}
