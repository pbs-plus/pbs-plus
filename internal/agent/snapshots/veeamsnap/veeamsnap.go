//go:build linux

package veeamsnap

import (
	"fmt"
	"unsafe"
)

const (
	VeeamSnapControlDevice = "/dev/veeamsnap"
	VeeamSnapModuleName    = "veeamsnap"
	VeeamSnapImageName     = "veeamimage"
	MaxTrackingDeviceCount = 256
	UUIDSize               = 16
)

// Compatibility flags
const (
	CompatibilitySnapstore = 0x0000000000000001
	CompatibilityBtrfs     = 0x0000000000000002
	CompatibilityMultidev  = 0x0000000000000004
	CompatibilityKentry    = 0x0000000000000008
)

// Character device commands
const (
	CharCmdUndefined           = 0x00
	CharCmdAcknowledge         = 0x01
	CharCmdInvalid             = 0xFF
	CharCmdInitiate            = 0x21
	CharCmdNextPortion         = 0x22
	CharCmdNextPortionMultidev = 0x23
	CharCmdHalfFill            = 0x41
	CharCmdOverflow            = 0x42
	CharCmdTerminate           = 0x43
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

func _IO(typ, nr uintptr) uintptr {
	return _IOC(_IOC_NONE, typ, nr, 0)
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

const veeamSnapMagic = 'V'

// IOCTL commands
var (
	IOCTL_COMPATIBILITY_FLAGS                    = _IOW(veeamSnapMagic, 0, unsafe.Sizeof(CompatibilityFlags{}))
	IOCTL_GETVERSION                             = _IOW(veeamSnapMagic, 1, unsafe.Sizeof(Version{}))
	IOCTL_TRACKING_ADD                           = _IOW(veeamSnapMagic, 2, unsafe.Sizeof(DevID{}))
	IOCTL_TRACKING_REMOVE                        = _IOW(veeamSnapMagic, 3, unsafe.Sizeof(DevID{}))
	IOCTL_TRACKING_COLLECT                       = _IOW(veeamSnapMagic, 4, unsafe.Sizeof(TrackingCollect{}))
	IOCTL_TRACKING_BLOCK_SIZE                    = _IOW(veeamSnapMagic, 5, unsafe.Sizeof(uint32(0)))
	IOCTL_TRACKING_READ_CBT_BITMAP               = _IOR(veeamSnapMagic, 6, unsafe.Sizeof(TrackingReadCbtBitmap{}))
	IOCTL_TRACKING_MARK_DIRTY_BLOCKS             = _IOR(veeamSnapMagic, 7, unsafe.Sizeof(TrackingMarkDirtyBlocks{}))
	IOCTL_SET_KERNEL_ENTRIES                     = _IOW(veeamSnapMagic, 0x8, unsafe.Sizeof(SetKernelEntries{}))
	IOCTL_GET_UNRESOLVED_KERNEL_ENTRIES          = _IOR(veeamSnapMagic, 0x9, unsafe.Sizeof(GetUnresolvedKernelEntries{}))
	IOCTL_SNAPSHOT_CREATE                        = _IOW(veeamSnapMagic, 0x10, unsafe.Sizeof(SnapshotCreate{}))
	IOCTL_SNAPSHOT_DESTROY                       = _IOR(veeamSnapMagic, 0x11, unsafe.Sizeof(uint64(0)))
	IOCTL_SNAPSHOT_ERRNO                         = _IOW(veeamSnapMagic, 0x12, unsafe.Sizeof(SnapshotErrno{}))
	IOCTL_SNAPSTORE_CREATE                       = _IOR(veeamSnapMagic, 0x28, unsafe.Sizeof(SnapstoreCreate{}))
	IOCTL_SNAPSTORE_FILE                         = _IOR(veeamSnapMagic, 0x29, unsafe.Sizeof(SnapstoreFileAdd{}))
	IOCTL_SNAPSTORE_MEMORY                       = _IOR(veeamSnapMagic, 0x2A, unsafe.Sizeof(SnapstoreMemoryLimit{}))
	IOCTL_SNAPSTORE_CLEANUP                      = _IOW(veeamSnapMagic, 0x2B, unsafe.Sizeof(SnapstoreCleanup{}))
	IOCTL_SNAPSTORE_FILE_MULTIDEV                = _IOR(veeamSnapMagic, 0x2C, unsafe.Sizeof(SnapstoreFileAddMultidev{}))
	IOCTL_COLLECT_SNAPSHOT_IMAGES                = _IOW(veeamSnapMagic, 0x30, unsafe.Sizeof(CollectSnapshotImages{}))
	IOCTL_COLLECT_SNAPSHOTDATA_LOCATION_START    = _IOW(veeamSnapMagic, 0x40, unsafe.Sizeof(CollectSnapshotdataLocationStart{}))
	IOCTL_COLLECT_SNAPSHOTDATA_LOCATION_GET      = _IOW(veeamSnapMagic, 0x41, unsafe.Sizeof(CollectSnapshotdataLocationGet{}))
	IOCTL_COLLECT_SNAPSHOTDATA_LOCATION_COMPLETE = _IOR(veeamSnapMagic, 0x42, unsafe.Sizeof(CollectSnapshotdataLocationComplete{}))
	IOCTL_PERSISTENTCBT_DATA                     = _IOR(veeamSnapMagic, 0x48, unsafe.Sizeof(PersistentCbtData{}))
	IOCTL_PRINTSTATE                             = _IO(veeamSnapMagic, 0x80)
)

// UUID represents a 16-byte UUID
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

// Version structure
type Version struct {
	Major    uint16
	Minor    uint16
	Revision uint16
	Build    uint16
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", v.Major, v.Minor, v.Revision, v.Build)
}

// CompatibilityFlags structure
type CompatibilityFlags struct {
	Flags uint64
}

// DevID represents a device ID with major and minor numbers
type DevID struct {
	Major int32
	Minor int32
}

// CbtInfo structure
type CbtInfo struct {
	DevID        DevID
	DevCapacity  uint64
	CbtMapSize   uint32
	SnapNumber   uint8
	GenerationID UUID
}

// TrackingCollect structure
type TrackingCollect struct {
	Count   uint32
	_       uint32 // padding
	CbtInfo uint64 // pointer to CbtInfo array
}

// TrackingReadCbtBitmap structure
type TrackingReadCbtBitmap struct {
	DevID  DevID
	Offset uint32
	Length uint32
	Buff   uint64 // pointer to buffer
}

// BlockRange structure
type BlockRange struct {
	Offset uint64 // sectors
	Count  uint64 // sectors
}

// TrackingMarkDirtyBlocks structure
type TrackingMarkDirtyBlocks struct {
	ImageDevID  DevID
	Count       uint32
	_           uint32 // padding
	DirtyBlocks uint64 // pointer to BlockRange array
}

// KernelEntry structure
type KernelEntry struct {
	Addr uint64
	Name uint64 // pointer to string
}

// SetKernelEntries structure
type SetKernelEntries struct {
	Count   uint32
	_       uint32 // padding
	Entries uint64 // pointer to KernelEntry array
}

// GetUnresolvedKernelEntries structure
type GetUnresolvedKernelEntries struct {
	Buf [4096]byte
}

// SnapshotCreate structure
type SnapshotCreate struct {
	SnapshotID uint64
	Count      uint32
	_          uint32 // padding
	DevIDSet   uint64 // pointer to DevID array
}

// SnapshotErrno structure
type SnapshotErrno struct {
	DevID   DevID
	ErrCode int32
}

// Range structure
type Range struct {
	Left  uint64
	Right uint64
}

// SnapstoreCreate structure
type SnapstoreCreate struct {
	ID             UUID
	SnapstoreDevID DevID
	Count          uint32
	_              uint32 // padding
	DevIDSet       uint64 // pointer to DevID array
}

// SnapstoreFileAdd structure
type SnapstoreFileAdd struct {
	ID         UUID
	RangeCount uint32
	_          uint32 // padding
	Ranges     uint64 // pointer to Range array
}

// SnapstoreMemoryLimit structure
type SnapstoreMemoryLimit struct {
	ID   UUID
	Size uint64
}

// SnapstoreCleanup structure
type SnapstoreCleanup struct {
	ID          UUID
	FilledBytes uint64
}

// SnapstoreFileAddMultidev structure
type SnapstoreFileAddMultidev struct {
	ID         UUID
	DevID      DevID
	RangeCount uint32
	_          uint32 // padding
	Ranges     uint64 // pointer to Range array
}

// ImageInfo structure
type ImageInfo struct {
	OriginalDevID DevID
	SnapshotDevID DevID
}

// CollectSnapshotImages structure
type CollectSnapshotImages struct {
	Count     int32
	_         uint32 // padding
	ImageInfo uint64 // pointer to ImageInfo array
}

// CollectSnapshotdataLocationStart structure
type CollectSnapshotdataLocationStart struct {
	DevID       DevID
	MagicLength uint32
	_           uint32 // padding
	MagicBuff   uint64 // pointer to buffer
}

// CollectSnapshotdataLocationGet structure
type CollectSnapshotdataLocationGet struct {
	DevID      DevID
	RangeCount uint32
	_          uint32 // padding
	Ranges     uint64 // pointer to Range array
}

// CollectSnapshotdataLocationComplete structure
type CollectSnapshotdataLocationComplete struct {
	DevID DevID
}

// PersistentCbtData structure
type PersistentCbtData struct {
	Size      uint32
	_         uint32 // padding
	Parameter uint64 // pointer to string
}
