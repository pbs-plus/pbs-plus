//go:build windows

package snapshots

import (
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	COLE_DEFAULT_PRINCIPAL        = ^uintptr(0) // Equivalent to -1 for COM
	RPC_C_AUTHN_LEVEL_PKT_PRIVACY = 6
	RPC_C_IMP_LEVEL_IDENTIFY      = 2
	EOAC_NONE                     = 0
)

const (
	VSS_OBJECT_SNAPSHOT = 1
)

type VSS_SNAPSHOT_PROP struct {
	m_SnapshotId               windows.GUID
	m_SnapshotSetId            windows.GUID
	m_lSnapshotsCount          int32
	m_pwszSnapshotDeviceObject *uint16
	m_pwszOriginalVolumeName   *uint16
	m_pwszOriginatingMachine   *uint16
	m_pwszServiceMachine       *uint16
	m_pwszExposedName          *uint16
	m_pwszExposedPath          *uint16
	m_ProviderId               windows.GUID
	m_lSnapshotAttributes      int32
	m_tsCreationTimestamp      int64
	m_eStatus                  int32
}

var (
	ole32  = windows.NewLazySystemDLL("ole32.dll")
	vssapi = windows.NewLazySystemDLL("vssapi.dll")

	procCoInitializeSecurity      = ole32.NewProc("CoInitializeSecurity")
	procCreateVssBackupComponents = vssapi.NewProc("CreateVssBackupComponents")
)

var IID_IVssBackupComponents = windows.GUID{
	Data1: 0x665c1d5f,
	Data2: 0xc218,
	Data3: 0x414d,
	Data4: [8]byte{0xa0, 0x5a, 0x87, 0x39, 0xee, 0x5d, 0x0b, 0x47},
}

type IVssBackupComponents struct {
	vtbl *IVssBackupComponentsVtbl
}

type IVssBackupComponentsVtbl struct {
	QueryInterface         uintptr
	AddRef                 uintptr
	Release                uintptr
	GetWriterComponents    uintptr
	InitializeForBackup    uintptr
	SetBackupState         uintptr
	InitializeForRestore   uintptr
	SetRestoreState        uintptr
	GatherWriterMetadata   uintptr
	GetWriterMetadataCount uintptr
	GetWriterMetadata      uintptr
	FreeWriterMetadata     uintptr
	AddComponent           uintptr
	PrepareForBackup       uintptr
	AbortBackup            uintptr
	GatherWriterStatus     uintptr
	GetWriterStatusCount   uintptr
	FreeWriterStatus       uintptr
	StartSnapshotSet       uintptr
	AddToSnapshotSet       uintptr
	DoSnapshotSet          uintptr
	DeleteSnapshots        uintptr
	ImportSnapshots        uintptr
	BreakSnapshotSet       uintptr
	GetSnapshotProperties  uintptr
	Query                  uintptr
	IsVolumeSupported      uintptr
	DisableWriterClasses   uintptr
	EnableWriterClasses    uintptr
	DisableWriterInstances uintptr
	ExposeSnapshot         uintptr
	RevertToSnapshot       uintptr
	QueryRevertStatus      uintptr
}

type VSSSnapshot struct {
	BackupComponents *IVssBackupComponents
	SnapshotID       windows.GUID
	SnapshotSetID    windows.GUID
	Path             string
}

func InitializeVSS() error {
	// Initialize COM
	if err := windows.CoInitializeEx(0, windows.COINIT_APARTMENTTHREADED); err != nil {
		return fmt.Errorf("CoInitializeEx failed: %v", err)
	}

	// Initialize COM Security
	if err := initializeComSecurity(); err != nil {
		return fmt.Errorf("failed to initialize COM security: %v", err)
	}

	return nil
}

func initializeComSecurity() error {
	ret, _, _ := syscall.SyscallN(procCoInitializeSecurity.Addr(),
		0,
		uintptr(COLE_DEFAULT_PRINCIPAL),
		0,
		0,
		uintptr(RPC_C_AUTHN_LEVEL_PKT_PRIVACY),
		uintptr(RPC_C_IMP_LEVEL_IDENTIFY),
		0,
		uintptr(EOAC_NONE),
		0,
	)
	if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
		return fmt.Errorf("CoInitializeSecurity failed: %v", ret)
	}
	return nil
}

func CreateVssBackupComponents() (*IVssBackupComponents, error) {
	var backupComponents *IVssBackupComponents
	ret, _, _ := syscall.SyscallN(procCreateVssBackupComponents.Addr(),
		uintptr(unsafe.Pointer(&backupComponents)),
	)
	if ret != 0 {
		return nil, fmt.Errorf("CreateVssBackupComponents failed: %v", ret)
	}
	return backupComponents, nil
}

func CreateSnapshot(volume string) (*VSSSnapshot, error) {
	backupComponents, err := CreateVssBackupComponents()
	if err != nil {
		return nil, err
	}

	// Initialize for backup
	ret, _, _ := syscall.SyscallN(backupComponents.vtbl.InitializeForBackup,
		uintptr(unsafe.Pointer(backupComponents)),
		0,
		0,
	)
	if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
		return nil, fmt.Errorf("InitializeForBackup failed: %v", ret)
	}

	// Start new snapshot set
	var snapshotSetId windows.GUID
	ret, _, _ = syscall.SyscallN(backupComponents.vtbl.StartSnapshotSet,
		uintptr(unsafe.Pointer(backupComponents)),
		uintptr(unsafe.Pointer(&snapshotSetId)),
		0,
	)
	if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
		return nil, fmt.Errorf("StartSnapshotSet failed: %v", ret)
	}

	// Add volume to snapshot set
	var snapshotId windows.GUID
	volumePtr, _ := windows.UTF16PtrFromString(volume)
	ret, _, _ = syscall.SyscallN(backupComponents.vtbl.AddToSnapshotSet,
		uintptr(unsafe.Pointer(backupComponents)),
		uintptr(unsafe.Pointer(volumePtr)),
		0,
		uintptr(unsafe.Pointer(&snapshotId)),
		0,
		0,
	)
	if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
		return nil, fmt.Errorf("AddToSnapshotSet failed: %v", ret)
	}

	// Create the snapshot
	ret, _, _ = syscall.SyscallN(backupComponents.vtbl.DoSnapshotSet,
		uintptr(unsafe.Pointer(backupComponents)),
		0,
		0,
	)
	if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
		return nil, fmt.Errorf("DoSnapshotSet failed: %v", ret)
	}

	return &VSSSnapshot{
		BackupComponents: backupComponents,
		SnapshotID:       snapshotId,
		SnapshotSetID:    snapshotSetId,
	}, nil
}

func (s *VSSSnapshot) GetSnapshotPath() (string, error) {
	if s.Path != "" {
		return s.Path, nil
	}

	// Get snapshot properties to find the device path
	var props VSS_SNAPSHOT_PROP
	ret, _, _ := syscall.SyscallN(s.BackupComponents.vtbl.GetSnapshotProperties,
		uintptr(unsafe.Pointer(s.BackupComponents)),
		uintptr(unsafe.Pointer(&s.SnapshotID)),
		uintptr(unsafe.Pointer(&props)),
	)
	if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
		return "", fmt.Errorf("GetSnapshotProperties failed: %v", ret)
	}
	defer windows.CoTaskMemFree(unsafe.Pointer(props.m_pwszSnapshotDeviceObject))

	s.Path = windows.UTF16PtrToString(props.m_pwszSnapshotDeviceObject)
	return s.Path, nil
}

func (s *VSSSnapshot) Close() error {
	if s.BackupComponents != nil {
		// Delete the snapshot
		var deleteCount uint32
		ret, _, _ := syscall.SyscallN(s.BackupComponents.vtbl.DeleteSnapshots,
			uintptr(unsafe.Pointer(s.BackupComponents)),
			uintptr(unsafe.Pointer(&s.SnapshotID)),
			uintptr(VSS_OBJECT_SNAPSHOT),
			1, // Delete all snapshots
			uintptr(unsafe.Pointer(&deleteCount)),
			0,
			0,
			0,
			0,
		)
		if ret != 0 && ret != uintptr(windows.ERROR_SUCCESS) {
			return fmt.Errorf("DeleteSnapshots failed: %v", ret)
		}

		// Release COM interface
		syscall.SyscallN(s.BackupComponents.vtbl.Release,
			uintptr(unsafe.Pointer(s.BackupComponents)),
			0,
			0,
		)
		s.BackupComponents = nil
	}
	return nil
}
