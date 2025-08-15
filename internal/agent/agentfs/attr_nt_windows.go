//go:build windows

package agentfs

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// NT native imports and structs

var (
	procNtQueryInformationFile = ntdll.NewProc("NtQueryInformationFile")
	procRtlNtStatusToDosError  = ntdll.NewProc("RtlNtStatusToDosError")
)

type ioStatusBlock struct {
	Status      uintptr // NTSTATUS (use uintptr to match pointer size)
	Information uintptr
}

type fileBasicInformation struct {
	CreationTime, LastAccessTime, LastWriteTime, ChangeTime int64
	FileAttributes                                          uint32
	_                                                       uint32 // alignment to 8
}

type fileStandardInformation struct {
	AllocationSize int64
	EndOfFile      int64
	NumberOfLinks  uint32
	DeletePending  byte
	Directory      byte
	_              [2]byte
}

// Substructures included in FILE_ALL_INFORMATION that we don't need to fill here
type fileInternalInformation struct {
	IndexNumber int64
}

type fileEaInformation struct {
	EaSize uint32
}

type fileAccessInformation struct {
	AccessFlags uint32
}

type filePositionInformation struct {
	CurrentByteOffset int64
}

type fileModeInformation struct {
	Mode uint32
}

type fileAlignmentInformation struct {
	AlignmentRequirement uint32
}

type unicodeString struct {
	Length        uint16
	MaximumLength uint16
	Buffer        *uint16
}

type fileNameInformation struct {
	FileNameLength uint32
	// Followed by FileNameLength bytes (UTF-16), but we don't need it here.
}

// FILE_ALL_INFORMATION packs several blocks back-to-back.
// This matches the WDK layout. We only read Basic and Standard.
type fileAllInformation struct {
	BasicInformation     fileBasicInformation
	StandardInformation  fileStandardInformation
	InternalInformation  fileInternalInformation
	EaInformation        fileEaInformation
	AccessInformation    fileAccessInformation
	PositionInformation  filePositionInformation
	ModeInformation      fileModeInformation
	AlignmentInformation fileAlignmentInformation
	NameInformation      fileNameInformation // variable length tail; we ignore it
}

const (
	fileAllInformationClass = 18 // FILE_INFORMATION_CLASS::FileAllInformation
)

func ntStatusToError(status uintptr) error {
	if status == 0 {
		return nil
	}
	// Convert NTSTATUS to Win32 error for consistency with other Windows calls.
	r0, _, _ := procRtlNtStatusToDosError.Call(status)
	return windows.Errno(r0)
}

// ntQueryFileAllInformation queries FILE_ALL_INFORMATION in one syscall.
func ntQueryFileAllInformation(h windows.Handle, out *fileAllInformation) error {
	var iosb ioStatusBlock
	buf := (*byte)(unsafe.Pointer(out))
	bufLen := uint32(unsafe.Sizeof(*out))

	r0, _, _ := procNtQueryInformationFile.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&iosb)),
		uintptr(unsafe.Pointer(buf)),
		uintptr(bufLen),
		uintptr(fileAllInformationClass),
	)
	if err := ntStatusToError(r0); err != nil {
		return err
	}
	return nil
}
