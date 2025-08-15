//go:build windows

package agentfs

import (
	"sync"

	"golang.org/x/sys/windows"
)

type fileStandardInfo struct {
	AllocationSize int64  // LARGE_INTEGER
	EndOfFile      int64  // LARGE_INTEGER
	NumberOfLinks  uint32 // DWORD
	DeletePending  byte   // BOOLEAN
	Directory      byte   // BOOLEAN
	_              [2]byte
}

type FileHandle struct {
	sync.Mutex
	handle        windows.Handle
	fileSize      int64
	isDir         bool
	dirReader     *DirReaderNT
	mapping       windows.Handle
	logicalOffset int64
}

type DirReaderNT struct {
	handle      uintptr
	ioStatus    IoStatusBlock
	restartScan bool
	noMoreFiles bool
	path        string
}

type UnicodeString struct {
	Length        uint16
	MaximumLength uint16
	Buffer        *uint16
}

type ObjectAttributes struct {
	Length                   uint32
	RootDirectory            uintptr
	ObjectName               *UnicodeString
	Attributes               uint32
	SecurityDescriptor       uintptr
	SecurityQualityOfService uintptr
}

type IoStatusBlock struct {
	Status      int32
	Information uintptr
}

type FileDirectoryInformation struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    int64
	LastAccessTime  int64
	LastWriteTime   int64
	ChangeTime      int64
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32
	FileName        uint16
}

type fileNetworkOpenInformation struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	AllocationSize int64
	EndOfFile      int64
	FileAttributes uint32
	_              uint32 // alignment
}

type allocatedRange struct {
	FileOffset int64
	Length     int64
}

type systemInfo struct {
	// This is the first member of the union
	OemID uint32
	// These are the second member of the union
	//      ProcessorArchitecture uint16;
	//      Reserved uint16;
	PageSize                  uint32
	MinimumApplicationAddress uintptr
	MaximumApplicationAddress uintptr
	ActiveProcessorMask       *uint32
	NumberOfProcessors        uint32
	ProcessorType             uint32
	AllocationGranularity     uint32
	ProcessorLevel            uint16
	ProcessorRevision         uint16
}
