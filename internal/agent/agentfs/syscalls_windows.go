//go:build windows

package agentfs

import (
	"fmt"
	"strings"
	"syscall"
	"unsafe"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

var (
	modadvapi32                    = windows.NewLazySystemDLL("advapi32.dll")
	modkernel32                    = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpace           = modkernel32.NewProc("GetDiskFreeSpaceW")
	procGetSystemInfo              = modkernel32.NewProc("GetSystemInfo")
	procGetSecurityDescriptorOwner = modadvapi32.NewProc("GetSecurityDescriptorOwner")
	procGetSecurityDescriptorGroup = modadvapi32.NewProc("GetSecurityDescriptorGroup")
)

func getStatFS(driveLetter string) (types.StatFS, error) {
	driveLetter = strings.TrimSpace(driveLetter)
	driveLetter = strings.ToUpper(driveLetter)

	if len(driveLetter) == 1 {
		driveLetter += ":"
	}

	if len(driveLetter) != 2 || driveLetter[1] != ':' {
		return types.StatFS{}, fmt.Errorf("invalid drive letter format: %s", driveLetter)
	}

	path := driveLetter + `\`

	var sectorsPerCluster, bytesPerSector, numberOfFreeClusters, totalNumberOfClusters uint32

	rootPathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return types.StatFS{}, fmt.Errorf("failed to convert path to UTF16: %w", err)
	}

	ret, _, err := procGetDiskFreeSpace.Call(
		uintptr(unsafe.Pointer(rootPathPtr)),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&numberOfFreeClusters)),
		uintptr(unsafe.Pointer(&totalNumberOfClusters)),
	)
	if ret == 0 {
		return types.StatFS{}, fmt.Errorf("GetDiskFreeSpaceW failed: %w", err)
	}

	blockSize := uint64(sectorsPerCluster) * uint64(bytesPerSector)
	totalBlocks := uint64(totalNumberOfClusters)

	stat := types.StatFS{
		Bsize:   blockSize,
		Blocks:  totalBlocks,
		Bfree:   0,
		Bavail:  0,               // Assuming Bavail is the same as Bfree
		Files:   uint64(1 << 20), // Windows does not provide total inodes
		Ffree:   0,               // Windows does not provide free inodes
		NameLen: 255,
	}

	return stat, nil
}

type FileAllocatedRangeBuffer struct {
	FileOffset int64 // Starting offset of the range
	Length     int64 // Length of the range
}

func queryAllocatedRanges(handle windows.Handle, fileSize int64) ([]FileAllocatedRangeBuffer, error) {
	// Handle edge case: zero file size
	if fileSize == 0 {
		return nil, nil
	}

	// Define the input range for the query
	var inputRange FileAllocatedRangeBuffer
	inputRange.FileOffset = 0
	inputRange.Length = fileSize

	// Constants for buffer size calculations
	rangeSize := int(unsafe.Sizeof(FileAllocatedRangeBuffer{}))

	// Start with a small buffer and dynamically resize if needed
	bufferSize := 1 // Start with space for 1 range
	var bytesReturned uint32

	for {
		// Allocate the output buffer
		outputBuffer := make([]FileAllocatedRangeBuffer, bufferSize)

		// Call DeviceIoControl
		err := windows.DeviceIoControl(
			handle,
			windows.FSCTL_QUERY_ALLOCATED_RANGES,
			(*byte)(unsafe.Pointer(&inputRange)),
			uint32(unsafe.Sizeof(inputRange)),
			(*byte)(unsafe.Pointer(&outputBuffer[0])),
			uint32(bufferSize*rangeSize),
			&bytesReturned,
			nil,
		)

		if err == nil {
			// Success: Calculate the number of ranges returned
			count := int(bytesReturned) / rangeSize
			return outputBuffer[:count], nil
		}

		if err == windows.ERROR_MORE_DATA {
			// Buffer was too small: Increase the buffer size and retry
			bufferSize *= 2
			continue
		}

		if err == windows.ERROR_INVALID_FUNCTION {
			// Filesystem does not support FSCTL_QUERY_ALLOCATED_RANGES
			// Return a single range covering the whole file
			return []FileAllocatedRangeBuffer{
				{FileOffset: 0, Length: fileSize},
			}, nil
		}

		return nil, fmt.Errorf("DeviceIoControl failed: %w", err)
	}
}

func getFileSize(handle windows.Handle) (int64, error) {
	var fileInfo windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return 0, mapWinError(err, "getFileSize GetFileInformationByHandle")
	}

	// Combine the high and low parts of the file size
	return int64(fileInfo.FileSizeHigh)<<32 + int64(fileInfo.FileSizeLow), nil
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

func GetAllocGranularity() int {
	var si systemInfo
	// this cannot fail
	procGetSystemInfo.Call(uintptr(unsafe.Pointer(&si)))
	return int(si.AllocationGranularity)
}

// filetimeToUnix converts a Windows FILETIME to a Unix timestamp.
// Windows file times are in 100-nanosecond intervals since January 1, 1601.
func filetimeToUnix(ft syscall.Filetime) int64 {
	const (
		winToUnixEpochDiff = 116444736000000000 // in 100-nanosecond units
		hundredNano        = 10000000           // 100-ns units per second
	)
	t := (int64(ft.HighDateTime) << 32) | int64(ft.LowDateTime)
	return (t - winToUnixEpochDiff) / hundredNano
}

// parseFileAttributes converts Windows file attribute flags into a map.
func parseFileAttributes(attr uint32) map[string]bool {
	attributes := make(map[string]bool)
	// Attributes are defined in golang.org/x/sys/windows.
	if attr&windows.FILE_ATTRIBUTE_READONLY != 0 {
		attributes["readOnly"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_HIDDEN != 0 {
		attributes["hidden"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_SYSTEM != 0 {
		attributes["system"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		attributes["directory"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_ARCHIVE != 0 {
		attributes["archive"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_NORMAL != 0 {
		attributes["normal"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_TEMPORARY != 0 {
		attributes["temporary"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_SPARSE_FILE != 0 {
		attributes["sparseFile"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		attributes["reparsePoint"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_COMPRESSED != 0 {
		attributes["compressed"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_OFFLINE != 0 {
		attributes["offline"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED != 0 {
		attributes["notContentIndexed"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_ENCRYPTED != 0 {
		attributes["encrypted"] = true
	}
	return attributes
}

// getFileSecurityInfo retrieves owner, group, and ACL details using the
// corrected GetNamedSecurityInfo usage. The ACL extraction is left as a stub.
func getFileSecurityInfo(fullPath string) (string, string, []types.WinACL, error) {
	// Call the wrapper version of GetNamedSecurityInfo, which takes a golang
	// string and returns a self-relative SECURITY_DESCRIPTOR.
	sd, err := windows.GetNamedSecurityInfo(
		fullPath,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|
			windows.GROUP_SECURITY_INFORMATION|
			windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return "", "", nil, err
	}
	// Retrieve owner SID.
	ownerSID, _, err := getSecurityDescriptorOwner(sd)
	if err != nil {
		return "", "", nil, err
	}
	ownerStr := ownerSID.String()

	// Retrieve group SID.
	groupSID, _, err := getSecurityDescriptorGroup(sd)
	if err != nil {
		return "", "", nil, err
	}
	groupStr := groupSID.String()

	// ACL extraction would normally enumerate the DACL here.
	acls := []types.WinACL{}

	return ownerStr, groupStr, acls, nil
}

// getSecurityDescriptorOwner is a wrapper for the Win32 API
// GetSecurityDescriptorOwner. It returns the owner SID.
func getSecurityDescriptorOwner(
	sd *windows.SECURITY_DESCRIPTOR,
) (*windows.SID, bool, error) {
	var owner *windows.SID
	var ownerDefaulted uint32

	ret, _, err := procGetSecurityDescriptorOwner.Call(
		uintptr(unsafe.Pointer(sd)),
		uintptr(unsafe.Pointer(&owner)),
		uintptr(unsafe.Pointer(&ownerDefaulted)),
	)
	if ret == 0 {
		return nil, false, err
	}
	return owner, ownerDefaulted != 0, nil
}

// getSecurityDescriptorGroup is a wrapper for the Win32 API
// GetSecurityDescriptorGroup. It returns the group SID.
func getSecurityDescriptorGroup(
	sd *windows.SECURITY_DESCRIPTOR,
) (*windows.SID, bool, error) {
	var group *windows.SID
	var groupDefaulted uint32

	ret, _, err := procGetSecurityDescriptorGroup.Call(
		uintptr(unsafe.Pointer(sd)),
		uintptr(unsafe.Pointer(&group)),
		uintptr(unsafe.Pointer(&groupDefaulted)),
	)
	if ret == 0 {
		return nil, false, err
	}
	return group, groupDefaulted != 0, nil
}
