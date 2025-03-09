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
	modkernel32          = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpace = modkernel32.NewProc("GetDiskFreeSpaceW")
	procGetSystemInfo    = modkernel32.NewProc("GetSystemInfo")
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
	// Validate fileSize.
	if fileSize < 0 {
		return nil, fmt.Errorf("invalid fileSize: %d", fileSize)
	}

	// Handle edge case: zero file size.
	if fileSize == 0 {
		return nil, nil
	}

	// Define the input range for the query.
	var inputRange FileAllocatedRangeBuffer
	inputRange.FileOffset = 0
	inputRange.Length = fileSize

	// Constant for buffer size calculations.
	rangeSize := int(unsafe.Sizeof(FileAllocatedRangeBuffer{}))
	if rangeSize == 0 {
		return nil, fmt.Errorf("computed rangeSize is 0, invalid FileAllocatedRangeBuffer")
	}

	// Start with a small buffer and dynamically resize if needed.
	bufferSize := 1 // Start with space for 1 range.
	var bytesReturned uint32

	// Set an arbitrary maximum to avoid infinite allocation.
	const maxBufferSize = 1 << 20 // for example, 1 million entries.
	for {
		if bufferSize > maxBufferSize {
			return nil, fmt.Errorf("buffer size exceeded maximum threshold: %d", bufferSize)
		}

		// Allocate the output buffer.
		outputBuffer := make([]FileAllocatedRangeBuffer, bufferSize)

		// Call DeviceIoControl.
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
			// Success: calculate the number of ranges returned.
			if bytesReturned%uint32(rangeSize) != 0 {
				return nil, fmt.Errorf("inconsistent number of bytes returned: %d", bytesReturned)
			}
			count := int(bytesReturned) / rangeSize
			if count > len(outputBuffer) {
				return nil, fmt.Errorf("invalid count computed: %d", count)
			}
			return outputBuffer[:count], nil
		}

		// If the buffer was too small, double the bufferSize and retry.
		if err == windows.ERROR_MORE_DATA {
			bufferSize *= 2
			continue
		}

		// If the filesystem doesn't support FSCTL_QUERY_ALLOCATED_RANGES, return
		// a single range covering the whole file.
		if err == windows.ERROR_INVALID_FUNCTION {
			return []FileAllocatedRangeBuffer{
				{FileOffset: 0, Length: fileSize},
			}, nil
		}

		// For any other error, return it wrapped.
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

func GetWinACLs(filePath string) (string, string, []types.WinACL, error) {
	// Request owner, group, and DACL information.
	secInfo := windows.OWNER_SECURITY_INFORMATION |
		windows.GROUP_SECURITY_INFORMATION |
		windows.DACL_SECURITY_INFORMATION

	// Retrieve the self-relative security descriptor.
	secDesc, err := GetFileSecurityDescriptor(filePath, uint32(secInfo))
	if err != nil {
		return "", "", nil, fmt.Errorf("GetFileSecurityDescriptor failed: %w", err)
	}

	// Validate the descriptor.
	valid, err := IsValidSecDescriptor(secDesc)
	if err != nil || !valid {
		return "", "", nil, fmt.Errorf("invalid security descriptor: %w", err)
	}

	// Convert to an absolute security descriptor.
	absoluteSD, err := MakeAbsoluteSD(secDesc)
	if err != nil {
		return "", "", nil, fmt.Errorf("MakeAbsoluteSD failed: %w", err)
	}

	// Extract owner and group SIDs.
	owner, group, err := getOwnerGroupAbsolute(absoluteSD)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to extract owner/group: %w", err)
	}

	// Retrieve the DACL.
	acl, present, _, err := GetSecurityDescriptorDACL(absoluteSD)
	if err != nil {
		return owner, group, nil, fmt.Errorf("GetSecurityDescriptorDACL failed: %w", err)
	}
	if !present {
		// No DACL present means no explicit ACL entries.
		return owner, group, []types.WinACL{}, nil
	}

	// Retrieve explicit ACE entries from the ACL.
	expEntries, err := GetExplicitEntriesFromACL(acl)
	if err != nil {
		return owner, group, []types.WinACL{}, nil
	}

	var winAcls []types.WinACL
	// Iterate over each explicit access entry.
	for _, entry := range expEntries {
		// In an absolute descriptor, Trustee.TrusteeValue is a pointer to a SID.
		pSid := (*windows.SID)(unsafe.Pointer(entry.Trustee.TrusteeValue))
		if pSid == nil {
			continue
		}
		sidStr := pSid.String()
		if sidStr == "" {
			return owner, group, nil, fmt.Errorf("failed to convert trustee SID to string: %w", err)
		}

		ace := types.WinACL{
			SID:        sidStr,
			AccessMask: uint32(entry.AccessPermissions),
			Type:       uint8(entry.AccessMode),
			Flags:      uint8(entry.Inheritance),
		}
		winAcls = append(winAcls, ace)
	}
	return owner, group, winAcls, nil
}
