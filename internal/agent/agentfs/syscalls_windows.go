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
	modadvapi32          = windows.NewLazySystemDLL("advapi32.dll")
	modkernel32          = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpace = modkernel32.NewProc("GetDiskFreeSpaceW")
	procGetSystemInfo    = modkernel32.NewProc("GetSystemInfo")
	procGetSecurityInfo  = modadvapi32.NewProc("GetSecurityInfo")
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
	// Convert the path to a UTF16 pointer.
	fullPathPtr, err := syscall.UTF16PtrFromString(fullPath)
	if err != nil {
		return "", "", nil, err
	}

	// Open the file or folder. The FILE_FLAG_BACKUP_SEMANTICS flag is required
	// to open directories (and many reparse-point objects, as is the case with VSS).
	handle, err := windows.CreateFile(
		fullPathPtr,
		windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return "", "", nil, err
	}
	defer windows.CloseHandle(handle)

	var pOwner *windows.SID
	var pGroup *windows.SID
	var pDACL *windows.ACL
	var pSecurityDescriptor uintptr

	// Call the handle-based GetSecurityInfo.
	r, _, err := procGetSecurityInfo.Call(
		uintptr(handle),
		uintptr(windows.SE_FILE_OBJECT),
		uintptr(windows.OWNER_SECURITY_INFORMATION|
			windows.GROUP_SECURITY_INFORMATION|
			windows.DACL_SECURITY_INFORMATION),
		uintptr(unsafe.Pointer(&pOwner)),
		uintptr(unsafe.Pointer(&pGroup)),
		uintptr(unsafe.Pointer(&pDACL)),
		0,
		uintptr(unsafe.Pointer(&pSecurityDescriptor)),
	)
	if r != 0 {
		return "", "", nil, syscall.Errno(r)
	}

	// Convert the owner SID to a string.
	ownerStr := pOwner.String()

	// Convert the group SID to a string.
	groupStr := pGroup.String()

	winacls, err := convertDaclToWinACL(pDACL)
	if err != nil {
		return "", "", nil, err
	}

	if pSecurityDescriptor != 0 {
		syscall.LocalFree(syscall.Handle(pSecurityDescriptor))
	}

	return ownerStr, groupStr, winacls, nil
}

// AceHeader defines the standard Windows ACE header.
type AceHeader struct {
	AceType  byte
	AceFlags byte
	AceSize  uint16
}

// AccessAllowedAce represents an ACCESS_ALLOWED_ACE structure.
// The SID starts immediately after the Mask field.
type AccessAllowedAce struct {
	Header   AceHeader
	Mask     uint32
	SidStart uint32 // This is the start of a variable-length SID.
}

// AccessDeniedAce represents an ACCESS_DENIED_ACE structure.
type AccessDeniedAce struct {
	Header   AceHeader
	Mask     uint32
	SidStart uint32
}

// getAceFromACL returns a pointer to the ACE header for the given index.
// We start after the ACL header (whose size we compute via unsafe.Sizeof).
func getAceFromACL(acl *windows.ACL, index uint32) (*AceHeader, error) {
	if index >= uint32(acl.AceCount) {
		return nil, windows.ERROR_INVALID_PARAMETER
	}

	// Start offset: the ACL header size.
	var offset uintptr = unsafe.Sizeof(*acl)
	var ace *AceHeader

	// Walk to the ACE we need.
	for i := uint32(0); i < index; i++ {
		ace = (*AceHeader)(unsafe.Pointer(uintptr(unsafe.Pointer(acl)) + offset))
		// Increase the offset by the size of the current ACE.
		offset += uintptr(ace.AceSize)
	}

	ace = (*AceHeader)(unsafe.Pointer(uintptr(unsafe.Pointer(acl)) + offset))
	return ace, nil
}

// convertDaclToWinACL converts the raw DACL (a pointer to windows.ACL)
// into a slice of WinACL entries.
func convertDaclToWinACL(pDACL *windows.ACL) ([]types.WinACL, error) {
	var winacls []types.WinACL

	// AceCount indicates how many ACEs are in the DACL.
	count := int(pDACL.AceCount)
	for i := 0; i < count; i++ {
		aceHeader, err := getAceFromACL(pDACL, uint32(i))
		if err != nil {
			return nil, err
		}

		var sid *windows.SID
		var mask uint32

		// Depending on the ACE type, interpret the ACE accordingly.
		switch aceHeader.AceType {
		case windows.ACCESS_ALLOWED_ACE_TYPE:
			ace := (*AccessAllowedAce)(unsafe.Pointer(aceHeader))
			mask = ace.Mask
			// The variable-length SID begins at the address of SidStart.
			sid = (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		case windows.ACCESS_DENIED_ACE_TYPE:
			ace := (*AccessDeniedAce)(unsafe.Pointer(aceHeader))
			mask = ace.Mask
			sid = (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		default:
			// Skip ACE types that are not ACCESS_ALLOWED or ACCESS_DENIED.
			continue
		}

		sidStr := sid.String()

		winace := types.WinACL{
			SID:        sidStr,
			AccessMask: mask,
			Type:       aceHeader.AceType,
			Flags:      aceHeader.AceFlags,
		}
		winacls = append(winacls, winace)
	}
	return winacls, nil
}
