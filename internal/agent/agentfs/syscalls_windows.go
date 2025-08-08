//go:build windows

package agentfs

import (
	"fmt"
	"strings"
	"unicode/utf16"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
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

	rootPath := utf16.Encode([]rune(path))
	if len(rootPath) == 0 || rootPath[len(rootPath)-1] != 0 {
		rootPath = append(rootPath, 0)
	}
	rootPathPtr := &rootPath[0]

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
func filetimeToUnix(ft windows.Filetime) int64 {
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

func GetWinACLs(filePath string) ([]byte, []byte, []types.WinACL, error) {
	secInfo := windows.OWNER_SECURITY_INFORMATION |
		windows.GROUP_SECURITY_INFORMATION |
		windows.DACL_SECURITY_INFORMATION

	secDesc, err := GetFileSecurityDescriptor(filePath, uint32(secInfo))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("GetFileSecurityDescriptor failed: %w", err)
	}

	// IsValidSecDescriptor is already called within GetFileSecurityDescriptor in the revised version.
	// If GetFileSecurityDescriptor succeeded, we assume it's valid for now.

	_, pDacl, _, pOwnerSid, pGroupSid, err := MakeAbsoluteSD(secDesc)
	if err != nil {
		// MakeAbsoluteSD might fail even if GetFileSecurityDescriptor succeeded.
		return nil, nil, nil, fmt.Errorf("MakeAbsoluteSD failed: %w", err)
	}

	// Use the SIDs returned by MakeAbsoluteSD directly if they are valid.
	var owner, group []byte
	if pOwnerSid != nil && pOwnerSid.IsValid() {
		var s *uint16
		err := windows.ConvertSidToStringSid(pOwnerSid, &s)
		if err == nil {
			defer windows.LocalFree((windows.Handle)(unsafe.Pointer(s)))
			owner = UTF16DecodeBytes((*[256]uint16)(unsafe.Pointer(s))[:])
		}
	} else {
		// Fallback or handle error if owner SID is expected but missing/invalid
		// For simplicity here, we proceed, but production code might error out.
		// Alternatively, call getOwnerGroupAbsolute as a fallback, but it might be redundant.
		// owner, group, err = getOwnerGroupAbsolute(absoluteSD)
		// if err != nil {
		// 	 return "", "", nil, fmt.Errorf("failed to extract owner/group: %w", err)
		// }
		return nil, nil, nil, fmt.Errorf("owner SID from MakeAbsoluteSD is nil or invalid")
	}

	if pGroupSid != nil && pGroupSid.IsValid() {
		var s *uint16
		err := windows.ConvertSidToStringSid(pGroupSid, &s)
		if err == nil {
			defer windows.LocalFree((windows.Handle)(unsafe.Pointer(s)))
			group = UTF16DecodeBytes((*[256]uint16)(unsafe.Pointer(s))[:])
		}
	} else {
		return owner, nil, nil, fmt.Errorf("group SID from MakeAbsoluteSD is nil or invalid")
	}

	// Check if a DACL is present (pDacl will be non-nil if MakeAbsoluteSD found one)
	if pDacl == nil {
		// No DACL present means no explicit ACL entries.
		return owner, group, []types.WinACL{}, nil
	}

	entriesPtr, entriesCount, err := GetExplicitEntriesFromACL(pDacl)
	if err != nil {
		// If GetExplicitEntriesFromACL fails, it might mean an empty but valid ACL,
		// or a real error. Treat failure as potentially no entries, but log/wrap error.
		return owner, group, []types.WinACL{}, fmt.Errorf("GetExplicitEntriesFromACL failed: %w", err)
	}
	if entriesPtr == 0 || entriesCount == 0 {
		// No entries found or pointer is null.
		return owner, group, []types.WinACL{}, nil
	}
	// Ensure the allocated memory is freed when the function returns.
	defer FreeExplicitEntries(entriesPtr)

	// Create a temporary slice header to access the Windows-allocated memory.
	// This is unsafe and the slice is only valid until FreeExplicitEntries is called.
	expEntries := unsafeEntriesToSlice(entriesPtr, entriesCount)

	winAcls := make([]types.WinACL, 0, entriesCount)
	for _, entry := range expEntries {
		// Trustee.TrusteeValue should point to a SID structure in this context.
		pSid := (*windows.SID)(unsafe.Pointer(entry.Trustee.TrusteeValue))
		if pSid == nil { // Check if the cast resulted in a nil pointer
			continue
		}

		if !pSid.IsValid() {
			// Log or handle invalid SID? Skipping for now.
			continue
		}

		sid := []byte{}
		var s *uint16
		err := windows.ConvertSidToStringSid(pSid, &s)
		if err == nil {
			defer windows.LocalFree((windows.Handle)(unsafe.Pointer(s)))
			sid = UTF16DecodeBytes((*[256]uint16)(unsafe.Pointer(s))[:])
		}

		ace := types.WinACL{
			SID:        sid,
			AccessMask: uint32(entry.AccessPermissions),
			Type:       uint8(entry.AccessMode),
			Flags:      uint8(entry.Inheritance),
		}
		winAcls = append(winAcls, ace)
	}

	return owner, group, winAcls, nil
}
