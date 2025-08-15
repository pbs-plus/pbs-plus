//go:build windows

package agentfs

import (
	"fmt"
	"os"
	"strings"
	"time"
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

func getFileStandardInfoByHandle(h windows.Handle, out *fileStandardInfo) error {
	// FileInformationClass = FileStandardInfo (1)
	const fileStandardInfoClass = 1
	size := uint32(unsafe.Sizeof(*out))
	err := windows.GetFileInformationByHandleEx(h, fileStandardInfoClass, (*byte)(unsafe.Pointer(out)), size)
	return err
}

func filetimeToTime(ft windows.Filetime) time.Time {
	// windows.NsecToFiletime is inverse; use Unix epoch conversion
	return time.Unix(0, ft.Nanoseconds())
}

func fileModeFromAttrs(attrs uint32, isDir bool) uint32 {
	mode := uint32(0)
	if isDir {
		mode |= uint32(os.ModeDir)
	}
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		mode |= uint32(os.ModeSymlink) // best-effort mapping
	}
	return mode
}

// Sparse SEEK_DATA/SEEK_HOLE using FSCTL_QUERY_ALLOCATED_RANGES
func sparseSeekAllocatedRanges(h windows.Handle, start int64, whence int, fileSize int64) (int64, error) {
	if start < 0 {
		return 0, os.ErrInvalid
	}
	if start >= fileSize {
		return fileSize, nil
	}

	in := fileAllocatedRangeBuffer{
		FileOffset: start,
		Length:     fileSize - start,
	}
	// buffer for a handful of ranges
	out := make([]fileAllocatedRangeBuffer, 32)
	var bytesReturned uint32

	err := windows.DeviceIoControl(
		h,
		windows.FSCTL_QUERY_ALLOCATED_RANGES,
		(*byte)(unsafe.Pointer(&in)),
		uint32(unsafe.Sizeof(in)),
		(*byte)(unsafe.Pointer(&out[0])),
		uint32(uintptr(len(out))*unsafe.Sizeof(out[0])),
		&bytesReturned,
		nil,
	)

	if err != nil && bytesReturned == 0 {
		// Filesystem might not support it
		if err == windows.ERROR_INVALID_FUNCTION {
			if whence == SeekData {
				return start, nil
			}
			return fileSize, nil
		}
		return 0, err
	}

	count := int(bytesReturned) / int(unsafe.Sizeof(out[0]))
	if count == 0 {
		// no allocated ranges after start
		if whence == SeekData {
			return fileSize, windows.ERROR_NO_MORE_FILES
		}
		return fileSize, nil
	}

	switch whence {
	case SeekData:
		// Find first range at or after start
		for i := 0; i < count; i++ {
			r := out[i]
			if start < r.FileOffset {
				return r.FileOffset, nil
			}
			if start >= r.FileOffset && start < r.FileOffset+r.Length {
				return start, nil
			}
		}
		return fileSize, windows.ERROR_NO_MORE_FILES
	case SeekHole:
		// If before first range, we're in a hole
		first := out[0]
		if start < first.FileOffset {
			return start, nil
		}
		// If inside a range, hole begins at end of that range
		for i := 0; i < count; i++ {
			r := out[i]
			if start >= r.FileOffset && start < r.FileOffset+r.Length {
				return r.FileOffset + r.Length, nil
			}
			// Between ranges is a hole; if start lies there, return start
			if i+1 < count {
				next := out[i+1]
				if start >= r.FileOffset+r.Length && start < next.FileOffset {
					return start, nil
				}
			}
		}
		// After last range is a hole to EOF
		last := out[count-1]
		if start >= last.FileOffset+last.Length {
			return start, nil
		}
		return last.FileOffset + last.Length, nil
	default:
		return 0, os.ErrInvalid
	}
}
