//go:build windows

package vssfs

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// FileStandardInfo structure for GetFileInformationByHandleEx
type FileStandardInfo struct {
	AllocationSize int64 // Allocated size in bytes
	EndOfFile      int64 // Logical size in bytes
	NumberOfLinks  uint32
	DeletePending  uint32
	Directory      uint32
}

var (
	modkernel32                      = windows.NewLazySystemDLL("kernel32.dll")
	procGetFileInformationByHandleEx = modkernel32.NewProc("GetFileInformationByHandleEx")
	procGetDiskFreeSpace             = modkernel32.NewProc("GetDiskFreeSpaceW")
)

// Constants for GetFileInformationByHandleEx
const (
	FileStandardInfoClass = 1
)

// Wrapper for GetFileInformationByHandleEx
func getFileStandardInfo(handle windows.Handle) (*FileStandardInfo, error) {
	var info FileStandardInfo
	r1, _, e1 := syscall.SyscallN(
		procGetFileInformationByHandleEx.Addr(),
		3,
		uintptr(handle),
		uintptr(FileStandardInfoClass),
		uintptr(unsafe.Pointer(&info)),
	)
	if r1 == 0 {
		return nil, e1
	}
	return &info, nil
}

func getStatFS(driveLetter string) (*StatFS, error) {
	driveLetter = strings.TrimSpace(driveLetter)
	driveLetter = strings.ToUpper(driveLetter)

	if len(driveLetter) == 1 {
		driveLetter += ":"
	}

	if len(driveLetter) != 2 || driveLetter[1] != ':' {
		return nil, fmt.Errorf("invalid drive letter format: %s", driveLetter)
	}

	path := driveLetter + `\`

	var sectorsPerCluster, bytesPerSector, freeClusters, totalClusters uint32

	r1, _, err := syscall.SyscallN(
		procGetDiskFreeSpace.Addr(),
		5,
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&freeClusters)),
		uintptr(unsafe.Pointer(&totalClusters)),
		0,
	)
	if r1 == 0 {
		return nil, err
	}

	blockSize := uint64(sectorsPerCluster) * uint64(bytesPerSector)
	totalBlocks := uint64(totalClusters)

	stat := &StatFS{
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

const (
	SeekData = 3 // SEEK_DATA: seek to the next data
	SeekHole = 4 // SEEK_HOLE: seek to the next hole
)

var bufferSizes = []int{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}

var rangeBufferPools = make([]sync.Pool, len(bufferSizes))

func init() {
	for i, size := range bufferSizes {
		size := size // Capture for closure
		rangeBufferPools[i] = sync.Pool{
			New: func() interface{} {
				return make([]FileAllocatedRangeBuffer, size)
			},
		}
	}
}

func queryAllocatedRanges(handle windows.Handle, fileSize int64) ([]FileAllocatedRangeBuffer, error) {
	// First, verify the file is actually sparse
	var fileInfo windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return nil, err
	}

	// Check if the file is marked as sparse
	if fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_SPARSE_FILE == 0 {
		// File is not sparse, return single range
		result := make([]FileAllocatedRangeBuffer, 1)
		result[0] = FileAllocatedRangeBuffer{FileOffset: 0, Length: fileSize}
		return result, nil
	}

	var inputRange FileAllocatedRangeBuffer
	inputRange.FileOffset = 0
	inputRange.Length = fileSize

	// Try with a reasonably large initial buffer
	outputBuffer := make([]FileAllocatedRangeBuffer, 64)
	var bytesReturned uint32

	err = windows.DeviceIoControl(
		handle,
		windows.FSCTL_QUERY_ALLOCATED_RANGES,
		(*byte)(unsafe.Pointer(&inputRange)),
		uint32(unsafe.Sizeof(inputRange)),
		(*byte)(unsafe.Pointer(&outputBuffer[0])),
		uint32(len(outputBuffer)*int(unsafe.Sizeof(FileAllocatedRangeBuffer{}))),
		&bytesReturned,
		nil,
	)

	if err != nil {
		if err == windows.ERROR_INVALID_FUNCTION {
			// Filesystem doesn't support FSCTL_QUERY_ALLOCATED_RANGES
			// but file is marked sparse - try alternative method
			return queryAllocatedRangesAlternative(handle, fileSize)
		}
		return nil, err
	}

	// Calculate how many entries were returned
	count := int(bytesReturned) / int(unsafe.Sizeof(FileAllocatedRangeBuffer{}))

	// Create result slice with exact size
	result := make([]FileAllocatedRangeBuffer, count)
	copy(result, outputBuffer[:count])

	return result, nil
}

func queryAllocatedRangesAlternative(handle windows.Handle, fileSize int64) ([]FileAllocatedRangeBuffer, error) {
	// Alternative method: read the file in chunks and look for non-zero regions
	const chunkSize = 64 * 1024 // 64KB chunks
	buffer := make([]byte, chunkSize)
	var ranges []FileAllocatedRangeBuffer
	var currentRange *FileAllocatedRangeBuffer

	for offset := int64(0); offset < fileSize; offset += chunkSize {
		// Read chunk
		var bytesRead uint32
		err := windows.ReadFile(handle, buffer, &bytesRead, nil)
		if err != nil {
			return nil, err
		}

		// Check if chunk contains non-zero data
		hasData := false
		for i := uint32(0); i < bytesRead; i++ {
			if buffer[i] != 0 {
				hasData = true
				break
			}
		}

		if hasData {
			if currentRange == nil {
				// Start new range
				currentRange = &FileAllocatedRangeBuffer{
					FileOffset: offset,
					Length:     int64(bytesRead),
				}
				ranges = append(ranges, *currentRange)
			} else {
				// Extend current range
				currentRange.Length += int64(bytesRead)
			}
		} else if currentRange != nil {
			// End current range
			currentRange = nil
		}
	}

	return ranges, nil
}

func getFileSize(handle windows.Handle) (int64, error) {
	var fileInfo windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return 0, err
	}

	// Combine the high and low parts of the file size
	return int64(fileInfo.FileSizeHigh)<<32 + int64(fileInfo.FileSizeLow), nil
}
