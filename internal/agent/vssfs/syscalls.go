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
	var inputRange FileAllocatedRangeBuffer
	inputRange.FileOffset = 0
	inputRange.Length = fileSize

	for i, bufferSize := range bufferSizes {
		bufferObj := rangeBufferPools[i].Get()
		outputRanges := bufferObj.([]FileAllocatedRangeBuffer)

		var bytesReturned uint32
		err := windows.DeviceIoControl(
			handle,
			windows.FSCTL_QUERY_ALLOCATED_RANGES,
			(*byte)(unsafe.Pointer(&inputRange)),
			uint32(unsafe.Sizeof(inputRange)),
			(*byte)(unsafe.Pointer(&outputRanges[0])),
			uint32(bufferSize*int(unsafe.Sizeof(FileAllocatedRangeBuffer{}))),
			&bytesReturned,
			nil,
		)

		if err != nil {
			// Return buffer to the pool before handling errors
			rangeBufferPools[i].Put(bufferObj)

			// If the file system doesn't support sparse files
			if err == windows.ERROR_INVALID_FUNCTION {
				// Return a single range covering the whole file
				result := make([]FileAllocatedRangeBuffer, 1)
				result[0] = FileAllocatedRangeBuffer{FileOffset: 0, Length: fileSize}
				return result, nil
			}

			// If buffer too small, try the next size
			if err == windows.ERROR_MORE_DATA && i < len(bufferSizes)-1 {
				continue
			}

			// Other errors or we've exhausted all buffer sizes
			return nil, err
		}

		// Calculate how many entries were returned
		rangeSize := int(unsafe.Sizeof(FileAllocatedRangeBuffer{}))
		count := int(bytesReturned) / rangeSize

		// If the buffer was exactly filled and we have larger sizes available, try a bigger one
		if count == bufferSize && i < len(bufferSizes)-1 {
			rangeBufferPools[i].Put(bufferObj)
			continue
		}

		// Create a new slice with just the results (we can't return the pooled slice)
		result := make([]FileAllocatedRangeBuffer, count)
		copy(result, outputRanges[:count])

		// Return the buffer to the pool
		rangeBufferPools[i].Put(bufferObj)

		return result, nil
	}

	// Should never reach here as we would have returned an error earlier
	return nil, fmt.Errorf("file too fragmented, exceeded maximum buffer size")
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
