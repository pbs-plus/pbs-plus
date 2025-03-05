//go:build windows

package vssfs

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
	"golang.org/x/sys/windows"
)

var (
	modkernel32          = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpace = modkernel32.NewProc("GetDiskFreeSpaceW")
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

const (
	SeekData = 3 // SEEK_DATA: seek to the next data
	SeekHole = 4 // SEEK_HOLE: seek to the next hole
)

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
		return 0, err
	}

	// Combine the high and low parts of the file size
	return int64(fileInfo.FileSizeHigh)<<32 + int64(fileInfo.FileSizeLow), nil
}
