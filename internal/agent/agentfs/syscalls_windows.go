//go:build windows

package agentfs

import (
	"fmt"
	"os"
	"strings"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

var (
	modkernel32          = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpace = modkernel32.NewProc("GetDiskFreeSpaceW")
	procGetSystemInfo    = modkernel32.NewProc("GetSystemInfo")
)

func openForAttrs(path string) (windows.Handle, error) {
	pathUTF16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	return windows.CreateFile(
		pathUTF16,
		windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
}

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

	rootPath, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return types.StatFS{}, err
	}

	ret, _, err := procGetDiskFreeSpace.Call(
		uintptr(unsafe.Pointer(rootPath)),
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

func getAllocGranularity() int {
	var si systemInfo
	// this cannot fail
	procGetSystemInfo.Call(uintptr(unsafe.Pointer(&si)))
	return int(si.AllocationGranularity)
}

func getFileStandardInfoByHandle(h windows.Handle, out *fileStandardInfo) error {
	// FileInformationClass = FileStandardInfo (1)
	const fileStandardInfoClass = 1
	size := uint32(unsafe.Sizeof(*out))
	err := windows.GetFileInformationByHandleEx(h, fileStandardInfoClass, (*byte)(unsafe.Pointer(out)), size)
	return err
}

// Sparse SEEK_DATA/SEEK_HOLE using FSCTL_QUERY_ALLOCATED_RANGES
func sparseSeekAllocatedRanges(h windows.Handle, start int64, whence int, fileSize int64) (int64, error) {
	if start < 0 {
		return 0, os.ErrInvalid
	}
	if start >= fileSize {
		return fileSize, nil
	}

	in := allocatedRange{
		FileOffset: start,
		Length:     fileSize - start,
	}
	// buffer for a handful of ranges
	out := make([]allocatedRange, 32)
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
