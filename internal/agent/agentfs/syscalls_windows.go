//go:build windows

package agentfs

import (
	"fmt"
	"io"
	"os"
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

func openForAttrs(path string) (windows.Handle, error) {
	pathUTF16 := utf16.Encode([]rune(path))
	if len(pathUTF16) == 0 || pathUTF16[len(pathUTF16)-1] != 0 {
		pathUTF16 = append(pathUTF16, 0)
	}
	return windows.CreateFile(
		&pathUTF16[0],
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

func readAtOverlapped(h windows.Handle, off int64, buf []byte) (int, error) {
	var ov windows.Overlapped
	ov.Offset = uint32(off & 0xffffffff)
	ov.OffsetHigh = uint32(uint64(off) >> 32)

	evt, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(evt)
	ov.HEvent = evt

	var n uint32
	err = windows.ReadFile(h, buf, &n, &ov)
	if err != nil {
		if err == windows.ERROR_IO_PENDING {
			_, werr := windows.WaitForSingleObject(evt, windows.INFINITE)
			if werr != nil {
				return int(n), werr
			}
			if gerr := windows.GetOverlappedResult(h, &ov, &n, false); gerr != nil {
				// If EOF, return what we got (may be zero) as EOF.
				if gerr == windows.ERROR_HANDLE_EOF {
					return int(n), io.EOF
				}
				return int(n), gerr
			}
		} else if err == windows.ERROR_HANDLE_EOF {
			return int(n), io.EOF
		} else {
			return int(n), err
		}
	}
	return int(n), nil
}

func queryAllocatedRanges(h windows.Handle, off, length int64) ([]allocatedRange, error) {
	in := allocatedRange{FileOffset: off, Length: length}

	// Start with a reasonable capacity.
	out := make([]allocatedRange, 64)

	call := func(dst []allocatedRange) (int, error) {
		var br uint32
		err := windows.DeviceIoControl(
			h,
			windows.FSCTL_QUERY_ALLOCATED_RANGES,
			(*byte)(unsafe.Pointer(&in)),
			uint32(unsafe.Sizeof(in)),
			(*byte)(unsafe.Pointer(&dst[0])),
			uint32(len(dst))*uint32(unsafe.Sizeof(dst[0])),
			&br,
			nil,
		)
		if err != nil {
			// ERROR_INSUFFICIENT_BUFFER is not typical here; ERROR_MORE_DATA can occur.
			if err == windows.ERROR_MORE_DATA {
				return int(br), err
			}
			return 0, err
		}
		return int(br), nil
	}

	br, err := call(out)
	if err == windows.ERROR_MORE_DATA {
		need := br / int(unsafe.Sizeof(out[0]))
		if need <= len(out) {
			need = len(out) * 2
		}
		out = make([]allocatedRange, need)
		// Second attempt
		br, err = call(out)
	}
	if err != nil {
		return nil, err
	}

	count := br / int(unsafe.Sizeof(out[0]))
	res := make([]allocatedRange, 0, count)
	for i := 0; i < count; i++ {
		res = append(res, allocatedRange{
			FileOffset: out[i].FileOffset,
			Length:     out[i].Length,
		})
	}
	return res, nil
}
