//go:build windows

package agentfs

import (
	"fmt"
	"io"
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

type allocatedRange struct {
	FileOffset int64
	Length     int64
}

func queryAllocatedRanges(h windows.Handle, off, length int64) ([]allocatedRange, error) {
	in := fileAllocatedRangeBuffer{FileOffset: off, Length: length}

	// Start with a reasonable capacity.
	out := make([]fileAllocatedRangeBuffer, 64)
	var bytesReturned uint32

	call := func(dst []fileAllocatedRangeBuffer) (int, error) {
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
		out = make([]fileAllocatedRangeBuffer, need)
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
