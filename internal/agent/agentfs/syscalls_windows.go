//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

var (
	modkernel32          = windows.NewLazySystemDLL("kernel32.dll")
	procGetDiskFreeSpace = modkernel32.NewProc("GetDiskFreeSpaceW")
	procGetSystemInfo    = modkernel32.NewProc("GetSystemInfo")
)

func openForAttrs(path string) (windows.Handle, error) {
	syslog.L.Debug().WithMessage("openForAttrs: opening for attributes").WithField("path", path).Write()
	pathUTF16 := toUTF16Z(path, utf16PathBuf[:0])
	h, err := windows.CreateFile(
		&pathUTF16[0],
		windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if !strings.HasSuffix(path, ".pxarexclude") {
			syslog.L.Error(err).WithMessage("openForAttrs: CreateFile failed").WithField("path", path).Write()
		}
		return 0, err
	}
	syslog.L.Debug().WithMessage("openForAttrs: handle opened").WithField("path", path).WithField("handle", uintptr(h)).Write()
	return h, nil
}

func getStatFS(driveLetter string) (types.StatFS, error) {
	syslog.L.Debug().WithMessage("getStatFS: fetching filesystem stats").WithField("drive_letter_in", driveLetter).Write()
	driveLetter = strings.TrimSpace(driveLetter)
	driveLetter = strings.ToUpper(driveLetter)

	if len(driveLetter) == 1 {
		driveLetter += ":"
	}

	if len(driveLetter) != 2 || driveLetter[1] != ':' {
		err := fmt.Errorf("invalid drive letter format: %s", driveLetter)
		syslog.L.Error(err).WithMessage("getStatFS: invalid drive letter").WithField("drive_letter", driveLetter).Write()
		return types.StatFS{}, err
	}

	path := driveLetter + `\`

	var sectorsPerCluster, bytesPerSector, numberOfFreeClusters, totalNumberOfClusters uint32

	rootPath := toUTF16Z(path, utf16PathBuf[:0])
	rootPathPtr := &rootPath[0]

	syslog.L.Debug().WithMessage("getStatFS: calling GetDiskFreeSpaceW").WithField("path", path).Write()
	ret, _, err := procGetDiskFreeSpace.Call(
		uintptr(unsafe.Pointer(rootPathPtr)),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&numberOfFreeClusters)),
		uintptr(unsafe.Pointer(&totalNumberOfClusters)),
	)
	if ret == 0 {
		syslog.L.Error(err).WithMessage("getStatFS: GetDiskFreeSpaceW failed").WithField("path", path).Write()
		return types.StatFS{}, fmt.Errorf("GetDiskFreeSpaceW failed: %w", err)
	}

	blockSize := uint64(sectorsPerCluster) * uint64(bytesPerSector)
	totalBlocks := uint64(totalNumberOfClusters)

	stat := types.StatFS{
		Bsize:   blockSize,
		Blocks:  totalBlocks,
		Bfree:   0,
		Bavail:  0,
		Files:   uint64(1 << 20),
		Ffree:   0,
		NameLen: 255,
	}

	syslog.L.Debug().WithMessage("getStatFS: success").
		WithField("drive", driveLetter).
		WithField("block_size", blockSize).
		WithField("total_blocks", totalBlocks).
		Write()

	return stat, nil
}

func getAllocGranularity() int {
	var si systemInfo
	procGetSystemInfo.Call(uintptr(unsafe.Pointer(&si)))
	syslog.L.Debug().WithMessage("getAllocGranularity: fetched").WithField("granularity", int(si.AllocationGranularity)).Write()
	return int(si.AllocationGranularity)
}

func getFileStandardInfoByHandle(h windows.Handle, out *fileStandardInfo) error {
	const fileStandardInfoClass = 1
	size := uint32(unsafe.Sizeof(*out))
	syslog.L.Debug().WithMessage("getFileStandardInfoByHandle: querying").WithField("handle", uintptr(h)).WithField("size", size).Write()
	err := windows.GetFileInformationByHandleEx(h, fileStandardInfoClass, (*byte)(unsafe.Pointer(out)), size)
	if err != nil {
		syslog.L.Error(err).WithMessage("getFileStandardInfoByHandle: failed").WithField("handle", uintptr(h)).Write()
		return err
	}
	syslog.L.Debug().WithMessage("getFileStandardInfoByHandle: success").WithField("handle", uintptr(h)).Write()
	return nil
}

func sparseSeekAllocatedRanges(h windows.Handle, start int64, whence int, fileSize int64) (int64, error) {
	syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: begin").
		WithField("handle", uintptr(h)).
		WithField("start", start).
		WithField("whence", whence).
		WithField("file_size", fileSize).
		Write()

	if start < 0 {
		syslog.L.Warn().WithMessage("sparseSeekAllocatedRanges: negative start").WithField("start", start).Write()
		return 0, os.ErrInvalid
	}
	if start >= fileSize {
		syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: start beyond EOF").WithField("start", start).WithField("file_size", fileSize).Write()
		return fileSize, nil
	}

	in := allocatedRange{
		FileOffset: start,
		Length:     fileSize - start,
	}
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
		if err == windows.ERROR_INVALID_FUNCTION {
			if whence == SeekData {
				syslog.L.Warn().WithMessage("sparseSeekAllocatedRanges: FSCTL not supported, returning start for SEEK_DATA").Write()
				return start, nil
			}
			syslog.L.Warn().WithMessage("sparseSeekAllocatedRanges: FSCTL not supported, returning EOF for SEEK_HOLE").Write()
			return fileSize, nil
		}
		syslog.L.Error(err).WithMessage("sparseSeekAllocatedRanges: DeviceIoControl failed with no bytes").Write()
		return 0, err
	}

	count := int(bytesReturned) / int(unsafe.Sizeof(out[0]))
	if count == 0 {
		if whence == SeekData {
			syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: no ranges after start for SEEK_DATA").Write()
			return fileSize, windows.ERROR_NO_MORE_FILES
		}
		syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: no ranges after start for SEEK_HOLE, returning EOF").Write()
		return fileSize, nil
	}

	switch whence {
	case SeekData:
		for i := 0; i < count; i++ {
			r := out[i]
			if start < r.FileOffset {
				syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_DATA before next allocated range").WithField("offset", r.FileOffset).Write()
				return r.FileOffset, nil
			}
			if start >= r.FileOffset && start < r.FileOffset+r.Length {
				syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_DATA within allocated range, returning start").Write()
				return start, nil
			}
		}
		syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_DATA no further data").Write()
		return fileSize, windows.ERROR_NO_MORE_FILES
	case SeekHole:
		first := out[0]
		if start < first.FileOffset {
			syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_HOLE before first range").WithField("offset", start).Write()
			return start, nil
		}
		for i := 0; i < count; i++ {
			r := out[i]
			if start >= r.FileOffset && start < r.FileOffset+r.Length {
				syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_HOLE inside range").WithField("offset", r.FileOffset+r.Length).Write()
				return r.FileOffset + r.Length, nil
			}
			if i+1 < count {
				next := out[i+1]
				if start >= r.FileOffset+r.Length && start < next.FileOffset {
					syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_HOLE between ranges, returning start").WithField("offset", start).Write()
					return start, nil
				}
			}
		}
		last := out[count-1]
		if start >= last.FileOffset+last.Length {
			syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_HOLE after last range, returning start").WithField("offset", start).Write()
			return start, nil
		}
		syslog.L.Debug().WithMessage("sparseSeekAllocatedRanges: SEEK_HOLE at end of last range").WithField("offset", last.FileOffset+last.Length).Write()
		return last.FileOffset + last.Length, nil
	default:
		syslog.L.Warn().WithMessage("sparseSeekAllocatedRanges: invalid whence").WithField("whence", whence).Write()
		return 0, os.ErrInvalid
	}
}

func queryAllocatedRanges(h windows.Handle, off, length int64) ([]allocatedRange, error) {
	syslog.L.Debug().WithMessage("queryAllocatedRanges: begin").
		WithField("handle", uintptr(h)).
		WithField("offset", off).
		WithField("length", length).
		Write()

	in := allocatedRange{FileOffset: off, Length: length}
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
			if err == windows.ERROR_MORE_DATA {
				syslog.L.Debug().WithMessage("queryAllocatedRanges: more data available, resizing buffer").
					WithField("bytes_returned", br).
					Write()
				return int(br), err
			}
			syslog.L.Error(err).WithMessage("queryAllocatedRanges: DeviceIoControl failed").Write()
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
		syslog.L.Debug().WithMessage("queryAllocatedRanges: reallocating range buffer").WithField("new_capacity", need).Write()
		out = make([]allocatedRange, need)
		br, err = call(out)
	}
	if err != nil {
		syslog.L.Error(err).WithMessage("queryAllocatedRanges: failed after retry").Write()
		return nil, err
	}

	count := br / int(unsafe.Sizeof(out[0]))
	res := out[:count]
	syslog.L.Debug().WithMessage("queryAllocatedRanges: success").WithField("range_count", len(res)).Write()
	return res, nil
}

type overlappedHandle struct {
	h windows.Handle
	m sync.Mutex
	e []windows.Handle

	DefaultTimeout int
}

func (f *overlappedHandle) getEvent() windows.Handle {
	f.m.Lock()
	if len(f.e) == 0 {
		f.m.Unlock()
		e, err := windows.CreateEvent(nil, 0, 0, nil)
		if err != nil {
			syslog.L.Error(err).WithMessage("overlappedHandle.getEvent: CreateEvent failed").Write()
			panic(err)
		}
		syslog.L.Debug().WithMessage("overlappedHandle.getEvent: created new event").WithField("event", uintptr(e)).Write()
		return e
	}
	e := f.e[len(f.e)-1]
	f.e = f.e[:len(f.e)-1]
	f.m.Unlock()
	syslog.L.Debug().WithMessage("overlappedHandle.getEvent: reused event").WithField("event", uintptr(e)).Write()
	return e
}

func (f *overlappedHandle) putEvent(e windows.Handle) {
	windows.ResetEvent(e)
	f.m.Lock()
	f.e = append(f.e, e)
	f.m.Unlock()
	syslog.L.Debug().WithMessage("overlappedHandle.putEvent: returned event to pool").WithField("event", uintptr(e)).Write()
}

func (f *overlappedHandle) asyncIo(fn func(windows.Handle, []byte, *uint32, *windows.Overlapped) error, b []byte, milliseconds int, o *windows.Overlapped) (uint32, error) {
	var n uint32
	syslog.L.Debug().WithMessage("overlappedHandle.asyncIo: begin").
		WithField("handle", uintptr(f.h)).
		WithField("buf_len", len(b)).
		WithField("timeout_ms", milliseconds).
		Write()
	err := fn(f.h, b, &n, o)

	if err == windows.ERROR_IO_PENDING {
		if milliseconds >= 0 {
			if n, _ = windows.WaitForSingleObject(o.HEvent, uint32(milliseconds)); n != windows.WAIT_OBJECT_0 {
				switch n {
				case syscall.WAIT_ABANDONED:
					err = os.NewSyscallError("WaitForSingleObject", fmt.Errorf("WAIT_ABANDONED"))
				case syscall.WAIT_TIMEOUT:
					err = os.NewSyscallError("WaitForSingleObject", windows.WAIT_TIMEOUT)
				case syscall.WAIT_FAILED:
					err = os.NewSyscallError("WaitForSingleObject", fmt.Errorf("WAIT_FAILED"))
				default:
					err = os.NewSyscallError("WaitForSingleObject", fmt.Errorf("UNKNOWN ERROR"))
				}
				syslog.L.Error(err).WithMessage("overlappedHandle.asyncIo: wait failed").Write()
				return 0, err
			}
		}
		if err = windows.GetOverlappedResult(f.h, o, &n, true); err != nil {
			if err == windows.ERROR_HANDLE_EOF {
				syslog.L.Debug().WithMessage("overlappedHandle.asyncIo: EOF reached").Write()
				err = io.EOF
				return n, err
			}
			err2 := os.NewSyscallError("GetOverlappedResult", err)
			syslog.L.Error(err2).WithMessage("overlappedHandle.asyncIo: GetOverlappedResult failed").Write()
			return 0, err2
		}
	} else if err != nil {
		syslog.L.Error(err).WithMessage("overlappedHandle.asyncIo: I/O call failed immediately").Write()
		return 0, err
	}
	syslog.L.Debug().WithMessage("overlappedHandle.asyncIo: success").WithField("bytes", n).Write()
	return n, nil
}

func (f *overlappedHandle) ReadAt(b []byte, off int64) (int, error) {
	syslog.L.Debug().WithMessage("overlappedHandle.ReadAt: begin").
		WithField("handle", uintptr(f.h)).
		WithField("offset", off).
		WithField("len", len(b)).
		Write()
	o := &windows.Overlapped{}
	o.Offset = uint32(off)
	o.OffsetHigh = uint32(uint64(off) >> 32)
	e := f.getEvent()
	defer f.putEvent(e)
	o.HEvent = e

	n, err := f.asyncIo(windows.ReadFile, b, f.DefaultTimeout, o)
	err = os.NewSyscallError("readAt", err)
	if errors.Is(err, io.EOF) || (err == nil && n == 0 && len(b) > 0) || (err == nil && len(b) > int(n)) {
		syslog.L.Debug().WithMessage("overlappedHandle.ReadAt: EOF condition").WithField("bytes", n).Write()
		err = io.EOF
	}
	if err != nil && !errors.Is(err, io.EOF) {
		syslog.L.Error(err).WithMessage("overlappedHandle.ReadAt: read failed").WithField("bytes", n).Write()
	} else {
		syslog.L.Debug().WithMessage("overlappedHandle.ReadAt: success").WithField("bytes", n).Write()
	}
	return int(n), err
}

func (f *overlappedHandle) Close() error {
	syslog.L.Debug().WithMessage("overlappedHandle.Close: closing").WithField("handle", uintptr(f.h)).Write()
	windows.CancelIoEx(f.h, nil)
	windows.Close(f.h)
	f.h = 0
	for _, h := range f.e {
		windows.Close(h)
	}
	f.e = nil
	syslog.L.Debug().WithMessage("overlappedHandle.Close: closed").Write()
	return nil
}

func newOverlapped(h windows.Handle) *overlappedHandle {
	syslog.L.Debug().WithMessage("newOverlapped: creating overlapped handle").WithField("handle", uintptr(h)).Write()
	return &overlappedHandle{h: h, DefaultTimeout: -1}
}
