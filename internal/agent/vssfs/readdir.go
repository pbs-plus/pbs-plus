//go:build windows

package vssfs

import (
	"errors"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// FILE_ID_BOTH_DIR_INFO remains the same as it needs to match Windows API
type FILE_ID_BOTH_DIR_INFO struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       uint64
	AllocationSize  uint64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	ShortNameLength uint32
	ShortName       [12]uint16
	FileID          uint64
	FileName        [1]uint16
}

// FILE_FULL_DIR_INFO remains the same as it needs to match Windows API
type FILE_FULL_DIR_INFO struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       uint64
	AllocationSize  uint64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	FileName        [1]uint16
}

// Constants for file attributes checking
const (
	initialBufSize  = 64 * 1024  // 64KB initial buffer on stack
	maxStackBufSize = 128 * 1024 // 128KB maximum stack buffer
	exclusions      = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
)

//go:noinline
func (s *VSSFSServer) readDirBulk(dirPath string) ([]byte, error) {
	var (
		entries   = make(ReadDirEntries, 0, 128)
		usingFull bool
		infoClass = windows.FileIdBothDirectoryInfo
	)

	pDir, err := windows.UTF16PtrFromString(dirPath)
	if err != nil {
		return nil, err
	}

	handle, err := windows.CreateFile(
		pDir,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(handle)

	// Start with stack buffer
	var stackBuf [initialBufSize]byte
	buffer := stackBuf[:]
	var heapBuf []byte

	for {
		err = windows.GetFileInformationByHandleEx(
			handle,
			uint32(infoClass),
			&buffer[0],
			uint32(len(buffer)),
		)

		// Keep buffer alive until Windows API call is complete
		if heapBuf != nil {
			runtime.KeepAlive(heapBuf)
		}

		if err != nil {
			if errno, ok := err.(syscall.Errno); ok {
				switch errno {
				case windows.ERROR_INVALID_PARAMETER:
					if !usingFull {
						infoClass = windows.FileFullDirectoryInfo
						usingFull = true
						continue
					}
					return nil, err
				case windows.ERROR_MORE_DATA:
					newSize := len(buffer) * 2
					if len(heapBuf) == 0 && newSize <= maxStackBufSize {
						buffer = stackBuf[:newSize]
					} else {
						heapBuf = make([]byte, newSize)
						buffer = heapBuf
					}
					continue
				case windows.ERROR_NO_MORE_FILES:
					return entries.MarshalMsg(nil)
				default:
					return nil, err
				}
			}
			return nil, err
		}

		entries, err = processBuffer(buffer, entries, usingFull)
		if err != nil {
			return nil, err
		}

		return entries.MarshalMsg(nil)
	}
}

func processBuffer(buffer []byte, entries ReadDirEntries, usingFull bool) (ReadDirEntries, error) {
	var offset uintptr
	for offset < uintptr(len(buffer)) {
		var info *FILE_ID_BOTH_DIR_INFO
		if usingFull {
			fullInfo := (*FILE_FULL_DIR_INFO)(unsafe.Pointer(&buffer[offset]))
			info = &FILE_ID_BOTH_DIR_INFO{
				NextEntryOffset: fullInfo.NextEntryOffset,
				FileIndex:       fullInfo.FileIndex,
				CreationTime:    fullInfo.CreationTime,
				LastAccessTime:  fullInfo.LastAccessTime,
				LastWriteTime:   fullInfo.LastWriteTime,
				ChangeTime:      fullInfo.ChangeTime,
				EndOfFile:       fullInfo.EndOfFile,
				AllocationSize:  fullInfo.AllocationSize,
				FileAttributes:  fullInfo.FileAttributes,
				FileNameLength:  fullInfo.FileNameLength,
				EaSize:          fullInfo.EaSize,
			}
			copy(info.FileName[:], fullInfo.FileName[:])
		} else {
			info = (*FILE_ID_BOTH_DIR_INFO)(unsafe.Pointer(&buffer[offset]))
		}

		nameLen := int(info.FileNameLength) / 2
		if nameLen > 0 {
			if offset+uintptr(info.FileNameLength) > uintptr(len(buffer)) {
				return nil, errors.New("buffer overflow while reading filename")
			}

			filenamePtr := (*uint16)(unsafe.Pointer(&info.FileName[0]))
			nameSlice := unsafe.Slice(filenamePtr, nameLen)
			name := syscall.UTF16ToString(nameSlice)

			if shouldIncludeEntry(name, info.FileAttributes) {
				entries = append(entries, VSSDirEntry{
					Name: name,
					Mode: windowsAttributesToFileMode(info.FileAttributes),
				})
			}
		}

		if info.NextEntryOffset == 0 {
			return entries, nil
		}
		offset += uintptr(info.NextEntryOffset)
	}
	return entries, nil
}

func shouldIncludeEntry(name string, attrs uint32) bool {
	if name == "." || name == ".." {
		return false
	}

	return attrs&(exclusions) == 0
}

func windowsAttributesToFileMode(attrs uint32) uint32 {
	mode := uint32(0644)
	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode = 0755 | uint32(os.ModeDir)
	}
	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		mode |= uint32(os.ModeSymlink)
	}
	if attrs&windows.FILE_ATTRIBUTE_DEVICE != 0 {
		mode |= uint32(os.ModeDevice)
	}
	return mode
}
