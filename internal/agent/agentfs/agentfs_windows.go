//go:build windows

package agentfs

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"golang.org/x/sys/windows"
)

func (s *AgentFSServer) abs(filename string) string {
	path := pathjoin.Join(s.snapshot.Path, filename)
	if strings.HasPrefix(path, `\\?\`) || strings.HasPrefix(path, `\??\`) {
		return path
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}
	if len(absPath) >= 2 && absPath[1] == ':' {
		return `\\?\` + absPath
	}
	if strings.HasPrefix(absPath, `\\`) {
		return `\\?\UNC\` + absPath[2:]
	}
	return `\\?\` + absPath
}

func (s *AgentFSServer) platformOpen(path string) (*FileHandle, error) {
	pathUTF16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	raw, err := windows.CreateFile(pathUTF16, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(raw), path)
	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(windows.Handle(f.Fd()), &std); err != nil {
		f.Close()
		return nil, err
	}
	fh := NewFileHandle(f)
	fh.fileSize = std.EndOfFile
	fh.isDir = std.Directory != 0
	if fh.isDir {
		fh.file.Close()
		fh.file = nil
		raw, err := windows.CreateFile(pathUTF16, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
		if err != nil {
			return nil, err
		}
		f := os.NewFile(uintptr(raw), path)
		reader, err := NewDirReader(f, path)
		if err != nil {
			f.Close()
			return nil, err
		}
		fh.dirReader = reader
	}
	return fh, nil
}

func (s *AgentFSServer) platformStat(path string) (types.AgentFileInfo, error) {
	h, err := openForAttrs(path)
	if err != nil {
		return types.AgentFileInfo{}, err
	}
	defer windows.CloseHandle(h)
	var nfo fileNetworkOpenInformation
	if err := ntQueryFileNetworkOpenInformation(h, &nfo); err != nil {
		return types.AgentFileInfo{}, err
	}
	bs := s.statFs.Bsize
	if bs == 0 {
		bs = 4096
	}
	alloc := nfo.AllocationSize
	if alloc < 0 {
		alloc = 0
	}
	return types.AgentFileInfo{
		Name: filepath.Base(filepath.Clean(path)),
		Size: nfo.EndOfFile,
		Mode: windowsFileModeFromHandle(h, nfo.FileAttributes),
		ModTime: filetimeToUnix(windows.Filetime{
			LowDateTime:  uint32(uint64(nfo.LastWriteTime) & 0xFFFFFFFF),
			HighDateTime: uint32(uint64(nfo.LastWriteTime) >> 32),
		}),
		IsDir:  (nfo.FileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) != 0,
		Blocks: uint64((alloc + int64(bs) - 1) / int64(bs)),
	}, nil
}

func (s *AgentFSServer) platformXstat(path string, aclOnly bool) (types.AgentFileInfo, error) {
	h, err := openForAttrs(path)
	if err != nil {
		return types.AgentFileInfo{}, err
	}
	defer windows.CloseHandle(h)
	info := types.AgentFileInfo{}
	if !aclOnly {
		var nfo fileNetworkOpenInformation
		if err := ntQueryFileNetworkOpenInformation(h, &nfo); err == nil {
			info.CreationTime = filetimeToUnix(windows.Filetime{
				LowDateTime:  uint32(uint64(nfo.CreationTime) & 0xFFFFFFFF),
				HighDateTime: uint32(uint64(nfo.CreationTime) >> 32),
			})
			info.LastAccessTime = filetimeToUnix(windows.Filetime{
				LowDateTime:  uint32(uint64(nfo.LastAccessTime) & 0xFFFFFFFF),
				HighDateTime: uint32(uint64(nfo.LastAccessTime) >> 32),
			})
			info.LastWriteTime = filetimeToUnix(windows.Filetime{
				LowDateTime:  uint32(uint64(nfo.LastWriteTime) & 0xFFFFFFFF),
				HighDateTime: uint32(uint64(nfo.LastWriteTime) >> 32),
			})
			info.FileAttributes = parseFileAttributes(nfo.FileAttributes)
		}
	}
	o, g, a, err := GetWinACLsHandle(h)
	if err == nil {
		info.Owner = o
		info.Group = g
		info.WinACLs = a
	}
	return info, nil
}

func (s *AgentFSServer) platformPread(f *os.File, b []byte, off int64) (int, error) {
	return f.ReadAt(b, off)
}

func (s *AgentFSServer) platformMmap(fh *FileHandle, off int64, length int) ([]byte, func(), bool) {
	h, err := windows.CreateFileMapping(windows.Handle(fh.file.Fd()), nil, windows.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return nil, nil, false
	}
	defer windows.CloseHandle(h)
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, uint32(off>>32), uint32(off), uintptr(length))
	if err != nil {
		return nil, nil, false
	}
	data := make([]byte, length)
	copy(data, unsafe.Slice((*byte)(unsafe.Pointer(addr)), length))
	windows.UnmapViewOfFile(addr)
	return data, func() {}, true
}

func (s *AgentFSServer) platformLseek(fh *FileHandle, off int64, whence int) (int64, error) {
	var std fileStandardInfo
	if err := getFileStandardInfoByHandle(windows.Handle(fh.file.Fd()), &std); err != nil {
		return 0, err
	}
	fSize := std.EndOfFile
	var next int64
	switch whence {
	case io.SeekStart:
		next = off
	case io.SeekCurrent:
		next = fh.logicalOffset + off
	case io.SeekEnd:
		next = fSize + off
	case SeekData, SeekHole:
		return sparseSeekAllocatedRanges(windows.Handle(fh.file.Fd()), off, whence, fSize)
	default:
		return 0, os.ErrInvalid
	}
	if next < 0 || next > fSize {
		return 0, os.ErrInvalid
	}
	return next, nil
}

func (s *AgentFSServer) platformCloseResources(fh *FileHandle) {
	if fh.mapping != 0 {
		windows.CloseHandle(windows.Handle(fh.mapping))
		fh.mapping = 0
	}
}

func (s *AgentFSServer) initializeStatFS() error {
	if s.snapshot.SourcePath == "" {
		return nil
	}
	drive := s.snapshot.SourcePath[:1]
	stat, err := getStatFS(drive)
	if err == nil {
		s.statFs = stat
	}
	return err
}
