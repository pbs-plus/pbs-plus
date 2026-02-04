//go:build unix

package agentfs

import (
	"io"
	"os"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/utils/pathjoin"
	"golang.org/x/sys/unix"
)

func (s *AgentFSServer) abs(filename string) string {
	if filename == "" || filename == "." || filename == "/" {
		return s.snapshot.Path
	}
	return pathjoin.Join(s.snapshot.Path, filename)
}

func (s *AgentFSServer) platformOpen(path string) (*FileHandle, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	isDir := (st.Mode & unix.S_IFMT) == unix.S_IFDIR
	f := os.NewFile(uintptr(fd), path)
	fh := NewFileHandle(f)
	fh.fileSize = st.Size
	fh.isDir = isDir
	if isDir {
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
	var st unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, path, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return types.AgentFileInfo{}, err
	}
	bs := s.statFs.Bsize
	if bs == 0 {
		bs = 4096
	}
	isDir := (st.Mode & unix.S_IFMT) == unix.S_IFDIR
	var blocks uint64
	if !isDir {
		blocks = uint64((st.Size + int64(bs) - 1) / int64(bs))
	}
	return types.AgentFileInfo{
		Name:    lastPathElem(path),
		Size:    st.Size,
		Mode:    uint32(modeFromUnix(uint32(st.Mode))),
		ModTime: time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)).UnixNano(),
		IsDir:   isDir,
		Blocks:  blocks,
	}, nil
}

func (s *AgentFSServer) platformXstat(path string, aclOnly bool) (types.AgentFileInfo, error) {
	acls, err := GetUnixACLs(path, -1)
	if err != nil {
		return types.AgentFileInfo{}, err
	}

	info := types.AgentFileInfo{
		PosixACLs: acls,
	}

	if !aclOnly {
		var st unix.Stat_t
		if err := unix.Fstatat(unix.AT_FDCWD, path, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return types.AgentFileInfo{}, err
		}
		info.Owner = strconv.FormatUint(uint64(st.Uid), 10)
		info.Group = strconv.FormatUint(uint64(st.Gid), 10)
		info.CreationTime = time.Unix(int64(st.Ctim.Sec), int64(st.Ctim.Nsec)).Unix()
		info.LastAccessTime = time.Unix(int64(st.Atim.Sec), int64(st.Atim.Nsec)).Unix()
		info.LastWriteTime = time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec)).Unix()
	}

	return info, nil
}

func (s *AgentFSServer) platformPread(f *os.File, b []byte, off int64) (int, error) {
	return unix.Pread(int(f.Fd()), b, off)
}

func (s *AgentFSServer) platformMmap(fh *FileHandle, off int64, length int) ([]byte, func(), bool) {
	aligned := off - (off % int64(s.allocGranularity))
	diff := int(off - aligned)
	size := length + diff
	data, err := unix.Mmap(int(fh.file.Fd()), aligned, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, false
	}
	return data[diff : diff+length], func() { unix.Munmap(data) }, true
}

func (s *AgentFSServer) platformLseek(fh *FileHandle, off int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = off
	case io.SeekCurrent:
		next = fh.logicalOffset + off
	case io.SeekEnd:
		next = fh.fileSize + off
	default:
		return 0, os.ErrInvalid
	}
	if next < 0 || next > fh.fileSize {
		return 0, os.ErrInvalid
	}
	return next, nil
}

func (s *AgentFSServer) platformCloseResources(fh *FileHandle) {}

func (s *AgentFSServer) initializeStatFS() error {
	if s.snapshot.SourcePath == "" {
		return nil
	}
	var st unix.Statfs_t
	if err := unix.Statfs(s.snapshot.SourcePath, &st); err != nil {
		return err
	}
	s.statFs = types.StatFS{
		Bsize:   uint64(st.Bsize),
		Blocks:  uint64(st.Blocks),
		Bfree:   uint64(st.Bfree),
		Bavail:  uint64(st.Bavail),
		Files:   uint64(st.Files),
		Ffree:   uint64(st.Ffree),
		NameLen: getNameLenPlatform(st),
	}
	return nil
}
