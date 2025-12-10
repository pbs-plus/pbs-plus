package vfs

import (
	"context"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	agenttypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

type DirStream interface {
	HasNext() bool
	Next() (fuse.DirEntry, syscall.Errno)
	Close()
}

type FileHandle interface {
	ReadAt(p []byte, off int64) (int, error)
	Close() error
	Lseek(off int64, whence int) (uint64, error)
}

type FS interface {
	Root() string
	Mount(mountpoint string) error
	Unmount()
	Open(path string) (FileHandle, error)
	OpenFile(path string, flags int, perm os.FileMode) (FileHandle, error)
	Attr(path string, isLookup bool) (agenttypes.AgentFileInfo, error)
	Xattr(path string) (agenttypes.AgentFileInfo, error)
	ReadDir(path string) (DirStream, error)
	StatFS() (agenttypes.StatFS, error)
	Context() context.Context
}
