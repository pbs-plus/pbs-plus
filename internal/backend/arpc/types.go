package arpcfs

import (
	"context"
	"sync/atomic"

	"github.com/alphadose/haxmap"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
)

// ARPCFS implements billy.Filesystem using aRPC calls
type ARPCFS struct {
	ctx      context.Context
	session  *arpc.Session
	JobId    string
	Hostname string
	Mount    *gofuse.Server
	basePath string

	accessedPaths *haxmap.Map[string, bool]

	// Atomic counters for the number of unique file and folder accesses.
	fileCount   int64
	folderCount int64
	totalBytes  int64

	lastAccessTime  int64 // UnixNano timestamp
	lastFileCount   int64
	lastFolderCount int64

	lastBytesTime  int64 // UnixNano timestamp
	lastTotalBytes int64
}

type Stats struct {
	FilesAccessed   int64   // Unique file count
	FoldersAccessed int64   // Unique folder count
	TotalAccessed   int64   // Sum of unique file and folder counts
	FileAccessSpeed float64 // (Unique accesses per second)
	TotalBytes      uint64  // Total bytes read
	ByteReadSpeed   float64 // (Bytes read per second)
}

// ARPCFile implements billy.File for remote files
type ARPCFile struct {
	fs       *ARPCFS
	name     string
	offset   int64
	handleID vssfs.FileHandleId
	isClosed atomic.Bool
	jobId    string
}
