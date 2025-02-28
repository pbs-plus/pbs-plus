//go:generate msgp
//msgp:ignore ARPCFS Stats ARPCFile

package arpcfs

import (
	"context"
	"os"
	"sync"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/types"
)

// Cache entry types.
type statCacheEntry struct {
	info os.FileInfo
}

type readDirCacheEntry struct {
	entries []os.FileInfo
}

type statFSCacheEntry struct {
	stat types.StatFS
}

// ARPCFS implements billy.Filesystem using aRPC calls
type ARPCFS struct {
	ctx      context.Context
	session  *arpc.Session
	JobId    string
	Hostname string
	Mount    *gofuse.Server
	basePath string

	accessedPaths sync.Map
	readdirOnce   sync.Map

	// Atomic counters for the number of unique file and folder accesses.
	fileCount   int64
	folderCount int64

	// For speed calculations we store the last seen state.
	lastAccessMu    sync.Mutex
	lastAccessTime  time.Time
	lastFileCount   int64
	lastFolderCount int64

	// Total bytes read and its speed metric.
	totalBytes     uint64
	totalBytesMu   sync.Mutex
	lastTotalBytes uint64
	lastBytesTime  time.Time

	// (Retain the caches below as-is, if needed.)
	statCache    *lru.Cache[string, statCacheEntry]
	readDirCache *lru.Cache[string, readDirCacheEntry]
	statFSCache  *lru.Cache[string, statFSCacheEntry]

	statCacheMu    *ShardedRWMutex
	readDirCacheMu *ShardedRWMutex
	statFSCacheMu  *ShardedRWMutex
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
	handleID int
	isClosed bool
	jobId    string
}

type FileInfoResponse struct {
	Name    string    `msg:"name"`
	Size    int64     `msg:"size"`
	Mode    uint32    `msg:"mode"`
	ModTime time.Time `msg:"modTime"`
	IsDir   bool      `msg:"isDir"`
}

// ReadDirResponse represents server's directory listing
type ReadDirResponse struct {
	Entries []FileInfoResponse `msg:"entries"`
}

// OpenRequest represents OpenFile request payload
type OpenRequest struct {
	Path string `msg:"path"`
	Flag int    `msg:"flag"`
	Perm int    `msg:"perm"`
}

// ReadRequest represents Read request payload
type ReadRequest struct {
	HandleID int   `msg:"handleID"`
	Offset   int64 `msg:"offset"`
	Length   int   `msg:"length"`
}

// ReadResponse represents Read response
type ReadResponse struct {
	Data []byte `msg:"data"`
	EOF  bool   `msg:"eof"`
}

type ReadDirRequest struct {
	Path string `msg:"path"`
}

type StatRequest struct {
	Path string `msg:"path"`
}

type CloseRequest struct {
	HandleID int `msg:"handleID"`
}

type SeekRequest struct {
	HandleID int `msg:"handleID"`
}
