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

	accessedFileHashes   map[uint64]struct{} // Set of unique file path hashes
	accessedFolderHashes map[uint64]struct{} // Set of unique folder path hashes
	accessStatsMu        sync.RWMutex        // Mutex to protect the hash maps and stats
	lastAccessTime       time.Time
	lastAccessStats      AccessStats
	fileSpeed            float64

	totalBytes     uint64
	totalBytesMu   sync.RWMutex
	lastTotalBytes uint64
	lastBytesTime  time.Time
	byteSpeed      float64

	statCache    *lru.Cache[string, statCacheEntry]
	readDirCache *lru.Cache[string, readDirCacheEntry]
	statFSCache  *lru.Cache[string, statFSCacheEntry]

	statCacheMu    *ShardedRWMutex
	readDirCacheMu *ShardedRWMutex
	statFSCacheMu  *ShardedRWMutex
}

type AccessStats struct {
	FilesAccessed   int // Number of unique files accessed
	FoldersAccessed int // Number of unique folders accessed
	TotalAccessed   int // Total number of unique paths accessed
}

// ARPCFile implements billy.File for remote files
type ARPCFile struct {
	fs       *ARPCFS
	name     string
	offset   int64
	handleID uint64
	isClosed bool
	jobId    string
}

// FileInfoResponse represents server's file info response
type FileInfoResponse struct {
	Name        string      `msgpack:"name"`
	Size        int64       `msgpack:"size"`
	Mode        os.FileMode `msgpack:"mode"`
	ModTimeUnix int64       `msgpack:"modTime"`
	IsDir       bool        `msgpack:"isDir"`
}

// ReadDirResponse represents server's directory listing
type ReadDirResponse struct {
	Entries []FileInfoResponse `msgpack:"entries"`
}

// OpenRequest represents OpenFile request payload
type OpenRequest struct {
	Path string `msgpack:"path"`
	Flag int    `msgpack:"flag"`
	Perm int    `msgpack:"perm"`
}

// ReadRequest represents Read request payload
type ReadRequest struct {
	HandleID uint64 `msgpack:"handleID"`
	Offset   int64  `msgpack:"offset"`
	Length   int    `msgpack:"length"`
}

// ReadResponse represents Read response
type ReadResponse struct {
	Data []byte `msgpack:"data"`
	EOF  bool   `msgpack:"eof"`
}
