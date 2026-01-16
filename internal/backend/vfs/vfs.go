package vfs

import (
	"context"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/memlocal"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/puzpuzpuz/xsync/v4"
)

var keyPool = NewMemcachedKeyPool()

type VFSBase struct {
	Ctx      context.Context
	Cancel   context.CancelFunc
	Backup   types.Backup
	Fuse     *fuse.Server
	BasePath string

	Memcache *memcache.Client

	FileCount     *xsync.Counter
	FolderCount   *xsync.Counter
	TotalBytes    *xsync.Counter
	StatCacheHits *xsync.Counter

	lastAccessTime  int64
	lastBytesTime   int64
	lastFileCount   int64
	lastFolderCount int64
	lastTotalBytes  int64
}

func InjectBase(base VFSBase) *VFSBase {
	base.FileCount = xsync.NewCounter()
	base.FolderCount = xsync.NewCounter()
	base.TotalBytes = xsync.NewCounter()
	base.StatCacheHits = xsync.NewCounter()

	return &base
}

func (fs *VFSBase) GetCacheKey(prefix string, pathKey string) string {
	sb := keyPool.Get()
	sb.WriteString(prefix)
	sb.WriteString(memlocal.Key(pathKey))

	key := sb.String()
	keyPool.Put(sb)

	return key
}

func (fs *VFSBase) GetStats() VFSStats {
	currentTime := time.Now().UnixNano()

	currentFileCount := fs.FileCount.Value()
	currentFolderCount := fs.FolderCount.Value()
	totalAccessed := currentFileCount + currentFolderCount

	elapsed := float64(currentTime-fs.lastAccessTime) / 1e9
	var accessSpeed float64
	if elapsed > 0 && fs.lastAccessTime > 0 {
		accessDelta := (currentFileCount + currentFolderCount) - (fs.lastFileCount + fs.lastFolderCount)
		accessSpeed = float64(accessDelta) / elapsed
	}

	fs.lastAccessTime = currentTime
	fs.lastFileCount = currentFileCount
	fs.lastFolderCount = currentFolderCount

	currentTotalBytes := fs.TotalBytes.Value()
	secDiff := float64(currentTime-fs.lastBytesTime) / 1e9
	var bytesSpeed float64
	if secDiff > 0 && fs.lastBytesTime > 0 {
		bytesSpeed = float64(currentTotalBytes-fs.lastTotalBytes) / secDiff
	}

	fs.lastBytesTime = currentTime
	fs.lastTotalBytes = currentTotalBytes

	return VFSStats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      uint64(currentTotalBytes),
		ByteReadSpeed:   bytesSpeed,
		StatCacheHits:   fs.StatCacheHits.Value(),
	}
}

type VFSStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}
