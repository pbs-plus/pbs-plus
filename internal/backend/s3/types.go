package s3fs

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/minio/minio-go/v7"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
)

type S3FS struct {
	ctx    context.Context
	client *minio.Client
	Job    storeTypes.Job
	bucket string
	prefix string
	Mount  *fuse.Server

	metaCache *lru.Cache[string, cacheEntry[agentTypes.AgentFileInfo]]
	dirCache  *lru.Cache[string, cacheEntry[agentTypes.ReadDirEntries]]

	fileCount       int64
	folderCount     int64
	totalBytes      int64
	lastAccessTime  int64
	lastFileCount   int64
	lastFolderCount int64
	lastBytesTime   int64
	lastTotalBytes  int64
}

type S3File struct {
	fs     *S3FS
	key    string
	offset int64
	size   int64
}

type Stats struct {
	FilesAccessed   int64   // Unique file count
	FoldersAccessed int64   // Unique folder count
	TotalAccessed   int64   // Sum of unique file and folder counts
	FileAccessSpeed float64 // (Unique accesses per second)
	TotalBytes      uint64  // Total bytes read
	ByteReadSpeed   float64 // (Bytes read per second)
}
