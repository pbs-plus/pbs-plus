//go:build linux

package s3fs

import (
	"context"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/minio/minio-go/v7"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
)

type S3FS struct {
	ctx      context.Context
	cancel   context.CancelFunc
	client   *minio.Client
	Job      storeTypes.Job
	bucket   string
	prefix   string
	Fuse     *fuse.Server
	basePath string

	memcache *memcache.Client

	fileCount       int64
	folderCount     int64
	totalBytes      int64
	lastAccessTime  int64
	lastFileCount   int64
	lastFolderCount int64
	lastBytesTime   int64
	lastTotalBytes  int64

	statCacheHits int64
}

type S3File struct {
	fs     *S3FS
	key    string
	offset int64
	size   int64
}
