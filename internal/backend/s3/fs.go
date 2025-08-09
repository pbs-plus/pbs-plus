//go:build linux

package s3fs

import (
	"context"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
)

// Configurable cache TTLs
const (
	metaCacheTTL    = 10 * time.Second // metadata cache
	dirCacheTTL     = 10 * time.Second // directory listing cache
	readAheadSize   = 2 * 1024 * 1024  // 2MB read-ahead
	maxCacheEntries = 1024
)

// cacheEntry wraps cached data with an expiration time
type cacheEntry[T any] struct {
	value     T
	expiresAt time.Time
}

func (e cacheEntry[T]) expired() bool {
	return time.Now().After(e.expiresAt)
}

// S3FS implements a read-only filesystem backed by an S3 bucket.
type S3FS struct {
	ctx    context.Context
	client *minio.Client
	Job    storeTypes.Job
	bucket string
	prefix string
	Mount  *gofuse.Server

	metaCache *lru.Cache[string, cacheEntry[types.AgentFileInfo]]
	dirCache  *lru.Cache[string, cacheEntry[types.ReadDirEntries]]

	fileCount       int64
	folderCount     int64
	totalBytes      int64
	lastAccessTime  int64
	lastFileCount   int64
	lastFolderCount int64
	lastBytesTime   int64
	lastTotalBytes  int64
}

// NewS3FS constructs an S3FS with caching.
func NewS3FS(
	ctx context.Context,
	endpoint, accessKey, secretKey, bucket, prefix string,
	useSSL bool,
) (*S3FS, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	metaCache, _ := lru.New[string, cacheEntry[types.AgentFileInfo]](maxCacheEntries)
	dirCache, _ := lru.New[string, cacheEntry[types.ReadDirEntries]](maxCacheEntries)

	return &S3FS{
		ctx:       ctx,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		metaCache: metaCache,
		dirCache:  dirCache,
	}, nil
}

// fullKey maps a fuse path "/foo/bar" → "<prefix>foo/bar"
func (fs *S3FS) fullKey(fpath string) string {
	p := strings.TrimPrefix(path.Clean(fpath), "/")
	return fs.prefix + p
}

// Attr implements types.AgentFileInfo lookup with caching and merged detection.
func (fs *S3FS) Attr(fpath string) (types.AgentFileInfo, error) {
	key := fs.fullKey(fpath)

	// Check cache
	if val, ok := fs.metaCache.Get(key); ok && !val.expired() {
		return val.value, nil
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 5*time.Second)
	defer cancel()

	// One call to detect both file and directory
	lo := minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: false,
		MaxKeys:   2,
	}

	var fileInfo *types.AgentFileInfo
	var isDir bool

	for obj := range fs.client.ListObjects(ctx, fs.bucket, lo) {
		if obj.Err != nil {
			return types.AgentFileInfo{}, obj.Err
		}
		if obj.Key == key {
			// It's a file
			mod := obj.LastModified.Unix()
			blocks := uint64((obj.Size + 511) / 512)
			fi := types.AgentFileInfo{
				IsDir:          false,
				Mode:           0644,
				Size:           obj.Size,
				Blocks:         blocks,
				CreationTime:   mod,
				LastAccessTime: mod,
				LastWriteTime:  mod,
			}
			fileInfo = &fi
		} else {
			isDir = true
		}
	}

	var result types.AgentFileInfo
	if isDir && fileInfo == nil {
		now := time.Now().Unix()
		result = types.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}
		atomic.AddInt64(&fs.folderCount, 1)
	} else if fileInfo != nil {
		result = *fileInfo
		atomic.AddInt64(&fs.fileCount, 1)
	} else {
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	fs.metaCache.Add(key, cacheEntry[types.AgentFileInfo]{value: result, expiresAt: time.Now().Add(metaCacheTTL)})
	return result, nil
}

func (fs *S3FS) Xattr(filename string) (types.AgentFileInfo, error) {
	return types.AgentFileInfo{}, syscall.ENODATA
}

func (fs *S3FS) StatFS() (types.StatFS, error) {
	return types.StatFS{
		Bsize:   4096,    // Block size
		Blocks:  1 << 50, // Pretend we have a huge number of blocks
		Bfree:   1 << 49, // Half "free"
		Bavail:  1 << 49, // Available to unprivileged users
		Files:   1 << 40, // Large number of files
		Ffree:   1 << 39, // Many free file slots
		NameLen: 1024,    // Max filename length
	}, nil
}

// ReadDir returns a cached snapshot of the directory.
func (fs *S3FS) ReadDir(fpath string) (S3DirStream, error) {
	key := fs.fullKey(fpath)
	prefix := key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// Check cache
	if val, ok := fs.dirCache.Get(prefix); ok && !val.expired() {
		return S3DirStream{entries: val.value}, nil
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 10*time.Second)
	defer cancel()

	lo := minio.ListObjectsOptions{Prefix: prefix, Recursive: false}
	stream := fs.client.ListObjects(ctx, fs.bucket, lo)

	entries := make(types.ReadDirEntries, 0)
	seenNames := make(map[string]bool)

	for obj := range stream {
		if obj.Err != nil {
			return S3DirStream{}, obj.Err
		}
		name := strings.TrimPrefix(obj.Key, prefix)
		if name == "" {
			continue
		}
		isDir := strings.HasSuffix(name, "/")
		if isDir {
			name = strings.TrimSuffix(name, "/")
		}
		if seenNames[name] {
			continue
		}
		seenNames[name] = true

		var mode uint32
		if isDir {
			mode = uint32(os.ModeDir | 0555)
		} else {
			mode = 0644
		}
		entries = append(entries, types.AgentDirEntry{Name: name, Mode: mode})
	}

	fs.dirCache.Add(prefix, cacheEntry[types.ReadDirEntries]{value: entries, expiresAt: time.Now().Add(dirCacheTTL)})
	return S3DirStream{entries: entries}, nil
}

// OpenFile only allows read-only
func (fs *S3FS) OpenFile(fpath string, flag int, _ os.FileMode) (S3File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return S3File{}, syscall.EROFS
	}
	key := fs.fullKey(fpath)
	// Skip StatObject — rely on Attr() cache
	if _, err := fs.Attr(fpath); err != nil {
		return S3File{}, err
	}
	return S3File{fs: fs, key: key}, nil
}
