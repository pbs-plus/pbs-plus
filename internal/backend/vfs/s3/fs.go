//go:build linux

package s3fs

import (
	"context"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/puzpuzpuz/xsync/v4"
)

const (
	metaCacheTTL    = 60 * time.Second
	dirCacheTTL     = 60 * time.Second
	maxCacheEntries = 4096
	readAheadSize   = 1024 * 1024
)

type cacheEntry[T any] struct {
	value     *T
	expiresAt time.Time
}

func (e cacheEntry[T]) expired() bool {
	return time.Now().After(e.expiresAt)
}

var seenNamesPool = sync.Pool{
	New: func() any {
		m := make(map[string]bool, 128)
		return &m
	},
}

func NewS3FS(
	ctx context.Context,
	backup storeTypes.Backup,
	endpoint, accessKey, secretKey, bucket, region, prefix string,
	useSSL bool,
) *S3FS {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    useSSL,
		Region:    region,
		Transport: transport,
	})
	if err != nil {
		return nil
	}

	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	metaCache, _ := lru.New[string, cacheEntry[agentTypes.AgentFileInfo]](maxCacheEntries)
	dirCache, _ := lru.New[string, cacheEntry[agentTypes.ReadDirEntries]](maxCacheEntries)

	s3ctx, cancel := context.WithCancel(ctx)

	return &S3FS{
		VFSBase: &vfs.VFSBase{
			Ctx:           s3ctx,
			Cancel:        cancel,
			Backup:        backup,
			FileCount:     xsync.NewCounter(),
			FolderCount:   xsync.NewCounter(),
			TotalBytes:    xsync.NewCounter(),
			StatCacheHits: xsync.NewCounter(),
		},
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		metaCache: metaCache,
		dirCache:  dirCache,
	}
}

func (fs *S3FS) fullKey(fpath string) string {
	if fpath == "/" || fpath == "" {
		return fs.prefix
	}
	p := strings.TrimPrefix(path.Clean(fpath), "/")
	if p == "." || p == "" {
		return fs.prefix
	}
	return fs.prefix + p
}

func (fs *S3FS) Attr(fpath string) (agentTypes.AgentFileInfo, error) {
	now := time.Now().Unix()

	if fpath == "/" || fpath == "" {
		return agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}, nil
	}

	key := fs.fullKey(fpath)

	if val, ok := fs.metaCache.Get(key); ok && !val.expired() {
		if val.value == nil {
			return agentTypes.AgentFileInfo{}, syscall.ENOENT
		}
		fs.StatCacheHits.Add(1)
		return *val.value, nil
	}

	ctx, cancel := context.WithTimeout(fs.Ctx, 5*time.Second)
	defer cancel()

	objInfo, err := fs.client.StatObject(ctx, fs.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		mod := objInfo.LastModified.Unix()
		result := agentTypes.AgentFileInfo{
			IsDir:          false,
			Mode:           0644,
			Size:           objInfo.Size,
			Blocks:         uint64((objInfo.Size + 511) / 512),
			CreationTime:   mod,
			LastAccessTime: mod,
			LastWriteTime:  mod,
		}

		fs.metaCache.Add(key, cacheEntry[agentTypes.AgentFileInfo]{
			value:     &result,
			expiresAt: time.Now().Add(metaCacheTTL),
		})
		fs.FileCount.Add(1)
		return result, nil
	}

	dirKey := key
	if !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}

	opts := minio.ListObjectsOptions{
		Prefix:    dirKey,
		Recursive: false,
		MaxKeys:   1,
	}

	for obj := range fs.client.ListObjects(ctx, fs.bucket, opts) {
		if obj.Err != nil {
			return agentTypes.AgentFileInfo{}, obj.Err
		}

		result := agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}

		fs.metaCache.Add(key, cacheEntry[agentTypes.AgentFileInfo]{
			value:     &result,
			expiresAt: time.Now().Add(metaCacheTTL),
		})
		fs.FolderCount.Add(1)
		return result, nil
	}

	fs.metaCache.Add(key, cacheEntry[agentTypes.AgentFileInfo]{
		value:     nil,
		expiresAt: time.Now().Add(10 * time.Second),
	})

	return agentTypes.AgentFileInfo{}, syscall.ENOENT
}

func (fs *S3FS) ReadDir(fpath string) (*S3DirStream, error) {
	var prefix string
	if fpath == "/" || fpath == "" {
		prefix = fs.prefix
	} else {
		key := fs.fullKey(fpath)
		prefix = key
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	if val, ok := fs.dirCache.Get(prefix); ok && !val.expired() {
		return &S3DirStream{entries: *val.value}, nil
	}

	ctx, cancel := context.WithTimeout(fs.Ctx, 15*time.Second)
	defer cancel()

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	}

	entries := make(agentTypes.ReadDirEntries, 0, 128)
	seenNames := *(seenNamesPool.Get().(*map[string]bool))
	for k := range seenNames {
		delete(seenNames, k)
	}

	for obj := range fs.client.ListObjects(ctx, fs.bucket, opts) {
		if obj.Err != nil {
			seenNamesPool.Put(&seenNames)
			return nil, obj.Err
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

		mod := obj.LastModified.Unix()
		mode := uint32(0644)
		if isDir {
			mode = uint32(os.ModeDir | 0555)
		}

		entry := agentTypes.AgentFileInfo{
			Name:           name,
			IsDir:          isDir,
			Mode:           mode,
			Size:           obj.Size,
			Blocks:         uint64((obj.Size + 511) / 512),
			CreationTime:   mod,
			LastAccessTime: mod,
			LastWriteTime:  mod,
		}

		entries = append(entries, entry)

		cachePath := path.Join(fpath, name)
		fs.metaCache.Add(fs.fullKey(cachePath), cacheEntry[agentTypes.AgentFileInfo]{
			value:     &entry,
			expiresAt: time.Now().Add(metaCacheTTL),
		})
	}

	seenNamesPool.Put(&seenNames)

	fs.dirCache.Add(prefix, cacheEntry[agentTypes.ReadDirEntries]{
		value:     &entries,
		expiresAt: time.Now().Add(dirCacheTTL),
	})
	return &S3DirStream{entries: entries}, nil
}

func (fs *S3FS) OpenFile(fpath string, flag int, _ os.FileMode) (*S3File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, syscall.EROFS
	}

	info, err := fs.Attr(fpath)
	if err != nil {
		return nil, err
	}

	if info.IsDir {
		return nil, syscall.EISDIR
	}

	return &S3File{
		fs:   fs,
		key:  fs.fullKey(fpath),
		size: info.Size,
	}, nil
}

func (fs *S3FS) Unmount(ctx context.Context) {
	if fs.Fuse != nil {
		_ = fs.Fuse.Unmount()
	}
	fs.Cancel()
}
