//go:build linux

package s3fs

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	"github.com/pbs-plus/pbs-plus/internal/memlocal"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (fs *S3FS) logError(fpath string, err error) {
	if !strings.HasSuffix(fpath, ".pxarexclude") {
		syslog.L.Error(err).
			WithField("path", fpath).
			WithJob(fs.Job.ID).
			Write()
	}
}

var _ vfs.FS = (*S3FS)(nil)

func NewS3FS(
	ctx context.Context,
	job storeTypes.Job,
	endpoint, accessKey, secretKey, bucket, region, prefix string,
	useSSL bool,
) *S3FS {
	ctxFs, cancel := context.WithCancel(ctx)

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
		Region: region,
	})
	if err != nil {
		cancel()
		return nil
	}

	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	memcachePath := filepath.Join(constants.MemcachedSocketPath, fmt.Sprintf("%s.sock", job.ID))

	stopMemLocal, err := memlocal.StartMemcachedOnUnixSocket(ctxFs, memlocal.MemcachedConfig{
		SocketPath:     memcachePath,
		MemoryMB:       1024,
		MaxConnections: 0,
	})
	if err != nil {
		cancel()
		return nil
	}

	fs := &S3FS{
		basePath: "/",
		ctx:      ctxFs,
		cancel:   cancel,
		Job:      job,
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		memcache: memcache.New(memcachePath),
	}

	go func() {
		<-ctxFs.Done()
		fs.memcache.DeleteAll()
		fs.memcache.Close()
		stopMemLocal()
	}()

	return fs
}

func (fs *S3FS) GetMemcache() *memcache.Client {
	return fs.memcache
}

func (fs *S3FS) Context() context.Context { return fs.ctx }

func (fs *S3FS) Root() string {
	return fs.basePath
}

func (fs *S3FS) Open(filename string) (vfs.FileHandle, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *S3FS) OpenFile(filename string, flag int, _ os.FileMode) (vfs.FileHandle, error) {
	defer func() {
		key := fs.fullKey(filename)
		fs.memcache.Delete("attr:" + key)
	}()

	info, err := fs.Attr(filename, false)
	if err != nil {
		return nil, err
	}
	if info.IsDir {
		return nil, syscall.EISDIR
	}

	return &S3File{
		fs:    fs,
		key:   fs.fullKey(filename),
		size:  info.Size,
		jobId: fs.Job.ID,
	}, nil
}

func (fs *S3FS) Attr(fpath string, isLookup bool) (agentTypes.AgentFileInfo, error) {
	now := time.Now().Unix()

	if fpath == "/" || fpath == "" {
		fi := agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}
		if !isLookup {
			atomic.AddInt64(&fs.folderCount, 1)
			_ = fs.memcache.Set(&memcache.Item{Key: "stats:foldersAccessed", Value: []byte(strconv.FormatInt(atomic.LoadInt64(&fs.folderCount), 10)), Expiration: 0})
		}
		return fi, nil
	}

	key := fs.fullKey(fpath)

	var cached agentTypes.AgentFileInfo
	if it, err := fs.memcache.Get("attr:" + key); err == nil {
		atomic.AddInt64(&fs.statCacheHits, 1)
		_ = fs.memcache.Set(&memcache.Item{Key: "stats:statCacheHits", Value: []byte(strconv.FormatInt(atomic.LoadInt64(&fs.statCacheHits), 10)), Expiration: 0})
		if err := cached.Decode(it.Value); err == nil {
			if !isLookup {
				if cached.IsDir {
					atomic.AddInt64(&fs.folderCount, 1)
					_ = fs.memcache.Set(&memcache.Item{Key: "stats:foldersAccessed", Value: []byte(strconv.FormatInt(atomic.LoadInt64(&fs.folderCount), 10)), Expiration: 0})
				} else {
					atomic.AddInt64(&fs.fileCount, 1)
					_ = fs.memcache.Set(&memcache.Item{Key: "stats:filesAccessed", Value: []byte(strconv.FormatInt(atomic.LoadInt64(&fs.fileCount), 10)), Expiration: 0})
				}
			}
			return cached, nil
		}
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 30*time.Second)
	defer cancel()

	if objInfo, err := fs.client.StatObject(ctx, fs.bucket, key, minio.StatObjectOptions{}); err == nil {
		mod := objInfo.LastModified.Unix()
		blocks := uint64((objInfo.Size + 511) / 512)
		fi := agentTypes.AgentFileInfo{
			IsDir:          false,
			Mode:           0644,
			Size:           objInfo.Size,
			Blocks:         blocks,
			CreationTime:   mod,
			LastAccessTime: mod,
			LastWriteTime:  mod,
		}
		raw, _ := fi.Encode()
		if isLookup {
			_ = fs.memcache.Set(&memcache.Item{Key: "attr:" + key, Value: raw, Expiration: 0})
		}
		if !isLookup {
			atomic.AddInt64(&fs.fileCount, 1)
			_ = fs.memcache.Set(&memcache.Item{Key: "stats:filesAccessed", Value: []byte(strconv.FormatInt(atomic.LoadInt64(&fs.fileCount), 10)), Expiration: 0})
		}
		return fi, nil
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

	foundDir := false
	for obj := range fs.client.ListObjects(ctx, fs.bucket, opts) {
		if obj.Err != nil {
			fs.logError(fpath, obj.Err)
			return agentTypes.AgentFileInfo{}, syscall.ENOENT
		}
		foundDir = true
		break
	}

	if foundDir {
		fi := agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}
		raw, _ := fi.Encode()
		if isLookup {
			_ = fs.memcache.Set(&memcache.Item{Key: "attr:" + key, Value: raw, Expiration: 0})
		}
		if !isLookup {
			atomic.AddInt64(&fs.folderCount, 1)
			_ = fs.memcache.Set(&memcache.Item{Key: "stats:foldersAccessed", Value: []byte(strconv.FormatInt(atomic.LoadInt64(&fs.folderCount), 10)), Expiration: 0})
		}
		return fi, nil
	}

	fs.logError(fpath, syscall.ENOENT)
	return agentTypes.AgentFileInfo{}, syscall.ENOENT
}

func (fs *S3FS) Xattr(fpath string) (agentTypes.AgentFileInfo, error) {
	var fi agentTypes.AgentFileInfo

	key := fs.fullKey(fpath)

	var fiCached agentTypes.AgentFileInfo
	reqAclOnly := false
	if it, err := fs.memcache.Get("xattr:" + key); err == nil {
		reqAclOnly = true
		_ = fiCached.Decode(it.Value)
		fs.memcache.Delete("xattr:" + key)
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 30*time.Second)
	defer cancel()

	if reqAclOnly {
		if objInfo, err := fs.client.StatObject(ctx, fs.bucket, key, minio.StatObjectOptions{}); err == nil {
			fi = agentTypes.AgentFileInfo{
				IsDir:          false,
				Mode:           0644,
				Size:           objInfo.Size,
				Blocks:         uint64((objInfo.Size + 511) / 512),
				CreationTime:   fiCached.CreationTime,
				LastAccessTime: fiCached.LastAccessTime,
				LastWriteTime:  fiCached.LastWriteTime,
				FileAttributes: fiCached.FileAttributes,
			}
			return fi, nil
		} else {
			fs.logError(fpath, err)
		}
		return agentTypes.AgentFileInfo{}, syscall.ENODATA
	}

	if objInfo, err := fs.client.StatObject(ctx, fs.bucket, key, minio.StatObjectOptions{}); err == nil {
		mod := objInfo.LastModified.Unix()
		fi = agentTypes.AgentFileInfo{
			IsDir:          false,
			Mode:           0644,
			Size:           objInfo.Size,
			Blocks:         uint64((objInfo.Size + 511) / 512),
			CreationTime:   mod,
			LastAccessTime: mod,
			LastWriteTime:  mod,
		}
		raw, _ := fi.Encode()
		_ = fs.memcache.Set(&memcache.Item{Key: "xattr:" + key, Value: raw, Expiration: 0})
		return fi, nil
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
			fs.logError(fpath, obj.Err)
			return agentTypes.AgentFileInfo{}, syscall.ENODATA
		}
		now := time.Now().Unix()
		fi = agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}
		raw, _ := fi.Encode()
		_ = fs.memcache.Set(&memcache.Item{Key: "xattr:" + key, Value: raw, Expiration: 0})
		return fi, nil
	}

	fs.logError(fpath, syscall.ENODATA)
	return agentTypes.AgentFileInfo{}, syscall.ENODATA
}

func (fs *S3FS) StatFS() (agentTypes.StatFS, error) {
	return agentTypes.StatFS{
		Bsize:   4096,
		Blocks:  1 << 50,
		Bfree:   1 << 49,
		Bavail:  1 << 49,
		Files:   1 << 40,
		Ffree:   1 << 39,
		NameLen: 1024,
	}, nil
}

func (fs *S3FS) ReadDir(fpath string) (vfs.DirStream, error) {
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

	if it, err := fs.memcache.Get("dir:" + prefix); err == nil {
		var cached agentTypes.ReadDirEntries
		if err := cached.Decode(it.Value); err == nil {
			return &S3DirStream{fs: fs, entries: cached}, nil
		}
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 30*time.Second)
	defer cancel()

	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	}

	entries := make(agentTypes.ReadDirEntries, 0, 64)
	seen := map[string]struct{}{}

	for obj := range fs.client.ListObjects(ctx, fs.bucket, opts) {
		if obj.Err != nil {
			fs.logError(fpath, obj.Err)
			return nil, syscall.ENOENT
		}

		name := strings.TrimPrefix(obj.Key, prefix)
		if name == "" {
			continue
		}

		isDir := strings.HasSuffix(name, "/")
		if isDir {
			name = strings.TrimSuffix(name, "/")
		}

		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		mode := uint32(0644)
		if isDir {
			mode = uint32(os.ModeDir | 0555)
		}

		entries = append(entries, agentTypes.AgentFileInfo{
			Name: name,
			Mode: mode,
		})
	}

	raw, _ := entries.Encode()
	_ = fs.memcache.Set(&memcache.Item{Key: "dir:" + prefix, Value: raw, Expiration: 0})
	fs.memcache.Delete("attr:" + strings.TrimSuffix(prefix, "/"))
	return &S3DirStream{fs: fs, entries: entries}, nil
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
