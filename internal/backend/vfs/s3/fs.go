//go:build linux

package s3fs

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/fxamacker/cbor/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	"github.com/pbs-plus/pbs-plus/internal/memlocal"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	attrPrefix = "attr:"
)

func NewS3FS(
	ctx context.Context,
	backup storeTypes.Backup,
	endpoint, accessKey, secretKey, bucket, region, prefix string,
	useSSL bool,
) *S3FS {
	syslog.L.Debug().
		WithMessage("NewS3FS called").
		WithField("endpoint", endpoint).
		WithField("bucket", bucket).
		WithField("backupId", backup.ID).
		Write()

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
		syslog.L.Error(err).WithMessage("failed to create minio client").Write()
		return nil
	}

	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	s3ctx, cancel := context.WithCancel(ctx)
	memcachePath := filepath.Join(constants.MemcachedSocketPath, fmt.Sprintf("%s.sock", backup.ID))

	syslog.L.Debug().
		WithMessage("Starting local memcached").
		WithField("socketPath", memcachePath).
		WithField("backupId", backup.ID).
		Write()

	stopMemLocal, err := memlocal.StartMemcachedOnUnixSocket(s3ctx, memlocal.MemcachedConfig{
		SocketPath:     memcachePath,
		MemoryMB:       1024,
		MaxConnections: 0,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to run memcached server").Write()
		cancel()
		return nil
	}

	fs := &S3FS{
		VFSBase: vfs.InjectBase(vfs.VFSBase{
			BasePath: "/",
			Ctx:      s3ctx,
			Cancel:   cancel,
			Backup:   backup,
			Memcache: memcache.New(memcachePath),
		}),
		client: client,
		bucket: bucket,
		prefix: prefix,
	}

	go func() {
		<-s3ctx.Done()
		syslog.L.Debug().
			WithMessage("Context done, cleaning up memcache and memlocal").
			WithField("backupId", fs.Backup.ID).
			Write()
		fs.Memcache.DeleteAll()
		fs.Memcache.Close()
		fs.TotalBytes.Reset()
		fs.FolderCount.Reset()
		fs.FileCount.Reset()
		fs.StatCacheHits.Reset()
		stopMemLocal()
	}()

	return fs
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

func (fs *S3FS) Attr(ctx context.Context, fpath string, isLookup bool) (agentTypes.AgentFileInfo, error) {
	syslog.L.Debug().
		WithMessage("Attr called").
		WithField("path", fpath).
		WithField("isLookup", isLookup).
		WithField("backupId", fs.Backup.ID).
		Write()

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
	cacheKey := fs.GetCacheKey(attrPrefix, key)

	var fi agentTypes.AgentFileInfo
	cached, err := fs.Memcache.Get(cacheKey)
	if err == nil {
		fs.StatCacheHits.Add(1)
		if err := cbor.Unmarshal(cached.Value, &fi); err == nil {
			syslog.L.Debug().
				WithMessage("Attr cache hit").
				WithField("path", fpath).
				WithField("backupId", fs.Backup.ID).
				Write()
			return fi, nil
		}
	}

	syslog.L.Debug().
		WithMessage("Attr cache miss, issuing S3 Stat").
		WithField("path", fpath).
		Write()

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	objInfo, err := fs.client.StatObject(ctxN, fs.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		mod := objInfo.LastModified.Unix()
		fi = agentTypes.AgentFileInfo{
			Name:           path.Base(fpath),
			IsDir:          false,
			Mode:           0644,
			Size:           objInfo.Size,
			Blocks:         uint64((objInfo.Size + 511) / 512),
			CreationTime:   mod,
			LastAccessTime: mod,
			LastWriteTime:  mod,
		}
	} else {
		dirKey := key
		if !strings.HasSuffix(dirKey, "/") {
			dirKey += "/"
		}
		opts := minio.ListObjectsOptions{Prefix: dirKey, Recursive: false, MaxKeys: 1}
		objects := fs.client.ListObjects(ctxN, fs.bucket, opts)
		_, ok := <-objects

		if !ok {
			return agentTypes.AgentFileInfo{}, syscall.ENOENT
		}

		fi = agentTypes.AgentFileInfo{
			Name:           path.Base(fpath),
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}
	}

	if raw, err := cbor.Marshal(fi); err == nil {
		if isLookup {
			fs.Memcache.Set(&memcache.Item{Key: cacheKey, Value: raw, Expiration: 0})
		}
	}

	if !isLookup {
		if !fi.IsDir {
			fs.Memcache.Delete(cacheKey)
			fs.FileCount.Add(1)
			syslog.L.Debug().
				WithMessage("Attr counted file and cleared cache").
				WithField("path", fpath).
				WithField("fileCount", fs.FileCount.Value()).
				Write()
		} else {
			fs.FolderCount.Add(1)
		}
	}

	return fi, nil
}

func (fs *S3FS) StatFS(ctx context.Context) (agentTypes.StatFS, error) {
	syslog.L.Debug().WithMessage("StatFS called").WithField("backupId", fs.Backup.ID).Write()
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

func (fs *S3FS) OpenFile(ctx context.Context, fpath string, flag int, perm os.FileMode) (*S3File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, syscall.EROFS
	}

	info, err := fs.Attr(ctx, fpath, false)
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

func (fs *S3FS) ReadDir(ctx context.Context, fpath string) (*S3DirStream, error) {
	syslog.L.Debug().
		WithMessage("ReadDir called").
		WithField("path", fpath).
		WithField("backupId", fs.Backup.ID).
		Write()

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

	fs.Memcache.Delete(fs.GetCacheKey(attrPrefix, prefix))

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: false}
	entries := make(agentTypes.ReadDirEntries, 0)
	seenNames := make(map[string]bool)

	for obj := range fs.client.ListObjects(ctxN, fs.bucket, opts) {
		if obj.Err != nil {
			syslog.L.Error(obj.Err).WithMessage("ReadDir S3 List failed").Write()
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

		itemKey := fs.GetCacheKey(attrPrefix, fs.fullKey(path.Join(fpath, name)))
		if raw, err := cbor.Marshal(entry); err == nil {
			fs.Memcache.Set(&memcache.Item{Key: itemKey, Value: raw, Expiration: 0})
		}
	}

	syslog.L.Debug().
		WithMessage("ReadDir completed").
		WithField("path", fpath).
		WithField("count", len(entries)).
		Write()

	return &S3DirStream{entries: entries}, nil
}

func (fs *S3FS) Root() string {
	return fs.BasePath
}

func (fs *S3FS) Unmount(ctx context.Context) {
	if fs.Fuse != nil {
		_ = fs.Fuse.Unmount()
	}
	fs.Cancel()
}
