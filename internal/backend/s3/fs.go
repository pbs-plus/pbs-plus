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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
)

// Configurable cache TTLs
const (
	metaCacheTTL    = 30 * time.Second
	dirCacheTTL     = 30 * time.Second
	readAheadSize   = 2 * 1024 * 1024
	maxCacheEntries = 2048
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
	client *s3.Client
	Job    storeTypes.Job
	bucket string
	prefix string
	Mount  *gofuse.Server

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

// NewS3FS constructs an S3FS with caching using AWS SDK v2.
func NewS3FS(
	ctx context.Context,
	job storeTypes.Job,
	endpoint, accessKey, secretKey, bucket, region, prefix string,
	useSSL bool,
	usePathStyle bool,
) (*S3FS, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           endpoint,
					SigningRegion: region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		},
	)

	// Load AWS config with static credentials and custom resolver
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	metaCache, _ := lru.New[string, cacheEntry[agentTypes.AgentFileInfo]](
		maxCacheEntries,
	)
	dirCache, _ := lru.New[string, cacheEntry[agentTypes.ReadDirEntries]](
		maxCacheEntries,
	)

	return &S3FS{
		ctx:       ctx,
		Job:       job,
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		metaCache: metaCache,
		dirCache:  dirCache,
	}, nil
}

// fullKey maps a fuse path "/foo/bar" → "<prefix>foo/bar"
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

// Attr implements agentTypes.AgentFileInfo lookup with caching and optimized detection.
func (fs *S3FS) Attr(fpath string) (agentTypes.AgentFileInfo, error) {
	// Handle root directory specially
	if fpath == "/" || fpath == "" {
		now := time.Now().Unix()
		return agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}, nil
	}

	key := fs.fullKey(fpath)

	// Check cache
	if val, ok := fs.metaCache.Get(key); ok && !val.expired() {
		return val.value, nil
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 5*time.Second)
	defer cancel()

	// First, try to get the object directly (most efficient for files)
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(key),
	}

	headOutput, err := fs.client.HeadObject(ctx, headInput)
	if err == nil {
		// It's a file
		mod := headOutput.LastModified.Unix()
		size := headOutput.ContentLength
		blocks := uint64((size + 511) / 512)
		result := agentTypes.AgentFileInfo{
			IsDir:          false,
			Mode:           0644,
			Size:           size,
			Blocks:         blocks,
			CreationTime:   mod,
			LastAccessTime: mod,
			LastWriteTime:  mod,
		}

		fs.metaCache.Add(key, cacheEntry[agentTypes.AgentFileInfo]{
			value:     result,
			expiresAt: time.Now().Add(metaCacheTTL),
		})
		atomic.AddInt64(&fs.fileCount, 1)
		return result, nil
	}

	// If HeadObject failed, check if it's a directory using ListObjectsV2 with delimiter
	dirKey := key
	if !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}

	listInput := &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(dirKey),
		Delimiter: aws.String("/"), // Key optimization for directory detection
		MaxKeys:   *aws.Int32(1),
	}

	listOutput, err := fs.client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return agentTypes.AgentFileInfo{}, err
	}

	// Check if any objects or common prefixes exist
	if len(listOutput.Contents) > 0 || len(listOutput.CommonPrefixes) > 0 {
		// It's a directory
		now := time.Now().Unix()
		result := agentTypes.AgentFileInfo{
			IsDir:          true,
			Mode:           uint32(os.ModeDir | 0555),
			CreationTime:   now,
			LastAccessTime: now,
			LastWriteTime:  now,
		}

		fs.metaCache.Add(key, cacheEntry[agentTypes.AgentFileInfo]{
			value:     result,
			expiresAt: time.Now().Add(metaCacheTTL),
		})
		atomic.AddInt64(&fs.folderCount, 1)
		return result, nil
	}

	return agentTypes.AgentFileInfo{}, syscall.ENOENT
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

// ReadDir returns a cached snapshot of the directory with delimiter optimization.
func (fs *S3FS) ReadDir(fpath string) (*S3DirStream, error) {
	var prefix string

	// Handle root directory
	if fpath == "/" || fpath == "" {
		prefix = fs.prefix
	} else {
		key := fs.fullKey(fpath)
		prefix = key
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	// Check cache
	if val, ok := fs.dirCache.Get(prefix); ok && !val.expired() {
		return &S3DirStream{entries: val.value}, nil
	}

	ctx, cancel := context.WithTimeout(fs.ctx, 10*time.Second)
	defer cancel()

	// Use ListObjectsV2 with delimiter for efficient directory listing
	listInput := &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"), // Key optimization for directory handling
	}

	entries := make(agentTypes.ReadDirEntries, 0)
	seenNames := make(map[string]bool)

	// Use paginator for large directories
	paginator := s3.NewListObjectsV2Paginator(fs.client, listInput)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		// Process regular objects (files)
		for _, obj := range page.Contents {
			name := strings.TrimPrefix(*obj.Key, prefix)
			if name == "" || strings.Contains(name, "/") {
				continue // Skip empty names or nested objects
			}
			if !seenNames[name] {
				seenNames[name] = true
				entries = append(entries, agentTypes.AgentDirEntry{
					Name: name,
					Mode: 0644,
				})
			}
		}

		// Process common prefixes (directories)
		for _, commonPrefix := range page.CommonPrefixes {
			name := strings.TrimPrefix(strings.TrimSuffix(*commonPrefix.Prefix, "/"), prefix)
			if name != "" && !seenNames[name] {
				seenNames[name] = true
				entries = append(entries, agentTypes.AgentDirEntry{
					Name: name,
					Mode: uint32(os.ModeDir | 0555),
				})
			}
		}
	}

	fs.dirCache.Add(prefix, cacheEntry[agentTypes.ReadDirEntries]{
		value:     entries,
		expiresAt: time.Now().Add(dirCacheTTL),
	})
	return &S3DirStream{entries: entries}, nil
}

// OpenFile only allows read-only
func (fs *S3FS) OpenFile(
	fpath string,
	flag int,
	_ os.FileMode,
) (*S3File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, syscall.EROFS
	}

	key := fs.fullKey(fpath)

	// Get file info to initialize size
	info, err := fs.Attr(fpath)
	if err != nil {
		return nil, err
	}

	if info.IsDir {
		return nil, syscall.EISDIR
	}

	return &S3File{
		fs:   fs,
		key:  key,
		size: info.Size,
	}, nil
}
