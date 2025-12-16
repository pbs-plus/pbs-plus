//go:build linux

package s3fs

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/minio/minio-go/v7"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
)

type S3FS struct {
	*vfs.VFSBase

	metaCache *lru.Cache[string, cacheEntry[agentTypes.AgentFileInfo]]
	dirCache  *lru.Cache[string, cacheEntry[agentTypes.ReadDirEntries]]
	client    *minio.Client
	bucket    string
	prefix    string
}

type S3File struct {
	fs     *S3FS
	key    string
	offset int64
	size   int64
	jobId  string
}
