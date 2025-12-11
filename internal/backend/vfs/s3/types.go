//go:build linux

package s3fs

import (
	"github.com/minio/minio-go/v7"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
)

type S3FS struct {
	vfs.VFSBase

	client *minio.Client
	bucket string
	prefix string
}

type S3DirStream struct {
	fs      *S3FS
	entries types.ReadDirEntries
	idx     int
	total   uint64
}

type S3File struct {
	fs     *S3FS
	key    string
	offset int64
	size   int64
	jobId  string
}
