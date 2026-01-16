//go:build linux

package s3fs

import (
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
)

type S3FS struct {
	*vfs.VFSBase

	client *minio.Client
	bucket string
	prefix string
}

type S3File struct {
	fs  *S3FS
	key string

	mu     sync.Mutex
	size   int64
	buf    []byte
	bufOff int64
}
