//go:build linux

package store

import (
	"sync"

	s3fs "github.com/pbs-plus/pbs-plus/internal/backend/s3"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type S3Mount struct {
	sync.Mutex
	fs *s3fs.S3FS
}

var activeS3Mounts = safemap.New[string, *S3Mount]()

func CreateS3Mount(connId string, fs *s3fs.S3FS) {
	conn := &S3Mount{
		fs: fs,
	}

	activeS3Mounts.Set(connId, conn)
}

func RemoveS3Mount(connId string) {
	if fs, ok := activeS3Mounts.GetAndDel(connId); ok {
		fs.fs.Unmount()
	}
}

func GetS3FS(connId string) *s3fs.S3FS {
	if conn, ok := activeS3Mounts.Get(connId); ok {
		return conn.fs
	} else {
		return nil
	}
}
