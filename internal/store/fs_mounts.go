//go:build linux

package store

import (
	"context"
	"sync"

	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc"
	s3fs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type FSMount struct {
	sync.Mutex
	arpcfs *arpcfs.ARPCFS
	s3fs   *s3fs.S3FS
	pxar   *pxar.PxarReader
}

var activeMounts = safemap.New[string, *FSMount]()

func CreateARPCFSMount(connId string, fs *arpcfs.ARPCFS) {
	conn := &FSMount{
		arpcfs: fs,
	}

	activeMounts.Set(connId, conn)
}

func CreatePxarReader(connId string, r *pxar.PxarReader) {
	conn := &FSMount{
		pxar: r,
	}

	activeMounts.Set(connId, conn)
}

func CreateS3FSMount(connId string, fs *s3fs.S3FS) {
	conn := &FSMount{
		s3fs: fs,
	}

	activeMounts.Set(connId, conn)
}

func DisconnectSession(connId string) {
	if fs, ok := activeMounts.GetAndDel(connId); ok {
		if fs.arpcfs != nil {
			fs.arpcfs.Unmount(context.Background())
		}
		if fs.s3fs != nil {
			fs.s3fs.Unmount(context.Background())
		}
		if fs.pxar != nil {
			fs.pxar.Close()
		}
	}
}

func GetSessionARPCFS(connId string) *arpcfs.ARPCFS {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.arpcfs
	} else {
		return nil
	}
}

func GetSessionS3FS(connId string) *s3fs.S3FS {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.s3fs
	} else {
		return nil
	}
}

func GetSessionPxarReader(connId string) *pxar.PxarReader {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.pxar
	} else {
		return nil
	}
}
