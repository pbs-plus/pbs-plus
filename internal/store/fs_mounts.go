//go:build linux

package store

import (
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type FSMount struct {
	sync.Mutex
	fs vfs.FS
}

var activeMounts = safemap.New[string, *FSMount]()

func CreateFSMount(connId string, fs vfs.FS) {
	conn := &FSMount{
		fs: fs,
	}

	activeMounts.Set(connId, conn)
}

func DisconnectSession(connId string) {
	if fs, ok := activeMounts.GetAndDel(connId); ok {
		fs.fs.Unmount()
	}
}

func GetSessionFS(connId string) vfs.FS {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.fs
	} else {
		return nil
	}
}
