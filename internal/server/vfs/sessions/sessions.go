//go:build linux

package sessions

import (
	"context"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
)

type FSMount struct {
	sync.Mutex
	arpcfs *arpcfs.ARPCFS
	pxar   *pxar.PxarReader
}

var activeMounts = safemap.New[string, *FSMount]()

func NewARPCFSMount(connId string, fs *arpcfs.ARPCFS) {
	conn := &FSMount{
		arpcfs: fs,
	}

	activeMounts.Set(connId, conn)
}

func NewPxarReader(connId string, r *pxar.PxarReader) {
	conn := &FSMount{
		pxar: r,
	}

	activeMounts.Set(connId, conn)
}

func DisconnectSession(connId string) {
	if fs, ok := activeMounts.GetAndDel(connId); ok {
		if fs.arpcfs != nil {
			fs.arpcfs.Unmount(context.Background())
		}
		if fs.pxar != nil {
			if err := fs.pxar.Close(); err != nil {
				log.Error(err, "")
			}
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

func GetSessionPxarReader(connId string) *pxar.PxarReader {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.pxar
	} else {
		return nil
	}
}
