package store

import (
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
)

type FSConnection struct {
	sync.Mutex
	arpc *arpc.Session
	fs   *arpcfs.ARPCFS
}

var activeConns = safemap.New[string, *FSConnection]()

func CreateFSConnection(connId string, arpc *arpc.Session, fs *arpcfs.ARPCFS) {
	conn := &FSConnection{
		arpc: arpc,
		fs:   fs,
	}

	activeConns.Set(connId, conn)
}

func DisconnectSession(connId string) {
	if fs, ok := activeConns.GetAndDel(connId); ok {
		fs.fs.Unmount()
	}
}

func GetSessionFS(connId string) *arpcfs.ARPCFS {
	if conn, ok := activeConns.Get(connId); ok {
		return conn.fs
	} else {
		return nil
	}
}
