package arpcfs

import (
	"sync/atomic"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
)

// ARPCFS implements billy.Filesystem using aRPC calls
type ARPCFS struct {
	*vfs.VFSBase

	session    *arpc.Session
	Hostname   string
	backupMode string
}

// ARPCFile implements billy.File for remote files
type ARPCFile struct {
	fs       *ARPCFS
	name     string
	offset   int64
	handleID types.FileHandleId
	isClosed atomic.Bool
	jobId    string
}
