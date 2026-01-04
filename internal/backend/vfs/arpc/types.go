package arpcfs

import (
	"sync"
	"sync/atomic"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
)

type ARPCFS struct {
	*vfs.VFSBase

	session    atomic.Pointer[arpc.StreamPipe]
	Hostname   string
	backupMode string
}

type DirStream struct {
	fs            *ARPCFS
	path          string
	handleId      types.FileHandleId
	closed        int32
	maxedOut      int32
	lastRespMu    sync.Mutex
	lastResp      types.ReadDirEntries
	curIdx        uint64
	totalReturned uint64
}

type ARPCFile struct {
	fs       *ARPCFS
	name     string
	offset   int64
	handleID types.FileHandleId
	isClosed atomic.Bool
	jobId    string
}
