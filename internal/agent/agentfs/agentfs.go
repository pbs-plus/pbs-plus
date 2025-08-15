package agentfs

import (
	"context"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/idgen"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

type AgentFSServer struct {
	ctx              context.Context
	ctxCancel        context.CancelFunc
	jobId            string
	snapshot         snapshots.Snapshot
	handleIdGen      *idgen.IDGenerator
	handles          *safemap.Map[uint64, *FileHandle]
	arpcRouter       *arpc.Router
	statFs           types.StatFS
	allocGranularity uint32
	readMode         string
}

func NewAgentFSServer(jobId string, readMode string, snapshot snapshots.Snapshot) *AgentFSServer {
	ctx, cancel := context.WithCancel(context.Background())

	allocGranularity := getAllocGranularity()
	if allocGranularity == 0 {
		allocGranularity = 65536 // 64 KB usually
	}

	s := &AgentFSServer{
		snapshot:         snapshot,
		jobId:            jobId,
		handles:          safemap.New[uint64, *FileHandle](),
		ctx:              ctx,
		ctxCancel:        cancel,
		handleIdGen:      idgen.NewIDGenerator(),
		allocGranularity: uint32(allocGranularity),
		readMode:         readMode,
	}

	if err := s.initializeStatFS(); err != nil && syslog.L != nil {
		syslog.L.Error(err).WithMessage("failed to initialize statfs").Write()
	}

	return s
}

func safeHandler(fn func(req arpc.Request) (arpc.Response, error)) func(req arpc.Request) (arpc.Response, error) {
	return func(req arpc.Request) (res arpc.Response, err error) {
		defer func() {
			if r := recover(); r != nil {
				syslog.L.Error(fmt.Errorf("panic in handler: %v", r)).
					WithMessage(fmt.Sprintf("panic in handler: %v", r)).
					WithField("payload", req.Payload).
					Write()
				err = os.ErrInvalid
			}
		}()
		return fn(req)
	}
}

func (s *AgentFSServer) RegisterHandlers(r *arpc.Router) {
	r.Handle(s.jobId+"/OpenFile", safeHandler(s.handleOpenFile))
	r.Handle(s.jobId+"/Attr", safeHandler(s.handleAttr))
	r.Handle(s.jobId+"/Xattr", safeHandler(s.handleXattr))
	r.Handle(s.jobId+"/ReadDir", safeHandler(s.handleReadDir))
	r.Handle(s.jobId+"/ReadAt", safeHandler(s.handleReadAt))
	r.Handle(s.jobId+"/Lseek", safeHandler(s.handleLseek))
	r.Handle(s.jobId+"/Close", safeHandler(s.handleClose))
	r.Handle(s.jobId+"/StatFS", safeHandler(s.handleStatFS))

	s.arpcRouter = r
}

func (s *AgentFSServer) Close() {
	if s.arpcRouter != nil {
		r := s.arpcRouter
		r.CloseHandle(s.jobId + "/OpenFile")
		r.CloseHandle(s.jobId + "/Attr")
		r.CloseHandle(s.jobId + "/Xattr")
		r.CloseHandle(s.jobId + "/ReadDir")
		r.CloseHandle(s.jobId + "/ReadAt")
		r.CloseHandle(s.jobId + "/Lseek")
		r.CloseHandle(s.jobId + "/Close")
		r.CloseHandle(s.jobId + "/StatFS")
	}

	s.closeFileHandles()
	s.ctxCancel()
}
