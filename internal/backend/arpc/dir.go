//go:build linux

package arpcfs

import (
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type DirStream struct {
	fs       *ARPCFS
	path     string
	handleId types.FileHandleId
	closed   atomic.Bool
	lastResp types.ReadDirEntries
	curIdx   atomic.Uint64
}

func (s *DirStream) HasNext() bool {
	if s.closed.Load() {
		return false
	}

	if int(s.curIdx.Load()) < len(s.lastResp.Entries)-1 {
		return true
	}

	req := types.ReadDirReq{HandleID: s.handleId}

	buf, bytesRead, err := s.fs.session.CallBinary(s.fs.ctx, s.fs.Job.ID+"/ReadDir", &req)
	if err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	err = s.lastResp.Decode(buf[:bytesRead])
	if err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	s.curIdx.Store(0)

	return s.lastResp.HasMore
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if s.closed.Load() {
		return fuse.DirEntry{}, syscall.EINVAL
	}

	curr := s.lastResp.Entries[s.curIdx.Load()]

	mode := os.FileMode(curr.Mode)
	modeBits := uint32(0)

	// Determine the file type using fuse.S_IF* constants
	switch {
	case mode.IsDir():
		modeBits = fuse.S_IFDIR
	case mode&os.ModeSymlink != 0:
		modeBits = fuse.S_IFLNK
	default:
		modeBits = fuse.S_IFREG
	}

	s.curIdx.Add(1)

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	closeReq := types.CloseReq{HandleID: s.handleId}
	_, err := s.fs.session.CallMsgWithTimeout(1*time.Minute, s.fs.Job.ID+"/Close", &closeReq)
	if err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
	}
	s.closed.Store(true)
}
