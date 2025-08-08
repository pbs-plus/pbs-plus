//go:build linux

package arpcfs

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type DirStream struct {
	fs            *ARPCFS
	path          string
	handleId      types.FileHandleId
	closed        atomic.Bool
	lastRespMu    sync.Mutex
	lastResp      types.ReadDirEntries
	curIdx        atomic.Uint64
	totalReturned atomic.Uint64
}

func (s *DirStream) HasNext() bool {
	if s.closed.Load() {
		return false
	}

	if s.totalReturned.Load() >= uint64(s.fs.Job.MaxDirEntries) {
		lastPath := ""
		s.lastRespMu.Lock()
		curIdxVal := s.curIdx.Load()
		if curIdxVal > 0 && int(curIdxVal) <= len(s.lastResp) {
			lastEntry := s.lastResp[curIdxVal-1]
			lastPath = lastEntry.Name
		}
		s.lastRespMu.Unlock()

		syslog.L.Error(fmt.Errorf("maximum directory entries reached: %d", s.fs.Job.MaxDirEntries)).
			WithField("path", s.path).
			WithField("lastFile", lastPath).
			WithJob(s.fs.Job.ID).
			Write()

		return false
	}

	s.lastRespMu.Lock()
	hasCurrentEntry := int(s.curIdx.Load()) < len(s.lastResp)
	s.lastRespMu.Unlock()

	if hasCurrentEntry {
		return true
	}

	req := types.ReadDirReq{HandleID: s.handleId}

	var buf []byte
	bytesRead, err := s.fs.session.CallBinary(s.fs.ctx, s.fs.Job.ID+"/ReadDir", &req, buf)
	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			s.closed.Store(true)
			return false
		}

		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	if bytesRead == 0 {
		s.closed.Store(true)
		return false
	}

	var entries types.ReadDirEntries

	err = entries.Decode(buf[:bytesRead])
	if err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	s.lastRespMu.Lock()
	defer s.lastRespMu.Unlock()

	s.lastResp = entries
	s.curIdx.Store(0)

	return len(s.lastResp) > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if s.closed.Load() {
		return fuse.DirEntry{}, syscall.EBADF
	}

	s.lastRespMu.Lock()
	defer s.lastRespMu.Unlock()

	curIdxVal := s.curIdx.Load()

	if int(curIdxVal) >= len(s.lastResp) {
		syslog.L.Error(fmt.Errorf("internal state error: index out of bounds in Next")).
			WithField("path", s.path).
			WithField("curIdx", curIdxVal).
			WithField("lastRespLen", len(s.lastResp)).
			WithJob(s.fs.Job.ID).
			Write()
		return fuse.DirEntry{}, syscall.EBADF
	}

	curr := s.lastResp[curIdxVal]

	mode := os.FileMode(curr.Mode)
	modeBits := uint32(0)

	switch {
	case mode.IsDir():
		modeBits = fuse.S_IFDIR
	case mode&os.ModeSymlink != 0:
		modeBits = fuse.S_IFLNK
	default:
		modeBits = fuse.S_IFREG
	}

	s.curIdx.Add(1)
	s.totalReturned.Add(1)

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	if s.closed.Swap(true) {
		return
	}

	closeReq := types.CloseReq{HandleID: s.handleId}
	_, err := s.fs.session.CallMsgWithTimeout(1*time.Minute, s.fs.Job.ID+"/Close", &closeReq)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Job.ID).
			Write()
	}
}
