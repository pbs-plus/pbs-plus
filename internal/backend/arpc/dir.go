//go:build linux

package arpcfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	closed        int32 // Use int32 for older Go compatibility
	lastRespMu    sync.Mutex
	lastResp      types.ReadDirEntries
	curIdx        uint64
	totalReturned uint64
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024*1024+64)
	},
}

func (s *DirStream) HasNext() bool {
	if atomic.LoadInt32(&s.closed) != 0 {
		return false
	}

	if atomic.LoadUint64(&s.totalReturned) >= uint64(s.fs.Job.MaxDirEntries) {
		lastPath := ""
		s.lastRespMu.Lock()
		curIdxVal := atomic.LoadUint64(&s.curIdx)
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
	hasCurrentEntry := int(atomic.LoadUint64(&s.curIdx)) < len(s.lastResp)
	s.lastRespMu.Unlock()

	if hasCurrentEntry {
		return true
	}

	req := types.ReadDirReq{HandleID: s.handleId}

	readBuf := bufPool.Get().([]byte)
	defer bufPool.Put(readBuf)

	bytesRead, err := s.fs.session.CallBinary(s.fs.ctx, s.fs.Job.ID+"/ReadDir", &req, readBuf)
	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			atomic.StoreInt32(&s.closed, 1)
			return false
		}

		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	if bytesRead == 0 {
		atomic.StoreInt32(&s.closed, 1)
		return false
	}

	s.lastRespMu.Lock()
	err = s.lastResp.Decode(readBuf[:bytesRead])
	if err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	atomic.StoreUint64(&s.curIdx, 0)
	s.lastRespMu.Unlock()

	return len(s.lastResp) > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return fuse.DirEntry{}, syscall.EBADF
	}

	s.lastRespMu.Lock()
	defer s.lastRespMu.Unlock()

	curIdxVal := atomic.LoadUint64(&s.curIdx)

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

	if curr.FileAttributes != nil {
		fullPath := filepath.Join(s.path, curr.Name)
		s.fs.readDirAttrCache.Set(fullPath, types.AgentFileInfo{
			Name:    curr.Name,
			Size:    curr.Size,
			Mode:    curr.Mode,
			ModTime: curr.ModTime,
			IsDir:   curr.IsDir,
		})

		s.fs.readDirXAttrCache.Set(fullPath, types.AgentFileInfo{
			CreationTime:   curr.CreationTime,
			LastAccessTime: curr.LastAccessTime,
			LastWriteTime:  curr.LastWriteTime,
			FileAttributes: curr.FileAttributes,
		})
	}

	atomic.AddUint64(&s.curIdx, 1)
	atomic.AddUint64(&s.totalReturned, 1)

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	if atomic.SwapInt32(&s.closed, 1) != 0 {
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
