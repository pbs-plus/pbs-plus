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

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/memlocal"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/quic-go/quic-go"
)

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 4*1024*1024)
	},
}

func (s *DirStream) HasNext() bool {
	syslog.L.Debug().
		WithMessage("HasNext called").
		WithField("path", s.path).
		WithField("closed", atomic.LoadInt32(&s.closed)).
		WithField("curIdx", atomic.LoadUint64(&s.curIdx)).
		WithField("totalReturned", atomic.LoadUint64(&s.totalReturned)).
		WithField("maxDirEntries", s.fs.Job.MaxDirEntries).
		WithJob(s.fs.Job.ID).
		Write()

	if atomic.LoadInt32(&s.closed) != 0 {
		syslog.L.Debug().
			WithMessage("HasNext early return: stream closed").
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
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
	localLen := len(s.lastResp)
	localIdx := atomic.LoadUint64(&s.curIdx)
	s.lastRespMu.Unlock()

	syslog.L.Debug().
		WithMessage("HasNext state before fetch").
		WithField("hasCurrentEntry", hasCurrentEntry).
		WithField("lastRespLen", localLen).
		WithField("curIdx", localIdx).
		WithField("path", s.path).
		WithJob(s.fs.Job.ID).
		Write()

	if hasCurrentEntry {
		syslog.L.Debug().
			WithMessage("HasNext hit in-memory entries").
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return true
	}

	req := types.ReadDirReq{HandleID: s.handleId}

	readBuf := bufPool.Get().([]byte)
	defer bufPool.Put(readBuf)
	bytesRead := 0

	syslog.L.Debug().
		WithMessage("HasNext issuing ReadDir RPC").
		WithField("path", s.path).
		WithField("handleId", s.handleId).
		WithJob(s.fs.Job.ID).
		Write()

	err := s.fs.session.Call(s.fs.Ctx, s.fs.Job.ID+"/ReadDir", &req, arpc.RawStreamHandler(func(s *quic.Stream) error {
		n, err := binarystream.ReceiveDataInto(s, readBuf)
		if err != nil {
			return err
		}
		bytesRead = n

		return nil
	}))

	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			syslog.L.Debug().
				WithMessage("HasNext: process done received, closing stream").
				WithField("path", s.path).
				WithJob(s.fs.Job.ID).
				Write()
			atomic.StoreInt32(&s.closed, 1)
			return false
		}

		syslog.L.Error(err).
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	syslog.L.Debug().
		WithMessage("HasNext RPC completed").
		WithField("bytesRead", bytesRead).
		WithField("path", s.path).
		WithJob(s.fs.Job.ID).
		Write()

	if bytesRead == 0 {
		syslog.L.Debug().
			WithMessage("HasNext: no bytes read, marking closed").
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		atomic.StoreInt32(&s.closed, 1)
		return false
	}

	s.lastRespMu.Lock()
	err = s.lastResp.Decode(readBuf[:bytesRead])
	if err != nil {
		s.lastRespMu.Unlock()
		syslog.L.Error(err).
			WithField("path", s.path).
			WithField("bytesRead", bytesRead).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	atomic.StoreUint64(&s.curIdx, 0)
	postLen := len(s.lastResp)
	s.lastRespMu.Unlock()

	syslog.L.Debug().
		WithMessage("HasNext decoded batch").
		WithField("entries", postLen).
		WithField("path", s.path).
		WithJob(s.fs.Job.ID).
		Write()

	return postLen > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if atomic.LoadInt32(&s.closed) != 0 {
		syslog.L.Debug().
			WithMessage("Next called on closed stream").
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
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

	syslog.L.Debug().
		WithMessage("Next returning entry").
		WithField("path", s.path).
		WithField("name", curr.Name).
		WithField("size", curr.Size).
		WithField("mode", curr.Mode).
		WithField("isDir", curr.IsDir).
		WithField("curIdx", curIdxVal).
		WithField("lastRespLen", len(s.lastResp)).
		WithField("totalReturned", atomic.LoadUint64(&s.totalReturned)).
		WithJob(s.fs.Job.ID).
		Write()

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

	fullPath := filepath.Join(s.path, curr.Name)
	if !curr.ModTime.IsZero() {
		currAttr := types.AgentFileInfo{
			Name:    curr.Name,
			Size:    curr.Size,
			Mode:    curr.Mode,
			ModTime: curr.ModTime,
			IsDir:   curr.IsDir,
		}

		attrBytes, err := currAttr.Encode()
		if err == nil {
			if mcErr := s.fs.Memcache.Set(&memcache.Item{Key: "attr:" + memlocal.Key(fullPath), Value: attrBytes, Expiration: 0}); mcErr != nil {
				syslog.L.Debug().
					WithMessage("memcache set attr failed").
					WithField("path", fullPath).
					WithField("error", mcErr.Error()).
					WithJob(s.fs.Job.ID).
					Write()
			} else {
				syslog.L.Debug().
					WithMessage("memcache set attr").
					WithField("path", fullPath).
					WithJob(s.fs.Job.ID).
					Write()
			}
		} else {
			syslog.L.Debug().
				WithMessage("encode attr failed").
				WithField("path", fullPath).
				WithField("error", err.Error()).
				WithJob(s.fs.Job.ID).
				Write()
		}
	}

	if curr.FileAttributes != nil {
		currXAttr := types.AgentFileInfo{
			CreationTime:   curr.CreationTime,
			LastAccessTime: curr.LastAccessTime,
			LastWriteTime:  curr.LastWriteTime,
			FileAttributes: curr.FileAttributes,
		}

		xattrBytes, err := currXAttr.Encode()
		if err == nil {
			if mcErr := s.fs.Memcache.Set(&memcache.Item{Key: "xattr:" + memlocal.Key(fullPath), Value: xattrBytes, Expiration: 0}); mcErr != nil {
				syslog.L.Debug().
					WithMessage("memcache set xattr failed").
					WithField("path", fullPath).
					WithField("error", mcErr.Error()).
					WithJob(s.fs.Job.ID).
					Write()
			} else {
				syslog.L.Debug().
					WithMessage("memcache set xattr").
					WithField("path", fullPath).
					WithJob(s.fs.Job.ID).
					Write()
			}
		} else {
			syslog.L.Debug().
				WithMessage("encode xattr failed").
				WithField("path", fullPath).
				WithField("error", err.Error()).
				WithJob(s.fs.Job.ID).
				Write()
		}
	}

	atomic.AddUint64(&s.curIdx, 1)
	newIdx := atomic.LoadUint64(&s.curIdx)
	atomic.AddUint64(&s.totalReturned, 1)
	newTotal := atomic.LoadUint64(&s.totalReturned)

	syslog.L.Debug().
		WithMessage("Next advanced indices").
		WithField("path", s.path).
		WithField("newCurIdx", newIdx).
		WithField("newTotalReturned", newTotal).
		WithJob(s.fs.Job.ID).
		Write()

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	if atomic.SwapInt32(&s.closed, 1) != 0 {
		syslog.L.Debug().
			WithMessage("Close called on already closed stream").
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Job.ID).
			Write()
		return
	}

	syslog.L.Debug().
		WithMessage("Closing DirStream").
		WithField("path", s.path).
		WithField("handleId", s.handleId).
		WithJob(s.fs.Job.ID).
		Write()

	closeReq := types.CloseReq{HandleID: s.handleId}
	_, err := s.fs.session.CallDataWithTimeout(1*time.Minute, s.fs.Job.ID+"/Close", &closeReq)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Job.ID).
			Write()
	} else {
		syslog.L.Debug().
			WithMessage("DirStream closed successfully").
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Job.ID).
			Write()
	}
}
