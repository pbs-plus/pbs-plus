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
	s.mu.Lock()
	defer s.mu.Unlock()

	syslog.L.Debug().
		WithMessage("HasNext called").
		WithField("path", s.path).
		WithField("closed", s.closed).
		WithField("curIdx", s.curIdx).
		WithField("totalReturned", s.totalReturned).
		WithField("maxDirEntries", s.fs.Job.MaxDirEntries).
		WithJob(s.fs.Job.ID).
		Write()

	if s.closed {
		syslog.L.Debug().
			WithMessage("HasNext early return: stream closed").
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	if s.totalReturned >= uint64(s.fs.Job.MaxDirEntries) {
		lastPath := ""
		if s.curIdx > 0 && int(s.curIdx) <= len(s.lastResp) {
			lastEntry := s.lastResp[s.curIdx-1]
			lastPath = lastEntry.Name
		}

		syslog.L.Error(fmt.Errorf("maximum directory entries reached: %d", s.fs.Job.MaxDirEntries)).
			WithField("path", s.path).
			WithField("lastFile", lastPath).
			WithJob(s.fs.Job.ID).
			Write()

		return false
	}

	if int(s.curIdx) < len(s.lastResp) {
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

	s.mu.Unlock()
	err := s.fs.session.Call(s.fs.Ctx, s.fs.Job.ID+"/ReadDir", &req, arpc.RawStreamHandler(func(st *quic.Stream) error {
		n, err := binarystream.ReceiveDataInto(st, readBuf)
		if err != nil {
			return err
		}
		bytesRead = n
		return nil
	}))
	s.mu.Lock()

	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			syslog.L.Debug().
				WithMessage("HasNext: process done received, closing dirstream").
				WithField("path", s.path).
				WithJob(s.fs.Job.ID).
				Write()
			s.closed = true
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
		s.closed = true
		return false
	}

	if err := s.lastResp.Decode(readBuf[:bytesRead]); err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithField("bytesRead", bytesRead).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	s.curIdx = 0
	postLen := len(s.lastResp)

	syslog.L.Debug().
		WithMessage("HasNext decoded batch").
		WithField("entries", postLen).
		WithField("path", s.path).
		WithJob(s.fs.Job.ID).
		Write()

	return postLen > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		syslog.L.Debug().
			WithMessage("Next called on closed stream").
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return fuse.DirEntry{}, syscall.EBADF
	}

	if int(s.curIdx) >= len(s.lastResp) {
		syslog.L.Error(fmt.Errorf("internal state error: index out of bounds in Next")).
			WithField("path", s.path).
			WithField("curIdx", s.curIdx).
			WithField("lastRespLen", len(s.lastResp)).
			WithJob(s.fs.Job.ID).
			Write()
		return fuse.DirEntry{}, syscall.EBADF
	}

	curr := s.lastResp[s.curIdx]

	syslog.L.Debug().
		WithMessage("Next returning entry").
		WithField("path", s.path).
		WithField("name", curr.Name).
		WithField("size", curr.Size).
		WithField("mode", curr.Mode).
		WithField("isDir", curr.IsDir).
		WithField("curIdx", s.curIdx).
		WithField("lastRespLen", len(s.lastResp)).
		WithField("totalReturned", s.totalReturned).
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

		if attrBytes, err := currAttr.Encode(); err == nil {
			if !currAttr.IsDir {
				atomic.AddInt64(&s.fs.FileCount, 1)
			} else {
				atomic.AddInt64(&s.fs.FolderCount, 1)
			}
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

		if xattrBytes, err := currXAttr.Encode(); err == nil {
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

	s.curIdx++
	s.totalReturned++

	syslog.L.Debug().
		WithMessage("Next advanced indices").
		WithField("path", s.path).
		WithField("newCurIdx", s.curIdx).
		WithField("newTotalReturned", s.totalReturned).
		WithJob(s.fs.Job.ID).
		Write()

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	s.mu.Lock()
	if s.closed {
		syslog.L.Debug().
			WithMessage("Close called on already closed stream").
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Job.ID).
			Write()
		s.mu.Unlock()
		return
	}
	s.closed = true

	syslog.L.Debug().
		WithMessage("Closing DirStream").
		WithField("path", s.path).
		WithField("handleId", s.handleId).
		WithJob(s.fs.Job.ID).
		Write()

	handleID := s.handleId
	jobID := s.fs.Job.ID
	s.mu.Unlock()

	closeReq := types.CloseReq{HandleID: handleID}
	_, err := s.fs.session.CallDataWithTimeout(1*time.Minute, jobID+"/Close", &closeReq)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithField("handleId", handleID).
			WithJob(jobID).
			Write()
	} else {
		syslog.L.Debug().
			WithMessage("DirStream closed successfully").
			WithField("path", s.path).
			WithField("handleId", handleID).
			WithJob(jobID).
			Write()
	}
}
