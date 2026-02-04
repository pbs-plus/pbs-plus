//go:build linux

package arpcfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/fxamacker/cbor/v2"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
		WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
		WithField("maxDirEntries", s.fs.Backup.MaxDirEntries).
		WithJob(s.fs.Backup.ID).
		Write()

	if atomic.LoadInt32(&s.closed) != 0 {
		syslog.L.Debug().
			WithMessage("HasNext early return: stream closed").
			WithField("path", s.path).
			WithJob(s.fs.Backup.ID).
			Write()
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadUint64(&s.totalReturned) >= uint64(s.fs.Backup.MaxDirEntries) {
		if atomic.SwapInt32(&s.maxedOut, 1) == 0 {
			syslog.L.Warn().
				WithMessage("maximum directory entries limit reached - stopping enumeration for this directory").
				WithField("path", s.path).
				WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
				WithField("maxDirEntries", s.fs.Backup.MaxDirEntries).
				WithJob(s.fs.Backup.ID).
				Write()
		}
		return false
	}

	curIdx := atomic.LoadUint64(&s.curIdx)
	if int(curIdx) < len(s.lastResp) {
		syslog.L.Debug().
			WithMessage("HasNext hit in-memory entries").
			WithField("path", s.path).
			WithField("curIdx", curIdx).
			WithField("lastRespLen", len(s.lastResp)).
			WithJob(s.fs.Backup.ID).
			Write()
		return true
	}

	syslog.L.Debug().
		WithMessage("HasNext needs new batch - issuing ReadDir RPC").
		WithField("path", s.path).
		WithField("handleId", s.handleId).
		WithJob(s.fs.Backup.ID).
		Write()

	req := types.ReadDirReq{HandleID: s.handleId}
	readBuf := bufPool.Get().([]byte)
	defer bufPool.Put(readBuf)

	pipe, err := s.fs.getPipe(s.fs.Ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(s.fs.Backup.ID).
			Write()
		return false
	}

	bytesRead, err := pipe.CallBinary(s.fs.Ctx, "ReadDir", &req, readBuf)
	syslog.L.Debug().
		WithMessage("HasNext RPC completed").
		WithField("bytesRead", bytesRead).
		WithField("error", err).
		WithField("path", s.path).
		WithJob(s.fs.Backup.ID).
		Write()

	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			syslog.L.Debug().
				WithMessage("HasNext: process done received, closing dirstream").
				WithField("path", s.path).
				WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
				WithJob(s.fs.Backup.ID).
				Write()
		} else {
			syslog.L.Error(err).
				WithMessage("HasNext: RPC error, closing dirstream").
				WithField("path", s.path).
				WithField("handleId", s.handleId).
				WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
				WithJob(s.fs.Backup.ID).
				Write()
		}
		return false
	}

	if bytesRead == 0 {
		syslog.L.Debug().
			WithMessage("HasNext: no bytes read, end of directory reached").
			WithField("path", s.path).
			WithField("totalEntriesReturned", atomic.LoadUint64(&s.totalReturned)).
			WithJob(s.fs.Backup.ID).
			Write()
		return false
	}

	oldLen := len(s.lastResp)
	s.lastResp = nil

	syslog.L.Debug().
		WithMessage("HasNext: decoding new batch").
		WithField("path", s.path).
		WithField("bytesRead", bytesRead).
		WithField("oldBatchLen", oldLen).
		WithJob(s.fs.Backup.ID).
		Write()

	err = cbor.Unmarshal(readBuf[:bytesRead], &s.lastResp)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("HasNext: decode failed, closing dirstream").
			WithField("path", s.path).
			WithField("bytesRead", bytesRead).
			WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
			WithJob(s.fs.Backup.ID).
			Write()
		return false
	}

	newBatchLen := len(s.lastResp)

	syslog.L.Debug().
		WithMessage("HasNext decoded batch").
		WithField("entries", newBatchLen).
		WithField("path", s.path).
		WithJob(s.fs.Backup.ID).
		Write()

	if newBatchLen == 0 {
		syslog.L.Debug().
			WithMessage("HasNext: empty batch received, end of directory").
			WithField("path", s.path).
			WithField("totalEntriesReturned", atomic.LoadUint64(&s.totalReturned)).
			WithJob(s.fs.Backup.ID).
			Write()
		return false
	}

	currentReturned := atomic.LoadUint64(&s.totalReturned)
	maxEntries := uint64(s.fs.Backup.MaxDirEntries)

	if currentReturned+uint64(newBatchLen) > maxEntries {
		allowedCount := maxEntries - currentReturned
		s.lastResp = s.lastResp[:allowedCount]
		syslog.L.Warn().
			WithMessage("HasNext: batch truncated to fit per-directory limit").
			WithField("path", s.path).
			WithField("originalBatchSize", newBatchLen).
			WithField("truncatedBatchSize", allowedCount).
			WithField("currentReturned", currentReturned).
			WithField("maxDirEntries", maxEntries).
			WithField("entriesSkipped", newBatchLen-int(allowedCount)).
			WithJob(s.fs.Backup.ID).
			Write()
		newBatchLen = int(allowedCount)
	}

	atomic.StoreUint64(&s.curIdx, 0)

	syslog.L.Debug().
		WithMessage("HasNext: returning true with new batch").
		WithField("path", s.path).
		WithField("batchSize", newBatchLen).
		WithField("curIdx", atomic.LoadUint64(&s.curIdx)).
		WithJob(s.fs.Backup.ID).
		Write()

	return newBatchLen > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadInt32(&s.closed) != 0 {
		syslog.L.Debug().
			WithMessage("Next called on closed stream").
			WithField("path", s.path).
			WithJob(s.fs.Backup.ID).
			Write()
		return fuse.DirEntry{}, syscall.EBADF
	}

	if atomic.LoadInt32(&s.maxedOut) != 0 {
		syslog.L.Debug().
			WithMessage("Next called on maxed out stream").
			WithField("path", s.path).
			WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
			WithJob(s.fs.Backup.ID).
			Write()
		return fuse.DirEntry{}, syscall.EBADF
	}

	curIdxVal := atomic.LoadUint64(&s.curIdx)

	if int(curIdxVal) >= len(s.lastResp) {
		syslog.L.Error(fmt.Errorf("internal state error: index out of bounds in Next")).
			WithField("path", s.path).
			WithField("curIdx", curIdxVal).
			WithField("lastRespLen", len(s.lastResp)).
			WithJob(s.fs.Backup.ID).
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
		WithField("entriesReturned", atomic.LoadUint64(&s.totalReturned)).
		WithJob(s.fs.Backup.ID).
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

	attrKey := s.fs.GetCacheKey(attrPrefix, fullPath)
	xattrKey := s.fs.GetCacheKey(xattrPrefix, fullPath)

	currAttr := types.AgentFileInfo{
		Name:    curr.Name,
		Size:    curr.Size,
		Mode:    curr.Mode,
		ModTime: curr.ModTime,
		IsDir:   curr.IsDir,
	}

	if attrBytes, err := cbor.Marshal(currAttr); err == nil {
		if !currAttr.IsDir {
			s.fs.FileCount.Add(1)
		} else {
			s.fs.FolderCount.Add(1)
		}
		if mcErr := s.fs.Memcache.Set(&memcache.Item{Key: attrKey, Value: attrBytes, Expiration: 0}); mcErr != nil {
			syslog.L.Debug().
				WithMessage("memcache set attr failed").
				WithField("path", fullPath).
				WithField("error", mcErr.Error()).
				WithJob(s.fs.Backup.ID).
				Write()
		} else {
			syslog.L.Debug().
				WithMessage("memcache set attr").
				WithField("path", fullPath).
				WithJob(s.fs.Backup.ID).
				Write()
		}
	} else {
		syslog.L.Debug().
			WithMessage("encode attr failed").
			WithField("path", fullPath).
			WithField("error", err.Error()).
			WithJob(s.fs.Backup.ID).
			Write()
	}

	currXAttr := types.AgentFileInfo{
		CreationTime:   curr.CreationTime,
		LastAccessTime: curr.LastAccessTime,
		LastWriteTime:  curr.LastWriteTime,
		FileAttributes: curr.FileAttributes,
	}

	if xattrBytes, err := cbor.Marshal(currXAttr); err == nil {
		if mcErr := s.fs.Memcache.Set(&memcache.Item{Key: xattrKey, Value: xattrBytes, Expiration: 0}); mcErr != nil {
			syslog.L.Debug().
				WithMessage("memcache set xattr failed").
				WithField("path", fullPath).
				WithField("error", mcErr.Error()).
				WithJob(s.fs.Backup.ID).
				Write()
		} else {
			syslog.L.Debug().
				WithMessage("memcache set xattr").
				WithField("path", fullPath).
				WithJob(s.fs.Backup.ID).
				Write()
		}
	} else {
		syslog.L.Debug().
			WithMessage("encode xattr failed").
			WithField("path", fullPath).
			WithField("error", err.Error()).
			WithJob(s.fs.Backup.ID).
			Write()
	}

	atomic.AddUint64(&s.curIdx, 1)
	atomic.AddUint64(&s.totalReturned, 1)

	syslog.L.Debug().
		WithMessage("Next advanced indices").
		WithField("path", s.path).
		WithField("newCurIdx", atomic.LoadUint64(&s.curIdx)).
		WithField("newEntriesReturned", atomic.LoadUint64(&s.totalReturned)).
		WithJob(s.fs.Backup.ID).
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
			WithJob(s.fs.Backup.ID).
			Write()
		return
	}

	syslog.L.Debug().
		WithMessage("Closing DirStream").
		WithField("path", s.path).
		WithField("handleId", s.handleId).
		WithField("totalEntriesReturned", atomic.LoadUint64(&s.totalReturned)).
		WithJob(s.fs.Backup.ID).
		Write()

	ctxN, cancelN := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancelN()

	closeReq := types.CloseReq{HandleID: s.handleId}
	pipe, err := s.fs.getPipe(s.fs.Ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(s.fs.Backup.ID).
			Write()
		return
	}

	_, err = pipe.CallData(ctxN, "Close", &closeReq)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		syslog.L.Error(err).
			WithMessage("DirStream close RPC failed").
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithJob(s.fs.Backup.ID).
			Write()
	} else {
		syslog.L.Debug().
			WithMessage("DirStream closed successfully").
			WithField("path", s.path).
			WithField("handleId", s.handleId).
			WithField("totalEntriesReturned", atomic.LoadUint64(&s.totalReturned)).
			WithJob(s.fs.Backup.ID).
			Write()
	}
}
