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
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 4*1024*1024)
	},
}

func (s *DirStream) HasNext() bool {
	log.Debug("hasNext called",

		"maxDirEntries", s.fs.Backup.MaxDirEntries, "entriesReturned", atomic.LoadUint64(&s.totalReturned), "curIdx", atomic.LoadUint64(&s.curIdx), "closed", atomic.LoadInt32(&s.closed), "path", s.path)

	if atomic.LoadInt32(&s.closed) != 0 {
		log.Debug("hasNext early return: stream closed",
			"path", s.path)

		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadUint64(&s.totalReturned) >= uint64(s.fs.Backup.MaxDirEntries) {
		if atomic.SwapInt32(&s.maxedOut, 1) == 0 {
			log.Warn("maximum directory entries limit reached - stopping enumeration for this directory",

				"maxDirEntries", s.fs.Backup.MaxDirEntries, "entriesReturned", atomic.LoadUint64(&s.totalReturned), "path", s.path)

		}
		return false
	}

	curIdx := atomic.LoadUint64(&s.curIdx)
	if int(curIdx) < len(s.lastResp) {
		log.Debug("hasNext hit in-memory entries",

			"lastRespLen", len(s.lastResp), "curIdx", curIdx, "path", s.path)

		return true
	}
	log.Debug("hasNext needs new batch - issuing ReadDir RPC",

		"handleId", s.handleId, "path", s.path)

	req := types.ReadDirReq{HandleID: s.handleId}
	readBuf := bufPool.Get().([]byte)
	defer bufPool.Put(readBuf)

	pipe, err := s.fs.getPipe(s.fs.Ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

		return false
	}

	bytesRead, err := pipe.CallBinary(s.fs.Ctx, "ReadDir", &req, readBuf)
	log.Debug("hasNext RPC completed",

		"path", s.path, "error", err, "bytesRead", bytesRead)

	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			log.Debug("hasNext: process done received, closing dirstream",

				"entriesReturned", atomic.LoadUint64(&s.totalReturned), "path", s.path)

		} else {
			log.Error(err,
				"HasNext: RPC error, closing dirstream",

				"entriesReturned", atomic.LoadUint64(&s.totalReturned), "handleId", s.handleId, "path", s.path)

		}
		return false
	}

	if bytesRead == 0 {
		log.Debug("hasNext: no bytes read, end of directory reached",

			"totalEntriesReturned", atomic.LoadUint64(&s.totalReturned), "path", s.path)

		return false
	}

	oldLen := len(s.lastResp)
	s.lastResp = nil
	log.Debug("hasNext: decoding new batch",

		"oldBatchLen", oldLen, "bytesRead", bytesRead, "path", s.path)

	err = s.cborDec.Unmarshal(readBuf[:bytesRead], &s.lastResp)
	if err != nil {
		log.Error(err,
			"HasNext: decode failed, closing dirstream",

			"entriesReturned", atomic.LoadUint64(&s.totalReturned), "bytesRead", bytesRead, "path", s.path)

		return false
	}

	newBatchLen := len(s.lastResp)
	log.Debug("hasNext decoded batch",

		"path", s.path, "entries", newBatchLen)

	if newBatchLen == 0 {
		log.Debug("hasNext: empty batch received, end of directory",

			"totalEntriesReturned", atomic.LoadUint64(&s.totalReturned), "path", s.path)

		return false
	}

	currentReturned := atomic.LoadUint64(&s.totalReturned)
	maxEntries := uint64(s.fs.Backup.MaxDirEntries)

	if currentReturned+uint64(newBatchLen) > maxEntries {
		allowedCount := maxEntries - currentReturned
		s.lastResp = s.lastResp[:allowedCount]
		log.Warn("hasNext: batch truncated to fit per-directory limit",

			"entriesSkipped", newBatchLen-int(allowedCount), "maxDirEntries", maxEntries, "currentReturned", currentReturned, "truncatedBatchSize", allowedCount, "originalBatchSize", newBatchLen, "path", s.path)

		newBatchLen = int(allowedCount)
	}

	atomic.StoreUint64(&s.curIdx, 0)
	log.Debug("hasNext: returning true with new batch",

		"curIdx", atomic.LoadUint64(&s.curIdx), "batchSize", newBatchLen, "path", s.path)

	return newBatchLen > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadInt32(&s.closed) != 0 {
		log.Debug("next called on closed stream",
			"path", s.path)

		return fuse.DirEntry{}, syscall.EBADF
	}

	if atomic.LoadInt32(&s.maxedOut) != 0 {
		log.Debug("next called on maxed out stream",

			"entriesReturned", atomic.LoadUint64(&s.totalReturned), "path", s.path)

		return fuse.DirEntry{}, syscall.EBADF
	}

	curIdxVal := atomic.LoadUint64(&s.curIdx)

	if int(curIdxVal) >= len(s.lastResp) {
		log.Error(fmt.Errorf("internal state error: index out of bounds in Next"), "", "lastRespLen", len(s.lastResp), "curIdx", curIdxVal, "path", s.path)

		return fuse.DirEntry{}, syscall.EBADF
	}

	curr := s.lastResp[curIdxVal]
	log.Debug("next returning entry",

		"entriesReturned", atomic.LoadUint64(&s.totalReturned), "lastRespLen", len(s.lastResp), "curIdx", curIdxVal, "isDir", curr.IsDir, "mode", curr.Mode, "size", curr.Size, "name", curr.Name, "path", s.path)

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
			log.Debug("memcache set attr failed",

				"error", mcErr.Error(), "path", fullPath)

		} else {
			log.Debug("memcache set attr",
				"path", fullPath)

		}
	} else {
		log.Debug("encode attr failed",

			"error", err.Error(), "path", fullPath)

	}

	currXAttr := types.AgentFileInfo{
		CreationTime:   curr.CreationTime,
		LastAccessTime: curr.LastAccessTime,
		LastWriteTime:  curr.LastWriteTime,
		FileAttributes: curr.FileAttributes,
	}

	if xattrBytes, err := cbor.Marshal(currXAttr); err == nil {
		if mcErr := s.fs.Memcache.Set(&memcache.Item{Key: xattrKey, Value: xattrBytes, Expiration: 0}); mcErr != nil {
			log.Debug("memcache set xattr failed",

				"error", mcErr.Error(), "path", fullPath)

		} else {
			log.Debug("memcache set xattr",
				"path", fullPath)

		}
	} else {
		log.Debug("encode xattr failed",

			"error", err.Error(), "path", fullPath)

	}

	atomic.AddUint64(&s.curIdx, 1)
	atomic.AddUint64(&s.totalReturned, 1)
	log.Debug("next advanced indices",

		"newEntriesReturned", atomic.LoadUint64(&s.totalReturned), "newCurIdx", atomic.LoadUint64(&s.curIdx), "path", s.path)

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	if atomic.SwapInt32(&s.closed, 1) != 0 {
		log.Debug("close called on already closed stream",

			"handleId", s.handleId, "path", s.path)

		return
	}
	log.Debug("closing DirStream",

		"totalEntriesReturned", atomic.LoadUint64(&s.totalReturned), "handleId", s.handleId, "path", s.path)

	ctxN, cancelN := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancelN()

	closeReq := types.CloseReq{HandleID: s.handleId}
	pipe, err := s.fs.getPipe(s.fs.Ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

		return
	}

	_, err = pipe.CallData(ctxN, "Close", &closeReq)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		log.Error(err,
			"DirStream close RPC failed",

			"handleId", s.handleId, "path", s.path)

	} else {
		log.Debug("dirStream closed successfully",

			"totalEntriesReturned", atomic.LoadUint64(&s.totalReturned), "handleId", s.handleId, "path", s.path)

	}
}
