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
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 4*1024*1024)
	},
}

func (s *DirStream) HasNext() bool {
	log.Debug("hasNext called",

		"maxDirEntries", s.fs.Backup.MaxDirEntries, "entriesReturned", s.totalReturned.Load(), "curIdx", s.curIdx.Load(), "closed", s.closed.Load(), "path", s.path)

	if s.closed.Load() != 0 {
		log.Debug("hasNext early return: stream closed",
			"path", s.path)

		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalReturned.Load() >= uint64(s.fs.Backup.MaxDirEntries) {
		if s.maxedOut.Swap(1) == 0 {
			log.Warn("maximum directory entries limit reached - stopping enumeration for this directory",

				"maxDirEntries", s.fs.Backup.MaxDirEntries, "entriesReturned", s.totalReturned.Load(), "path", s.path)

		}
		return false
	}

	curIdx := s.curIdx.Load()
	if int(curIdx) < len(s.lastResp) {
		log.Debug("hasNext hit in-memory entries",

			"lastRespLen", len(s.lastResp), "curIdx", curIdx, "path", s.path)

		return true
	}
	log.Debug("hasNext needs new batch - issuing ReadDir RPC",

		"handleId", s.handleId, "path", s.path)

	entries, err := s.nextBatch()
	log.Debug("hasNext RPC completed",

		"path", s.path, "error", err)

	if err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			log.Debug("hasNext: process done received, closing dirstream",

				"entriesReturned", s.totalReturned.Load(), "path", s.path)

		} else {
			log.Error(err,
				"HasNext: RPC error, closing dirstream",

				"entriesReturned", s.totalReturned.Load(), "handleId", s.handleId, "path", s.path)

		}
		return false
	}

	oldLen := len(s.lastResp)
	s.lastResp = entries
	log.Debug("hasNext: decoding new batch",

		"oldBatchLen", oldLen, "path", s.path)

	newBatchLen := len(s.lastResp)
	log.Debug("hasNext decoded batch",

		"path", s.path, "entries", newBatchLen)

	if newBatchLen == 0 {
		log.Debug("hasNext: empty batch received, end of directory",

			"totalEntriesReturned", s.totalReturned.Load(), "path", s.path)

		return false
	}

	currentReturned := s.totalReturned.Load()
	maxEntries := uint64(s.fs.Backup.MaxDirEntries)

	if currentReturned+uint64(newBatchLen) > maxEntries {
		allowedCount := maxEntries - currentReturned
		s.lastResp = s.lastResp[:allowedCount]
		log.Warn("hasNext: batch truncated to fit per-directory limit",

			"entriesSkipped", newBatchLen-int(allowedCount), "maxDirEntries", maxEntries, "currentReturned", currentReturned, "truncatedBatchSize", allowedCount, "originalBatchSize", newBatchLen, "path", s.path)

		newBatchLen = int(allowedCount)
	}

	s.curIdx.Store(0)
	if currentReturned+uint64(newBatchLen) < maxEntries {
		s.startPrefetch()
	}
	log.Debug("hasNext: returning true with new batch",

		"curIdx", s.curIdx.Load(), "batchSize", newBatchLen, "path", s.path)

	return newBatchLen > 0
}

func (s *DirStream) fetchBatch(ctx context.Context) (fswire.ReadDirEntries, error) {
	if s.fetch != nil {
		return s.fetch(ctx)
	}

	req := fswire.ReadDirReq{HandleID: s.handleId}
	readBuf := bufPool.Get().([]byte)
	defer bufPool.Put(readBuf)

	pipe, err := s.fs.getPipe(ctx)
	if err != nil {
		return nil, err
	}

	bytesRead, err := pipe.CallBinary(ctx, "ReadDir", &req, readBuf)
	if err != nil {
		return nil, err
	}
	if bytesRead == 0 {
		return nil, os.ErrProcessDone
	}

	var entries fswire.ReadDirEntries
	if err := s.cborDec.Unmarshal(readBuf[:bytesRead], &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *DirStream) nextBatch() (fswire.ReadDirEntries, error) {
	s.prefetchMu.Lock()
	resultCh := s.prefetch
	cancel := s.prefetchCancel
	s.prefetch = nil
	s.prefetchCancel = nil
	s.prefetchMu.Unlock()

	if resultCh == nil {
		return s.fetchBatch(s.fs.Ctx)
	}
	defer cancel()
	result := <-resultCh
	return result.entries, result.err
}

func (s *DirStream) startPrefetch() {
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	if s.closed.Load() != 0 || s.maxedOut.Load() != 0 || s.prefetch != nil {
		return
	}

	ctx, cancel := context.WithCancel(s.fs.Ctx)
	resultCh := make(chan dirBatchResult, 1)
	s.prefetch = resultCh
	s.prefetchCancel = cancel
	s.prefetchWG.Go(func() {
		entries, err := s.fetchBatch(ctx)
		resultCh <- dirBatchResult{entries: entries, err: err}
		close(resultCh)
	})
}

func (s *DirStream) stopPrefetch() {
	s.prefetchMu.Lock()
	if s.prefetchCancel != nil {
		s.prefetchCancel()
	}
	s.prefetch = nil
	s.prefetchCancel = nil
	s.prefetchMu.Unlock()
	s.prefetchWG.Wait()
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() != 0 {
		log.Debug("next called on closed stream",
			"path", s.path)

		return fuse.DirEntry{}, syscall.EBADF
	}

	if s.maxedOut.Load() != 0 {
		log.Debug("next called on maxed out stream",

			"entriesReturned", s.totalReturned.Load(), "path", s.path)

		return fuse.DirEntry{}, syscall.EBADF
	}

	curIdxVal := s.curIdx.Load()

	if int(curIdxVal) >= len(s.lastResp) {
		log.Error(fmt.Errorf("internal state error: index out of bounds in Next"), "", "lastRespLen", len(s.lastResp), "curIdx", curIdxVal, "path", s.path)

		return fuse.DirEntry{}, syscall.EBADF
	}

	curr := s.lastResp[curIdxVal]
	log.Debug("next returning entry",

		"entriesReturned", s.totalReturned.Load(), "lastRespLen", len(s.lastResp), "curIdx", curIdxVal, "isDir", curr.IsDir, "mode", curr.Mode, "size", curr.Size, "name", curr.Name, "path", s.path)

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

	currAttr := fswire.AgentFileInfo{
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

	currXAttr := fswire.AgentFileInfo{
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

	s.curIdx.Add(1)
	s.totalReturned.Add(1)
	log.Debug("next advanced indices",

		"newEntriesReturned", s.totalReturned.Load(), "newCurIdx", s.curIdx.Load(), "path", s.path)

	return fuse.DirEntry{
		Name: curr.Name,
		Mode: modeBits,
	}, 0
}

func (s *DirStream) Close() {
	if s.closed.Swap(1) != 0 {
		log.Debug("close called on already closed stream",

			"handleId", s.handleId, "path", s.path)

		return
	}
	s.stopPrefetch()
	log.Debug("closing DirStream",

		"totalEntriesReturned", s.totalReturned.Load(), "handleId", s.handleId, "path", s.path)

	ctxN, cancelN := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancelN()

	closeReq := fswire.CloseReq{HandleID: s.handleId}
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

			"totalEntriesReturned", s.totalReturned.Load(), "handleId", s.handleId, "path", s.path)

	}
}

type dirBatchResult struct {
	entries fswire.ReadDirEntries
	err     error
}

type DirStream struct {
	fs            *ARPCFS
	path          string
	handleId      fswire.FileHandleID
	closed        atomic.Int32
	maxedOut      atomic.Int32
	mu            sync.Mutex
	lastResp      fswire.ReadDirEntries
	curIdx        atomic.Uint64
	totalReturned atomic.Uint64
	cborDec       cbor.DecMode
	fetch         func(context.Context) (fswire.ReadDirEntries, error)
	prefetchMu     sync.Mutex
	prefetchWG     sync.WaitGroup
	prefetch       <-chan dirBatchResult
	prefetchCancel context.CancelFunc
}
