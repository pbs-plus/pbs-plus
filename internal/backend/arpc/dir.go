//go:build linux

package arpcfs

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type DirStream struct {
	fs            *ARPCFS
	path          string
	handleId      types.FileHandleId
	closed        int32          // Use int32 instead of atomic.Bool
	lastResp      unsafe.Pointer // Atomic pointer to types.ReadDirEntries
	curIdx        uint64         // Use uint64 instead of atomic.Uint64
	totalReturned uint64         // Use uint64 instead of atomic.Uint64
	readBuf       []byte

	reusableEntry   fuse.DirEntry
	modeCache       [3]uint32 // Cache for common mode bits
	fetchInProgress int32     // Atomic flag to prevent concurrent fetches
}

// Initialize mode cache for faster lookups
func (s *DirStream) initModeCache() {
	s.modeCache[0] = fuse.S_IFREG // Regular file
	s.modeCache[1] = fuse.S_IFDIR // Directory
	s.modeCache[2] = fuse.S_IFLNK // Symlink
}

// Helper methods for atomic operations
func (s *DirStream) isClosed() bool {
	return atomic.LoadInt32(&s.closed) != 0
}

func (s *DirStream) setClosed() bool {
	return atomic.SwapInt32(&s.closed, 1) != 0
}

func (s *DirStream) loadCurIdx() uint64 {
	return atomic.LoadUint64(&s.curIdx)
}

func (s *DirStream) storeCurIdx(val uint64) {
	atomic.StoreUint64(&s.curIdx, val)
}

func (s *DirStream) addCurIdx(delta uint64) {
	atomic.AddUint64(&s.curIdx, delta)
}

func (s *DirStream) loadTotalReturned() uint64 {
	return atomic.LoadUint64(&s.totalReturned)
}

func (s *DirStream) addTotalReturned(delta uint64) {
	atomic.AddUint64(&s.totalReturned, delta)
}

// Atomic operations for lastResp
func (s *DirStream) loadLastResp() *types.ReadDirEntries {
	ptr := atomic.LoadPointer(&s.lastResp)
	if ptr == nil {
		return nil
	}
	return (*types.ReadDirEntries)(ptr)
}

func (s *DirStream) storeLastResp(entries *types.ReadDirEntries) {
	atomic.StorePointer(&s.lastResp, unsafe.Pointer(entries))
}

func (s *DirStream) tryStartFetch() bool {
	return atomic.CompareAndSwapInt32(&s.fetchInProgress, 0, 1)
}

func (s *DirStream) finishFetch() {
	atomic.StoreInt32(&s.fetchInProgress, 0)
}

func (s *DirStream) HasNext() bool {
	if s.isClosed() {
		return false
	}

	if s.loadTotalReturned() >= uint64(s.fs.Job.MaxDirEntries) {
		lastPath := s.getLastPath()

		syslog.L.Error(fmt.Errorf("maximum directory entries reached: %d", s.fs.Job.MaxDirEntries)).
			WithField("path", s.path).
			WithField("lastFile", lastPath).
			WithJob(s.fs.Job.ID).
			Write()

		return false
	}

	// Check if we have current entry without locking
	lastResp := s.loadLastResp()
	if lastResp != nil && int(s.loadCurIdx()) < len(*lastResp) {
		return true
	}

	return s.fetchNextBatch()
}

func (s *DirStream) getLastPath() string {
	lastResp := s.loadLastResp()
	if lastResp == nil {
		return ""
	}

	curIdxVal := s.loadCurIdx()
	if curIdxVal > 0 && int(curIdxVal) <= len(*lastResp) {
		// Direct reference to avoid string copy
		return (*lastResp)[curIdxVal-1].Name
	}
	return ""
}

func (s *DirStream) fetchNextBatch() bool {
	// Prevent concurrent fetches
	if !s.tryStartFetch() {
		// Another goroutine is fetching, wait and check again
		for atomic.LoadInt32(&s.fetchInProgress) != 0 {
			// Busy wait with small delay
			time.Sleep(time.Microsecond)
		}
		// Check if we now have data
		lastResp := s.loadLastResp()
		return lastResp != nil && int(s.loadCurIdx()) < len(*lastResp)
	}
	defer s.finishFetch()

	req := types.ReadDirReq{HandleID: s.handleId}

	bytesRead, err := s.fs.session.CallBinary(s.fs.ctx, s.fs.Job.ID+"/ReadDir", &req, s.readBuf)
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

	var entries types.ReadDirEntries

	// Reuse existing capacity if possible
	oldResp := s.loadLastResp()
	if oldResp != nil && cap(*oldResp) > 0 {
		entries = (*oldResp)[:0] // Reset length but keep capacity
	}

	err = entries.Decode(s.readBuf[:bytesRead])
	if err != nil {
		syslog.L.Error(err).
			WithField("path", s.path).
			WithJob(s.fs.Job.ID).
			Write()
		return false
	}

	// Atomically update the response and reset index
	s.storeLastResp(&entries)
	s.storeCurIdx(0)

	return len(entries) > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if s.isClosed() {
		return fuse.DirEntry{}, syscall.EBADF
	}

	lastResp := s.loadLastResp()
	if lastResp == nil {
		return fuse.DirEntry{}, syscall.EBADF
	}

	curIdxVal := s.loadCurIdx()

	if int(curIdxVal) >= len(*lastResp) {
		syslog.L.Error(fmt.Errorf("internal state error: index out of bounds in Next")).
			WithField("path", s.path).
			WithField("curIdx", curIdxVal).
			WithField("lastRespLen", len(*lastResp)).
			WithJob(s.fs.Job.ID).
			Write()
		return fuse.DirEntry{}, syscall.EBADF
	}

	curr := (*lastResp)[curIdxVal]

	modeBits := s.calculateModeBits(os.FileMode(curr.Mode))

	s.addCurIdx(1)
	s.addTotalReturned(1)

	s.reusableEntry.Name = curr.Name
	s.reusableEntry.Mode = modeBits

	return s.reusableEntry, 0
}

// Optimized mode calculation using cached values and bit operations
func (s *DirStream) calculateModeBits(mode os.FileMode) uint32 {
	// Use bit operations for faster mode detection
	if mode&os.ModeDir != 0 {
		return s.modeCache[1] // Directory
	}
	if mode&os.ModeSymlink != 0 {
		return s.modeCache[2] // Symlink
	}
	return s.modeCache[0] // Regular file (default)
}

func (s *DirStream) Close() {
	if s.setClosed() {
		return // Already closed
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
