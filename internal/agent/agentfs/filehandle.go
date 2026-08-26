package agentfs

import (
	"os"
	"sync"
	"syscall"
	"time"
)

type FileHandle struct {
	file          *os.File
	fileSize      int64
	isDir         bool
	dirReader     *DirReader
	logicalOffset int64

	mu        sync.Mutex
	activeOps int32
	closing   bool
	closeDone chan struct{}
}

func NewFileHandle(handle *os.File) *FileHandle {
	return &FileHandle{
		file: handle,
	}
}

func (fh *FileHandle) releaseOp() {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.activeOps--
	if fh.activeOps == 0 && fh.closing {
		// Close channel outside the lock
		go func() { close(fh.closeDone) }()
	}
}

func (fh *FileHandle) acquireOp() bool {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closing {
		return false
	}
	fh.activeOps++
	return true
}

func wrapPathError(op, path string, err error) error {
	if _, ok := err.(*os.PathError); ok {
		return err
	}
	if errno, ok := err.(syscall.Errno); ok {
		return &os.PathError{Op: op, Path: path, Err: errno}
	}
	return &os.PathError{Op: op, Path: path, Err: err}
}
func (fh *FileHandle) waitForOps(timeout time.Duration) bool {
	fh.mu.Lock()
	done := fh.closeDone
	fh.mu.Unlock()

	if done == nil {
		return true
	}

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (fh *FileHandle) beginClose() bool {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.closing {
		return false
	}
	fh.closing = true
	fh.closeDone = make(chan struct{})
	if fh.activeOps == 0 {
		close(fh.closeDone)
	}
	return true
}
