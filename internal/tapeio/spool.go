package tapeio

import (
	"io"
	"os"
	"sync"
	"syscall"
)

type spool struct {
	dir string
	mu  sync.Mutex
	cv  *sync.Cond

	live   int64
	cap    int64
	margin int64
	closed bool
}

func newSpool(dir string, capBytes, margin int64) (*spool, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	if margin < 0 {
		margin = 0
	}
	s := &spool{dir: dir, cap: capBytes, margin: margin}
	s.cv = sync.NewCond(&s.mu)
	return s, nil
}

func (s *spool) freeSpace() int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &st); err != nil {
		return 1<<62 - 1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

func (s *spool) reserve(size int64) error {
	s.mu.Lock()
	for !s.closed && (s.live+size > s.cap || s.freeSpace()-size < s.margin) {
		s.cv.Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return os.ErrClosed
	}
	s.live += size
	s.mu.Unlock()
	return nil
}

func (s *spool) write(r io.Reader) (f *os.File, n int64, err error) {
	f, err = os.CreateTemp(s.dir, "bkf2pxar-blob-*")
	if err != nil {
		return nil, 0, err
	}
	n, err = io.Copy(f, r)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, 0, err
	}
	return f, n, nil
}

func (s *spool) adjust(delta int64) {
	s.mu.Lock()
	s.live += delta
	s.cv.Signal()
	s.mu.Unlock()
}

func (s *spool) consume(f *os.File, size int64) {
	_ = f.Close()
	_ = os.Remove(f.Name())
	s.mu.Lock()
	s.live -= size
	s.cv.Signal()
	s.mu.Unlock()
}

func (s *spool) close() error {
	s.mu.Lock()
	s.closed = true
	s.cv.Broadcast()
	s.mu.Unlock()
	return nil
}
