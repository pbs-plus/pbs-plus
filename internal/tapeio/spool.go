package tapeio

import (
	"io"
	"os"
	"sync"
)

type spool struct {
	mu     sync.Mutex
	cv     *sync.Cond
	live   int64
	cap    int64
	closed bool
}

func newSpool(_ string, capBytes, _ int64) (*spool, error) {
	if capBytes <= 0 {
		capBytes = 1 << 30
	}
	s := &spool{cap: capBytes}
	s.cv = sync.NewCond(&s.mu)
	return s, nil
}

func (s *spool) reserve(size int64) error {
	s.mu.Lock()
	for !s.closed {
		if size > s.cap {
			if s.live == 0 {
				break
			}
		} else if s.live+size <= s.cap {
			break
		}
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

func (s *spool) read(r io.Reader) ([]byte, int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, err
	}
	return b, int64(len(b)), nil
}

func (s *spool) adjust(delta int64) {
	s.mu.Lock()
	s.live += delta
	s.cv.Signal()
	s.mu.Unlock()
}

func (s *spool) release(size int64) {
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
