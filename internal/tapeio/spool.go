package tapeio

import (
	"io"
	"os"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

type spool struct {
	f  *os.File
	mu sync.Mutex
	cv *sync.Cond

	writeOff int64
	readOff  int64
	outBytes int64
	capBytes int64

	closed bool
}

func newSpool(capBytes int64) (*spool, error) {
	f, err := os.CreateTemp("", "bkf2pxar-spool-*")
	if err != nil {
		return nil, err
	}
	s := &spool{f: f, capBytes: capBytes}
	s.cv = sync.NewCond(&s.mu)
	return s, nil
}

func (s *spool) write(r io.Reader, size int64) (int64, error) {
	s.mu.Lock()
	for s.outBytes+size > s.capBytes && !s.closed {
		s.cv.Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return 0, os.ErrClosed
	}
	off := s.writeOff
	s.writeOff += size
	s.outBytes += size
	s.mu.Unlock()

	if _, err := io.Copy(io.NewOffsetWriter(s.f, off), io.LimitReader(r, size)); err != nil {
		return 0, err
	}
	return off, nil
}

func (s *spool) reader(off, size int64) io.Reader {
	return io.NewSectionReader(s.f, off, size)
}

func (s *spool) consume(size int64) {
	s.mu.Lock()
	s.readOff += size
	s.outBytes -= size
	if s.readOff >= s.capBytes/2 && !s.closed {
		if err := s.compactLocked(); err != nil {
			log.Error(err, "")
		}
	}
	s.cv.Signal()
	s.mu.Unlock()
}

func (s *spool) compactLocked() error {
	live := s.writeOff - s.readOff
	if s.readOff <= 0 || live <= 0 {
		return nil
	}
	buf := make([]byte, 32<<20)
	src := io.NewSectionReader(s.f, s.readOff, live)
	dst := io.NewOffsetWriter(s.f, 0)
	if _, err := io.CopyBuffer(dst, src, buf); err != nil {
		return err
	}
	s.writeOff = live
	s.readOff = 0
	return s.f.Truncate(live)
}

func (s *spool) close() error {
	s.mu.Lock()
	s.closed = true
	s.cv.Broadcast()
	s.mu.Unlock()
	err := s.f.Close()
	if rerr := os.Remove(s.f.Name()); err == nil {
		err = rerr
	}
	return err
}
