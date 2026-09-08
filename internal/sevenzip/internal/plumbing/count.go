package plumbing

import (
	"sync/atomic"
)

// WriteCounter is an io.Writer that simply counts the number of bytes written
// to it.
type WriteCounter struct {
	count atomic.Uint64
}

func (wc *WriteCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.count.Add(uint64(n))

	return n, nil
}

// Count returns the number of bytes written.
func (wc *WriteCounter) Count() uint64 {
	return wc.count.Load()
}
