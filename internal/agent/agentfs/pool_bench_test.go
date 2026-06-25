package agentfs

import (
	"bytes"
	"io"
	"sync"
	"testing"
)

const readBufSize = 1024 * 1024

func BenchmarkOldPreadBuffer(b *testing.B) {
	pool := &sync.Pool{
		New: func() any { b := make([]byte, readBufSize); return &b },
	}
	sinkBuf := make([]byte, 2*readBufSize)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		bp := pool.Get().(*[]byte)
		workBuf := *bp
		reqLen := 2 * readBufSize
		if len(workBuf) < reqLen {
			workBuf = make([]byte, reqLen)
		} else {
			pool.Put(bp)
		}
		n := copy(workBuf, sinkBuf)
		_ = bytes.NewReader(workBuf[:n])
		if len(*bp) < reqLen {
		}
	}
}

func BenchmarkStreamApproach(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sr := io.NewSectionReader(sinkReader{}, 0, 2*readBufSize)
		_ = sr
	}
}

type sinkReader struct{}

func (sinkReader) ReadAt(p []byte, off int64) (int, error) {
	return len(p), nil
}

var _ io.ReaderAt = sinkReader{}
