//go:build linux

package pxar

import (
	"sync"
	"testing"
)

const poolBufSize = 1024 * 1024

func BenchmarkReadBufPoolFixed(b *testing.B) {
	pool := &sync.Pool{
		New: func() any { b := make([]byte, poolBufSize); return &b },
	}
	b.ReportAllocs()
	for b.Loop() {
		bp := pool.Get().(*[]byte)
		buf := *bp
		if len(buf) < 2*poolBufSize {
			pool.Put(bp)
			buf = make([]byte, 2*poolBufSize)
		} else {
			pool.Put(bp)
		}
		sink = buf
	}
}

func BenchmarkReadBufPoolLeak(b *testing.B) {
	pool := &sync.Pool{
		New: func() any { b := make([]byte, poolBufSize); return &b },
	}
	b.ReportAllocs()
	for b.Loop() {
		bp := pool.Get().(*[]byte)
		buf := *bp
		if len(buf) < 2*poolBufSize {
			buf = make([]byte, 2*poolBufSize)
		} else {
			pool.Put(bp)
		}
		sink = buf
	}
}

var sink []byte
