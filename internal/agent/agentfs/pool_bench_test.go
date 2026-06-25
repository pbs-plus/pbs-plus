package agentfs

import (
	"runtime"
	"sync"
	"testing"
)

const poolBufSize = 1024 * 1024

func TestPoolLeakCumulative(t *testing.T) {
	pool := &sync.Pool{
		New: func() any { b := make([]byte, poolBufSize); return &b },
	}

	for range 100 {
		bp := pool.Get().(*[]byte)
		pool.Put(bp)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for range 1000 {
		bp := pool.Get().(*[]byte)
		buf := *bp
		if len(buf) < 2*poolBufSize {
			buf = make([]byte, 2*poolBufSize)
		} else {
			pool.Put(bp)
		}
		_ = buf
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if after.HeapInuse > before.HeapInuse {
		diffMB := (after.HeapInuse - before.HeapInuse) / 1024 / 1024
		t.Logf("OLD pattern: heap grew by %d MB after 1000 iterations", diffMB)
	}
	t.Logf("before: %.1f MB, after: %.1f MB", float64(before.HeapInuse)/1024/1024, float64(after.HeapInuse)/1024/1024)
}

func TestPoolFixedCumulative(t *testing.T) {
	pool := &sync.Pool{
		New: func() any { b := make([]byte, poolBufSize); return &b },
	}

	for range 100 {
		bp := pool.Get().(*[]byte)
		pool.Put(bp)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for range 1000 {
		bp := pool.Get().(*[]byte)
		buf := *bp
		if len(buf) < 2*poolBufSize {
			pool.Put(bp)
			buf = make([]byte, 2*poolBufSize)
		} else {
			pool.Put(bp)
		}
		_ = buf
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	diffMB := max(int64(after.HeapInuse)-int64(before.HeapInuse), 0)
	t.Logf("FIXED pattern: heap delta %d MB after 1000 iterations", diffMB/1024/1024)
	t.Logf("before: %.1f MB, after: %.1f MB", float64(before.HeapInuse)/1024/1024, float64(after.HeapInuse)/1024/1024)
}

func BenchmarkPoolFixedAllocs(b *testing.B) {
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

var sink []byte

// BenchmarkPoolLeakAllocs shows the leak: pool buffer lost per op
func BenchmarkPoolLeakAllocs(b *testing.B) {
	pool := &sync.Pool{
		New: func() any { b := make([]byte, poolBufSize); return &b },
	}
	b.ReportAllocs()
	for b.Loop() {
		bp := pool.Get().(*[]byte)
		buf := *bp
		if len(buf) < 2*poolBufSize {
			buf = make([]byte, 2*poolBufSize)
			// LEAK: bp never returned
		} else {
			pool.Put(bp)
		}
		sink = buf
	}
}
