//go:build windows

package agentfs

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// MemStats captures memory statistics for comparison
type MemStats struct {
	Alloc      uint64
	TotalAlloc uint64
	Sys        uint64
	NumGC      uint32
	HeapAlloc  uint64
	HeapInuse  uint64
}

// captureMemStats returns current memory statistics
func captureMemStats() MemStats {
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.GC()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MemStats{
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
		HeapAlloc:  m.HeapAlloc,
		HeapInuse:  m.HeapInuse,
	}
}

// memoryDelta calculates the difference between two memory snapshots
func memoryDelta(before, after MemStats) MemStats {
	return MemStats{
		Alloc:     after.Alloc - before.Alloc,
		HeapAlloc: after.HeapAlloc - before.HeapAlloc,
		HeapInuse: after.HeapInuse - before.HeapInuse,
	}
}

// TestMemoryLeak_OpenForAttrs tests for leaks in openForAttrs
func TestMemoryLeak_OpenForAttrs(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	const iterations = 1000
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			t.Fatalf("openForAttrs failed: %v", err)
		}
		windows.Close(h)
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	// Allow 100KB threshold for normal allocations
	const maxAllowedBytes = 100 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak detected in openForAttrs: %d bytes leaked", delta.HeapAlloc)
	}
}

// TestMemoryLeak_UTF16PathBufPool tests for leaks in buffer pool
func TestMemoryLeak_UTF16PathBufPool(t *testing.T) {
	const iterations = 10000
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		bufPtr := utf16PathBufPool.Get().(*[]uint16)
		_ = toUTF16Z("C:\\Windows\\System32\\test.txt", *bufPtr)
		utf16PathBufPool.Put(bufPtr)
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 50 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak detected in UTF16 buffer pool: %d bytes leaked",
			delta.HeapAlloc)
	}
}

// TestMemoryLeak_OverlappedHandle tests for leaks in overlapped I/O
func TestMemoryLeak_OverlappedHandle(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.dat")

	// Create a test file with data
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	const iterations = 500
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			t.Fatalf("openForAttrs failed: %v", err)
		}

		oh := newOverlapped(h)
		buf := make([]byte, 1024)
		_, err = oh.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt failed: %v", err)
		}

		if err := oh.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 200 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak in overlappedHandle: %d bytes leaked",
			delta.HeapAlloc)
	}
}

// TestMemoryLeak_EventHandlePool tests for leaks in event handle pooling
func TestMemoryLeak_EventHandlePool(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.dat")

	data := make([]byte, 8192)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	h, err := openForAttrs(testFile)
	if err != nil {
		t.Fatalf("openForAttrs failed: %v", err)
	}
	defer windows.Close(h)

	oh := newOverlapped(h)
	defer oh.Close()

	const iterations = 2000
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		e, err := oh.getEvent()
		if err != nil {
			t.Fatalf("getEvent failed: %v", err)
		}
		oh.putEvent(e)
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 50 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak in event handle pool: %d bytes leaked",
			delta.HeapAlloc)
	}
}

// TestMemoryLeak_GetStatFS tests for leaks in filesystem stats
func TestMemoryLeak_GetStatFS(t *testing.T) {
	const iterations = 1000
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		_, err := getStatFS("C")
		if err != nil {
			t.Fatalf("getStatFS failed: %v", err)
		}
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 100 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak in getStatFS: %d bytes leaked", delta.HeapAlloc)
	}
}

// TestMemoryLeak_QueryAllocatedRanges tests for leaks in sparse file ops
func TestMemoryLeak_QueryAllocatedRanges(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "sparse.dat")

	// Create a sparse file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	f.Write(make([]byte, 4096))
	f.Close()

	h, err := openForAttrs(testFile)
	if err != nil {
		t.Fatalf("openForAttrs failed: %v", err)
	}
	defer windows.Close(h)

	const iterations = 500
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		_, err := queryAllocatedRanges(h, 0, 4096)
		if err != nil && err != windows.ERROR_INVALID_FUNCTION {
			t.Logf("queryAllocatedRanges warning: %v", err)
		}
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 150 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak in queryAllocatedRanges: %d bytes leaked",
			delta.HeapAlloc)
	}
}

// TestMemoryLeak_ConcurrentOperations tests for leaks under concurrent load
func TestMemoryLeak_ConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "concurrent.dat")

	data := make([]byte, 16384)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	const goroutines = 10
	const iterationsPerGoroutine = 100

	before := captureMemStats()

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterationsPerGoroutine; i++ {
				h, err := openForAttrs(testFile)
				if err != nil {
					t.Errorf("openForAttrs failed: %v", err)
					return
				}

				oh := newOverlapped(h)
				buf := make([]byte, 512)
				_, _ = oh.ReadAt(buf, 0)
				oh.Close()
			}
		}()
	}

	wg.Wait()

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 500 * 1024
	if delta.HeapAlloc > maxAllowedBytes {
		t.Errorf("Memory leak under concurrent operations: %d bytes leaked",
			delta.HeapAlloc)
	}
}

// TestHandleLeak_OpenForAttrs verifies handles are properly closed
func TestHandleLeak_OpenForAttrs(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "handle_test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	const iterations = 100
	var handles []windows.Handle

	// Open without closing to detect handle leaks
	for i := 0; i < iterations; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			// If we run out of handles, we have a leak
			if len(handles) > 0 {
				t.Errorf("Handle leak detected: opened %d handles before failure",
					len(handles))
			}
			break
		}
		handles = append(handles, h)
	}

	// Clean up
	for _, h := range handles {
		windows.Close(h)
	}

	// Now test with proper closing
	for i := 0; i < iterations*10; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			t.Fatalf("openForAttrs failed after %d iterations: %v", i, err)
		}
		windows.Close(h)
	}
}

// BenchmarkMemoryAllocation_OpenForAttrs benchmarks memory allocation patterns
func BenchmarkMemoryAllocation_OpenForAttrs(b *testing.B) {
	tempDir := b.TempDir()
	testFile := filepath.Join(tempDir, "bench.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			b.Fatalf("openForAttrs failed: %v", err)
		}
		windows.Close(h)
	}
}

// BenchmarkMemoryAllocation_OverlappedIO benchmarks overlapped I/O allocations
func BenchmarkMemoryAllocation_OverlappedIO(b *testing.B) {
	tempDir := b.TempDir()
	testFile := filepath.Join(tempDir, "bench.dat")

	data := make([]byte, 4096)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h, _ := openForAttrs(testFile)
		oh := newOverlapped(h)
		buf := make([]byte, 1024)
		oh.ReadAt(buf, 0)
		oh.Close()
	}
}
