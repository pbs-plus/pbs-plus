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
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

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
// Returns 0 if there's an underflow (before > after due to GC)
func memoryDelta(before, after MemStats) int64 {
	// Use signed arithmetic to detect underflow
	allocDelta := int64(after.Alloc) - int64(before.Alloc)
	heapAllocDelta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	heapInuseDelta := int64(after.HeapInuse) - int64(before.HeapInuse)

	// If all deltas are negative, GC cleaned up more than was allocated
	if allocDelta < 0 && heapAllocDelta < 0 && heapInuseDelta < 0 {
		return 0
	}

	// Return the maximum positive delta
	max := allocDelta
	if heapAllocDelta > max {
		max = heapAllocDelta
	}
	if heapInuseDelta > max {
		max = heapInuseDelta
	}

	if max < 0 {
		return 0
	}
	return max
}

// TestMemoryLeak_OpenForAttrs tests for leaks in openForAttrs
func TestMemoryLeak_OpenForAttrs(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Warmup to stabilize allocations
	for i := 0; i < 100; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			t.Fatalf("openForAttrs warmup failed: %v", err)
		}
		windows.Close(h)
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

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
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak detected in openForAttrs: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
		t.Logf("Before: Alloc=%d, HeapAlloc=%d, HeapInuse=%d",
			before.Alloc, before.HeapAlloc, before.HeapInuse)
		t.Logf("After:  Alloc=%d, HeapAlloc=%d, HeapInuse=%d",
			after.Alloc, after.HeapAlloc, after.HeapInuse)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
	}
}

// TestMemoryLeak_UTF16PathBufPool tests for leaks in buffer pool
func TestMemoryLeak_UTF16PathBufPool(t *testing.T) {
	// Warmup
	for i := 0; i < 100; i++ {
		bufPtr := utf16PathBufPool.Get().(*[]uint16)
		_ = toUTF16Z("C:\\Windows\\System32\\test.txt", *bufPtr)
		utf16PathBufPool.Put(bufPtr)
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

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
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak detected in UTF16 buffer pool: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
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

	// Warmup
	for i := 0; i < 50; i++ {
		pathUTF16, err := windows.UTF16PtrFromString(testFile)
		if err != nil {
			t.Fatalf("UTF16PtrFromString failed: %v", err)
		}

		h, err := windows.CreateFile(
			pathUTF16,
			windows.GENERIC_READ,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
			nil,
			windows.OPEN_EXISTING,
			windows.FILE_FLAG_OVERLAPPED,
			0,
		)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}

		oh := newOverlapped(h)
		buf := make([]byte, 1024)
		_, _ = oh.ReadAt(buf, 0)
		oh.Close()
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	const iterations = 500
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		pathUTF16, err := windows.UTF16PtrFromString(testFile)
		if err != nil {
			t.Fatalf("UTF16PtrFromString failed: %v", err)
		}

		h, err := windows.CreateFile(
			pathUTF16,
			windows.GENERIC_READ,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
			nil,
			windows.OPEN_EXISTING,
			windows.FILE_FLAG_OVERLAPPED,
			0,
		)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
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
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak in overlappedHandle: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
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

	pathUTF16, err := windows.UTF16PtrFromString(testFile)
	if err != nil {
		t.Fatalf("UTF16PtrFromString failed: %v", err)
	}

	h, err := windows.CreateFile(
		pathUTF16,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OVERLAPPED,
		0,
	)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	defer windows.Close(h)

	oh := newOverlapped(h)
	defer oh.Close()

	// Warmup
	for i := 0; i < 100; i++ {
		e, err := oh.getEvent()
		if err != nil {
			t.Fatalf("getEvent failed: %v", err)
		}
		oh.putEvent(e)
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

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
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak in event handle pool: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
	}
}

// TestMemoryLeak_GetStatFS tests for leaks in filesystem stats
func TestMemoryLeak_GetStatFS(t *testing.T) {
	// Warmup
	for i := 0; i < 100; i++ {
		_, err := getStatFS("C")
		if err != nil {
			t.Fatalf("getStatFS warmup failed: %v", err)
		}
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

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
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak in getStatFS: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
	}
}

// TestMemoryLeak_QueryAllocatedRanges tests for leaks in sparse file ops
func TestMemoryLeak_QueryAllocatedRanges(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "sparse.dat")

	// Create a file with proper permissions
	data := make([]byte, 4096)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	pathUTF16, err := windows.UTF16PtrFromString(testFile)
	if err != nil {
		t.Fatalf("UTF16PtrFromString failed: %v", err)
	}

	// Open with proper access rights for FSCTL_QUERY_ALLOCATED_RANGES
	h, err := windows.CreateFile(
		pathUTF16,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	defer windows.Close(h)

	// Test if queryAllocatedRanges is supported
	_, err = queryAllocatedRanges(h, 0, 4096)
	if err == windows.ERROR_INVALID_FUNCTION {
		t.Skip("FSCTL_QUERY_ALLOCATED_RANGES not supported on this filesystem")
	}
	if err == windows.ERROR_ACCESS_DENIED {
		t.Skip("Insufficient permissions for FSCTL_QUERY_ALLOCATED_RANGES")
	}

	// Warmup
	for i := 0; i < 50; i++ {
		_, _ = queryAllocatedRanges(h, 0, 4096)
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	const iterations = 500
	before := captureMemStats()

	for i := 0; i < iterations; i++ {
		_, _ = queryAllocatedRanges(h, 0, 4096)
	}

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 150 * 1024
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak in queryAllocatedRanges: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
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
	const iterationsPerGoroutine = 50

	// Warmup
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				h, err := openForAttrs(testFile)
				if err == nil {
					windows.Close(h)
				}
			}
		}()
	}
	wg.Wait()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	before := captureMemStats()

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
				windows.Close(h)
			}
		}()
	}

	wg.Wait()

	after := captureMemStats()
	delta := memoryDelta(before, after)

	const maxAllowedBytes = 500 * 1024
	if delta > maxAllowedBytes {
		t.Errorf("Memory leak under concurrent operations: %d bytes leaked (%.2f KB)",
			delta, float64(delta)/1024)
	} else {
		t.Logf("Memory change: %d bytes (%.2f KB) - PASS", delta, float64(delta)/1024)
	}
}

// TestHandleLeak_OpenForAttrs verifies handles are properly closed
func TestHandleLeak_OpenForAttrs(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "handle_test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test with proper closing - should handle many iterations
	const iterations = 1000
	for i := 0; i < iterations; i++ {
		h, err := openForAttrs(testFile)
		if err != nil {
			t.Fatalf("openForAttrs failed after %d iterations: %v", i, err)
		}
		windows.Close(h)
	}

	t.Logf("Successfully opened and closed handle %d times", iterations)
}
