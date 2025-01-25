//go:build windows

package nfs

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/willscott/go-nfs"
)

type mockHandler struct {
	nfs.Handler
}

func (m *mockHandler) FromHandle([]byte) (billy.Filesystem, []string, error) {
	return memfs.New(), []string{"mock"}, nil
}

type mockFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return m.size }
func (m mockFileInfo) Mode() os.FileMode  { return m.mode }
func (m mockFileInfo) ModTime() time.Time { return m.modTime }
func (m mockFileInfo) IsDir() bool        { return m.isDir }
func (m mockFileInfo) Sys() interface{}   { return m.sys }

func TestHandleCachingPerformance(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()

	paths := make([][]string, 1000)
	for i := range paths {
		paths[i] = []string{uuid.New().String(), uuid.New().String()}
	}

	t.Run("ToHandle Performance", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 10000; i++ {
			handler.ToHandle(fs, paths[i%len(paths)])
		}
		duration := time.Since(start)

		opsPerSec := float64(10000) / duration.Seconds()
		assert.GreaterOrEqual(t, opsPerSec, float64(50000),
			"ToHandle operations should process at least 50K ops/sec, got %.2f ops/sec", opsPerSec)
	})

	handles := make([][]byte, len(paths))
	for i, path := range paths {
		handles[i] = handler.ToHandle(fs, path)
	}

	t.Run("FromHandle Performance", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 10000; i++ {
			_, _, _ = handler.FromHandle(handles[i%len(handles)])
		}
		duration := time.Since(start)

		opsPerSec := float64(10000) / duration.Seconds()
		assert.GreaterOrEqual(t, opsPerSec, float64(100000),
			"FromHandle operations should process at least 100K ops/sec, got %.2f ops/sec", opsPerSec)
	})
}

func TestHighConcurrencyPerformance(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()
	concurrency := 100
	opsPerGoroutine := 1000

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				path := []string{uuid.New().String()}
				handle := handler.ToHandle(fs, path)
				_, _, _ = handler.FromHandle(handle)
				_ = handler.InvalidateHandle(fs, handle)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	totalOps := concurrency * opsPerGoroutine
	opsPerSec := float64(totalOps) / duration.Seconds()

	assert.GreaterOrEqual(t, opsPerSec, float64(20000),
		"Concurrent operations should process at least 20K ops/sec, got %.2f ops/sec", opsPerSec)
}

func TestVerifierPerformance(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)

	createContents := func(size int) []os.FileInfo {
		contents := make([]os.FileInfo, size)
		for i := range contents {
			contents[i] = mockFileInfo{
				name:    fmt.Sprintf("file%d.txt", i),
				size:    int64(i * 1000),
				mode:    0644,
				modTime: time.Now(),
			}
		}
		return contents
	}

	tests := []struct {
		name         string
		size         int
		minOpsPerSec float64
	}{
		{"Small_Directory", 100, 50000},
		{"Medium_Directory", 1000, 10000},
		{"Large_Directory", 10000, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contents := createContents(tt.size)
			path := filepath.Join("/test", uuid.New().String())

			start := time.Now()
			for i := 0; i < 1000; i++ {
				verifier := handler.VerifierFor(path, contents)
				handler.DataForVerifier(verifier)
			}
			duration := time.Since(start)

			opsPerSec := float64(1000) / duration.Seconds()
			assert.GreaterOrEqual(t, opsPerSec, tt.minOpsPerSec,
				"Verifier operations for %d files should process at least %.0f ops/sec, got %.2f ops/sec",
				tt.size, tt.minOpsPerSec, opsPerSec)
		})
	}
}

func TestCacheResizingPerformance(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()

	start := time.Now()
	targetSize := 100000

	for i := 0; i < targetSize; i++ {
		path := []string{uuid.New().String()}
		handler.ToHandle(fs, path)
		if i > 0 && i%10000 == 0 {
			handler.adjustCacheSize()
		}
	}
	duration := time.Since(start)

	opsPerSec := float64(targetSize) / duration.Seconds()
	assert.GreaterOrEqual(t, opsPerSec, float64(10000),
		"Cache growth operations should process at least 10K ops/sec, got %.2f ops/sec", opsPerSec)
	assert.LessOrEqual(t, handler.activeHandles.Len(), maxHandleCacheSize,
		"Cache size should not exceed maximum limit")
}

func TestMassiveFileOperations(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()

	createMassiveFileSet := func(size int) [][]string {
		paths := make([][]string, size)
		for i := range paths {
			depth := rand.Intn(5) + 1
			path := make([]string, depth)
			for j := range path {
				path[j] = fmt.Sprintf("dir%d_%d", j, rand.Intn(1000))
			}
			path = append(path, fmt.Sprintf("file%d.txt", i))
			paths[i] = path
		}
		return paths
	}

	tests := []struct {
		name         string
		fileCount    int
		minOpsPerSec float64
	}{
		{"100K_Files", 100000, 10000},
		{"500K_Files", 500000, 5000},
		{"1M_Files", 1000000, 2500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paths := createMassiveFileSet(tt.fileCount)
			handles := make([][]byte, len(paths))

			start := time.Now()
			for i, path := range paths {
				handles[i] = handler.ToHandle(fs, path)
			}
			duration := time.Since(start)
			opsPerSec := float64(tt.fileCount) / duration.Seconds()
			assert.GreaterOrEqual(t, opsPerSec, tt.minOpsPerSec,
				"Handle creation for %d files should process at least %.0f ops/sec, got %.2f ops/sec",
				tt.fileCount, tt.minOpsPerSec, opsPerSec)

			t.Run("ConcurrentLookups", func(t *testing.T) {
				concurrency := 20
				lookupsPerGoroutine := tt.fileCount / concurrency
				start := time.Now()
				var wg sync.WaitGroup
				wg.Add(concurrency)

				for i := 0; i < concurrency; i++ {
					go func(offset int) {
						defer wg.Done()
						for j := 0; j < lookupsPerGoroutine; j++ {
							idx := (offset + j) % len(handles)
							_, _, _ = handler.FromHandle(handles[idx])
						}
					}(i * lookupsPerGoroutine)
				}

				wg.Wait()
				duration := time.Since(start)
				opsPerSec := float64(tt.fileCount) / duration.Seconds()
				assert.GreaterOrEqual(t, opsPerSec, tt.minOpsPerSec*2,
					"Concurrent handle lookups should be at least 2x faster than creation")
			})

			hitRate := float64(handler.stats.hits) / float64(handler.stats.hits+handler.stats.misses)
			assert.GreaterOrEqual(t, hitRate, 0.85,
				"Cache hit rate should be at least 85%% for repeated operations, got %.2f%%", hitRate*100)
		})
	}
}
