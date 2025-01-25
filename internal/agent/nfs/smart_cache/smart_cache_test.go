//go:build windows

package smart_cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
)

type mockHandler struct {
	nfs.Handler
}

func (m *mockHandler) FromHandle([]byte) (billy.Filesystem, []string, error) {
	return memfs.New(), []string{"mock"}, nil
}

func TestNewSmartCachingHandler(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{})
	assert.NotNil(t, handler)
}

func TestHandleCaching(t *testing.T) {
	t.Run("Basic handle caching", func(t *testing.T) {
		handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
		fs := memfs.New()
		path := []string{"test", "path"}

		// First access should create new handle
		handle1 := handler.ToHandle(fs, path)
		require.NotEmpty(t, handle1)

		// Second access should return same handle
		handle2 := handler.ToHandle(fs, path)
		assert.Equal(t, handle1, handle2)

		// Verify handle resolves correctly
		resultFS, resultPath, err := handler.FromHandle(handle1)
		require.NoError(t, err)
		assert.Equal(t, fs, resultFS)
		assert.Equal(t, path, resultPath)
	})

	t.Run("Handle invalidation", func(t *testing.T) {
		handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
		fs := memfs.New()
		path := []string{"test"}

		handle := handler.ToHandle(fs, path)
		require.NotEmpty(t, handle)

		err := handler.InvalidateHandle(fs, handle)
		require.NoError(t, err)

		// Handle should no longer be valid
		_, _, err = handler.FromHandle(handle)
		assert.Error(t, err)
	})

	t.Run("Cache size adjustment", func(t *testing.T) {
		handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
		fs := memfs.New()

		// Generate many handles to trigger cache adjustment
		for i := 0; i < 100000; i++ {
			handler.ToHandle(fs, []string{uuid.New().String()})
		}

		// Verify cache size remains within limits
		assert.LessOrEqual(t, handler.activeHandles.Len(), maxHandleCacheSize)
	})
}

func TestVerifierHandling(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)

	t.Run("Verifier creation and retrieval", func(t *testing.T) {
		path := "/test/path"
		contents := []os.FileInfo{
			mockFileInfo{name: "file1.txt", mode: 0644},
			mockFileInfo{name: "file2.txt", mode: 0644},
		}

		// Create verifier
		verifierID := handler.VerifierFor(path, contents)
		require.NotZero(t, verifierID)

		// Retrieve contents
		retrievedContents := handler.DataForVerifier(verifierID)
		require.NotNil(t, retrievedContents)
		assert.Equal(t, len(contents), len(retrievedContents))

		// Verify contents match
		for i, c := range contents {
			assert.Equal(t, c.Name(), retrievedContents[i].Name())
			assert.Equal(t, c.Mode(), retrievedContents[i].Mode())
		}
	})

	t.Run("Verifier cache limits", func(t *testing.T) {
		// Create more verifiers than cache size
		for i := 0; i < verifierCacheSize+100; i++ {
			path := filepath.Join("/test", uuid.New().String())
			contents := []os.FileInfo{mockFileInfo{name: "test.txt", mode: 0644}}
			handler.VerifierFor(path, contents)
		}

		// Verify cache size is limited
		assert.LessOrEqual(t, handler.activeVerifiers.Len(), verifierCacheSize)
	})
}

// Mock FileInfo implementation for testing
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

func TestConcurrentAccess(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()

	t.Run("Concurrent handle operations", func(t *testing.T) {
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 1000; j++ {
					path := []string{uuid.New().String()}
					handle := handler.ToHandle(fs, path)
					_, _, _ = handler.FromHandle(handle)
					_ = handler.InvalidateHandle(fs, handle)
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

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

func BenchmarkVerifierOperations(b *testing.B) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)

	contents := make([][]os.FileInfo, 1000)
	for i := range contents {
		contents[i] = []os.FileInfo{
			mockFileInfo{
				name:    fmt.Sprintf("file%d.txt", i),
				size:    int64(i * 1000),
				mode:    0644,
				modTime: time.Now(),
			},
		}
	}

	paths := make([]string, len(contents))
	for i := range paths {
		paths[i] = filepath.Join("/test", uuid.New().String())
	}

	b.Run("VerifierCreation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			handler.VerifierFor(paths[i%len(paths)], contents[i%len(contents)])
		}
	})

	verifiers := make([]uint64, len(contents))
	for i := range verifiers {
		verifiers[i] = handler.VerifierFor(paths[i], contents[i])
	}

	b.Run("VerifierRetrieval", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			handler.DataForVerifier(verifiers[i%len(verifiers)])
		}
	})
}

func BenchmarkCacheResizing(b *testing.B) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()

	b.Run("Cache growth under load", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			paths := make([][]string, 10000)
			for j := range paths {
				paths[j] = []string{uuid.New().String()}
				handler.ToHandle(fs, paths[j])
			}
			handler.adjustCacheSize()
		}
	})
}

func BenchmarkLargeDirectoryOperations(b *testing.B) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)

	createLargeDirectory := func(size int) ([]os.FileInfo, string) {
		contents := make([]os.FileInfo, size)
		for i := range contents {
			contents[i] = mockFileInfo{
				name:    fmt.Sprintf("file%d.txt", i),
				size:    int64(i * 1000),
				mode:    0644,
				modTime: time.Now(),
			}
		}
		return contents, filepath.Join("/test", uuid.New().String())
	}

	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		contents, path := createLargeDirectory(size)
		b.Run(fmt.Sprintf("Directory_Size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				verifier := handler.VerifierFor(path, contents)
				handler.DataForVerifier(verifier)
			}
		})
	}
}

func TestEdgeCases(t *testing.T) {
	handler := NewSmartCachingHandler(&mockHandler{}).(*CachingHandler)
	fs := memfs.New()

	t.Run("Empty path", func(t *testing.T) {
		handle := handler.ToHandle(fs, []string{})
		require.NotEmpty(t, handle)

		resultFS, resultPath, err := handler.FromHandle(handle)
		require.NoError(t, err)
		assert.Equal(t, fs, resultFS)
		assert.Empty(t, resultPath)
	})

	t.Run("Invalid handle", func(t *testing.T) {
		_, _, err := handler.FromHandle([]byte("invalid"))
		assert.Error(t, err)
	})

	t.Run("Duplicate invalidation", func(t *testing.T) {
		path := []string{"test"}
		handle := handler.ToHandle(fs, path)

		// First invalidation
		err := handler.InvalidateHandle(fs, handle)
		require.NoError(t, err)

		// Second invalidation should not error
		err = handler.InvalidateHandle(fs, handle)
		assert.NoError(t, err)
	})
}
