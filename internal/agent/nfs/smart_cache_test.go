//go:build windows

package nfs

import (
	"os"
	"path/filepath"
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
