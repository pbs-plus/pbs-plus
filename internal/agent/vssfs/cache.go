//go:build windows
// +build windows

package vssfs

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Light-weight stat cache entry with atomic reference counting
type statCacheEntry struct {
	info     *VSSFileInfo
	refCount int32 // Reference counter using atomic operations
}

// Directory cache entry that only stores paths and refers to stat entries
type dirCacheEntry struct {
	paths []string // Just store paths, not full file info
}

type FSCache struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	statCache sync.Map // map[string]*statCacheEntry
	dirCache  sync.Map // map[string]*dirCacheEntry
}

// Background filesystem traversal with multiple workers
type BuildOptions struct {
	RootDir    string
	NumWorkers int
}

// NewFSCache creates a new FSCache rooted at rootDir. It starts a background
// DFS scan using numWorkers workers. The scanning is context aware.
func NewFSCache(ctx context.Context, rootDir string, numWorkers int) *FSCache {
	childCtx, cancel := context.WithCancel(ctx)
	fc := &FSCache{
		ctx:       childCtx,
		ctxCancel: cancel,
	}
	go fc.buildBackground(BuildOptions{RootDir: rootDir, NumWorkers: numWorkers})
	return fc
}

// Retrieve a stat entry from cache
func (fc *FSCache) getStatCache(path string) (*VSSFileInfo, bool) {
	if val, ok := fc.statCache.Load(path); ok {
		entry := val.(*statCacheEntry)
		return entry.info, true
	}
	return nil, false
}

// Retrieve directory contents from cache
func (fc *FSCache) getDirCache(dir string) (ReadDirEntries, bool) {
	if val, ok := fc.dirCache.Load(dir); ok {
		dirEntry := val.(*dirCacheEntry)

		// Directory entry is valid, build the file info array
		return fc.buildFileInfoArray(dirEntry.paths), true
	}
	return nil, false
}

// Build cache in background with parallel workers
func (fc *FSCache) buildBackground(options BuildOptions) {
	if options.NumWorkers <= 0 {
		options.NumWorkers = 4 // Default to 4 workers
	}

	// Create work queues
	workQueue := make(chan string, options.NumWorkers*100)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < options.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for dirPath := range workQueue {
				select {
				case <-fc.ctx.Done():
					return
				default:
					// Process directory
					entries, err := fc.readDir(dirPath)
					if err != nil {
						continue
					}

					for _, entryPath := range entries {
						// Check if this is a directory
						if entryPath.IsDir {
							workQueue <- filepath.Join(dirPath, entryPath.Name)
						}
					}
				}
			}
		}()
	}

	// Start with the root directory
	workQueue <- options.RootDir

	// Close queue when all directories have been processed
	go func() {
		wg.Wait()
		close(workQueue)
	}()
}

// Add a new stat entry to the cache
func (fc *FSCache) addStatEntry(path string, info *VSSFileInfo) {
	if val, ok := fc.statCache.Load(path); ok {
		entry := val.(*statCacheEntry)
		atomic.AddInt32(&entry.refCount, 1)
		return
	}
	entry := &statCacheEntry{
		info:     info,
		refCount: 0,
	}

	fc.statCache.Store(path, entry)
}

// Build a file info array from paths by looking up stat cache
func (fc *FSCache) buildFileInfoArray(paths []string) ReadDirEntries {
	result := make([]*VSSFileInfo, 0, len(paths))

	for _, path := range paths {
		if info, err := fc.stat(path); err == nil {
			result = append(result, info)
		}
	}

	return result
}

// Invalidate a specific path in the cache with reference counting
func (fc *FSCache) invalidatePath(path string) {
	if val, ok := fc.statCache.Load(path); ok {
		entry := val.(*statCacheEntry)

		// Only remove immediately if no references
		if atomic.LoadInt32(&entry.refCount) == 0 {
			fc.statCache.Delete(path)
		}
	}

	// If it's a directory entry, invalidate it
	if val, ok := fc.dirCache.Load(path); ok {
		dirEntry := val.(*dirCacheEntry)
		fc.dirCache.Delete(path)
		for _, entry := range dirEntry.paths {
			if val2, ok2 := fc.statCache.Load(entry); ok2 {
				entry := val2.(*statCacheEntry)
				atomic.AddInt32(&entry.refCount, -1)
				if atomic.LoadInt32(&entry.refCount) == 0 {
					fc.statCache.Delete(path)
				}
			}
		}
	}
}

// Clear the entire cache
func (fc *FSCache) clearCache() {
	fc.dirCache.Range(func(key, value interface{}) bool {
		fc.dirCache.Delete(key)
		return true
	})

	fc.statCache.Range(func(key, value interface{}) bool {
		fc.statCache.Delete(key)
		return true
	})
}
