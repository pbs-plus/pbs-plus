//go:build windows
// +build windows

package vssfs

import (
	"context"
	"path/filepath"
	"sync"
)

// CachedEntry holds the cached metadata for a file or directory.
// For directories, DirEntries caches the result of readdir (list of children).
type CachedEntry struct {
	Stat       *VSSFileInfo   // file or directory metadata
	DirEntries []*VSSFileInfo // non-nil only if Stat represents a directory
}

// FileCache builds and holds a one‑time cache of file system metadata.
// It scans the file system starting at rootDir using a pool of numWorkers workers.
// The cache uses the already–existing “way” of doing stat and readdir
// (see cachedStat and cachedReadDir below) so that we reduce syscalls, caching both
// stat and readdir results. When a client accesses an entry it is popped immediately.
// Since the underlying file system is read‑only, this one‑time cache is safe.
type FileCache struct {
	mu      sync.RWMutex
	entries map[string]*CachedEntry
	rootDir string

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileCache creates a new FileCache rooted at rootDir. It starts a background
// DFS scan using numWorkers workers. The cache is context aware; when ctx is canceled
// the scanning will stop.
func NewFileCache(ctx context.Context, rootDir string, numWorkers int) *FileCache {
	childCtx, cancel := context.WithCancel(ctx)
	fc := &FileCache{
		rootDir: rootDir,
		entries: make(map[string]*CachedEntry),
		ctx:     childCtx,
		cancel:  cancel,
	}
	go fc.buildBackground(numWorkers)
	return fc
}

// buildBackground performs a DFS-like traversal of the file system.
// For each directory, it calls our helper functions to perform stat and readdir
// using the same Windows syscalls as in our server. For every file and
// directory it caches a CachedEntry, keyed by the full path.
// For directories it also caches the list of children (so the readdir syscall is saved).
// The work is distributed over numWorkers; the routine respects fc.ctx for cancellation.
func (fc *FileCache) buildBackground(numWorkers int) {
	jobs := make(chan string, 1000)
	var dirWg sync.WaitGroup

	worker := func() {
		for {
			select {
			case <-fc.ctx.Done():
				return
			case dir, ok := <-jobs:
				if !ok {
					return
				}
				// Process this directory.
				entry, err := fc.cacheEntryForDir(dir)
				if err == nil {
					fc.mu.Lock()
					fc.entries[dir] = entry
					fc.mu.Unlock()

					// For each child entry reported in the readdir result:
					for _, child := range entry.DirEntries {
						childPath := filepath.Join(dir, child.Name)
						// Cache the child's stat info if not already present.
						fc.mu.Lock()
						if _, exists := fc.entries[childPath]; !exists {
							fc.entries[childPath] = &CachedEntry{Stat: child}
						}
						fc.mu.Unlock()
						// If the child is a directory, schedule it for processing.
						if child.IsDir {
							dirWg.Add(1)
							select {
							case jobs <- childPath:
							case <-fc.ctx.Done():
								// Cancellation requested.
							}
						}
					}
				}
				dirWg.Done()
			}
		}
	}

	// Start worker pool.
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	// Seed the process with the root directory.
	dirWg.Add(1)
	select {
	case jobs <- fc.rootDir:
	case <-fc.ctx.Done():
	}

	// Close the jobs channel once all directories have been processed.
	go func() {
		dirWg.Wait()
		close(jobs)
	}()
}

// cacheEntryForDir uses the common helpers to perform stat and readdir for dir.
func (fc *FileCache) cacheEntryForDir(dir string) (*CachedEntry, error) {
	stat, err := stat(dir)
	if err != nil {
		return nil, err
	}
	children, err := readDir(dir)
	// Even if readdir fails, we still return stat.
	if err != nil {
		children = nil
	}
	return &CachedEntry{Stat: stat, DirEntries: children}, nil
}

// Pop returns (and immediately removes) the cached result for path.
// The returned CachedEntry contains both stat and, if applicable, the readdir cache.
func (fc *FileCache) Pop(path string) (*CachedEntry, bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	entry, ok := fc.entries[path]
	if ok {
		delete(fc.entries, path)
	}
	return entry, ok
}

// Cancel stops the background scanning.
func (fc *FileCache) Cancel() {
	fc.cancel()
}
