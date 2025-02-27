//go:build windows
// +build windows

package vssfs

import (
	"context"
	"path/filepath"
	"sync"
)

// CachedEntry holds the cached metadata for both a file/directory (its stat)
// and, if the entry is a directory, the result from readdir.
type CachedEntry struct {
	Stat       *VSSFileInfo   // file or directory metadata
	DirEntries []*VSSFileInfo // cached directory listing; nil if not applicable or pop'd.
}

// FileCache implements a one‑time background cache of file system metadata.
// It builds a DFS over the file system (using a configurable number of workers)
// and, for each directory, caches both its stat and readdir result in one entry.
// Later, two separate “pop” methods allow a caller to retrieve and invalidate the
// stat or the readdir result independently.
type FileCache struct {
	mu      sync.RWMutex
	entries map[string]*CachedEntry
	rootDir string

	ctx    context.Context
	cancel context.CancelFunc
}

// NewFileCache creates a new FileCache rooted at rootDir. It starts a background
// DFS scan using numWorkers workers. The scanning is context aware.
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
// For each directory it calls cacheEntryForDir() to obtain both its stat and
// cached readdir result. For each child it schedules further scanning if it is a
// directory. The work is distributed over numWorkers workers and respects fc.ctx.
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

					// For each child entry found via readdir:
					for _, child := range entry.DirEntries {
						childPath := filepath.Join(dir, child.Name)
						// Cache the child's stat info if not already present.
						fc.mu.Lock()
						if e, exists := fc.entries[childPath]; !exists {
							fc.entries[childPath] = &CachedEntry{Stat: child}
						} else if e.Stat == nil {
							e.Stat = child
						}
						fc.mu.Unlock()
						// If the child is a directory, schedule it for scanning.
						if child.IsDir {
							dirWg.Add(1)
							select {
							case jobs <- childPath:
							case <-fc.ctx.Done():
							}
						}
					}
				}
				dirWg.Done()
			}
		}
	}

	// Spawn a worker pool.
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	// Seed with the root directory.
	dirWg.Add(1)
	select {
	case jobs <- fc.rootDir:
	case <-fc.ctx.Done():
	}

	// Close the jobs channel when processing is finished.
	go func() {
		dirWg.Wait()
		close(jobs)
	}()
}

// cacheEntryForDir uses the helper functions cachedStat and cachedReadDir
// to build a CachedEntry for directory dir.
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

// PopStat pops (retrieves and invalidates) only the stat part of the cached
// entry for path. (If DirEntries remain, they are preserved in the cache.)
func (fc *FileCache) PopStat(path string) (*VSSFileInfo, bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	entry, ok := fc.entries[path]
	if !ok {
		return nil, false
	}
	if entry.Stat == nil {
		return nil, false
	}
	stat := entry.Stat
	entry.Stat = nil
	// Optionally, remove the entire entry if both parts are now nil.
	if entry.DirEntries == nil || len(entry.DirEntries) == 0 {
		delete(fc.entries, path)
	}
	return stat, true
}

// PopReaddir pops (retrieves and invalidates) only the directory listing
// (readdir result) for path. (The stat part, if still available, is preserved.)
func (fc *FileCache) PopReaddir(path string) ([]*VSSFileInfo, bool) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	entry, ok := fc.entries[path]
	if !ok {
		return nil, false
	}
	if entry.DirEntries == nil {
		return nil, false
	}
	entries := entry.DirEntries
	entry.DirEntries = nil
	// Optionally remove the entry entirely if stat is already nil.
	if entry.Stat == nil {
		delete(fc.entries, path)
	}
	return entries, true
}

// Cancel stops the background scanning.
func (fc *FileCache) Cancel() {
	fc.cancel()
}
