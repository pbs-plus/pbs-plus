//go:build windows
// +build windows

package vssfs

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

// FSCache provides a memory-efficient filesystem cache optimized for
// depth-first traversal with single-access patterns
type FSCache struct {
	ctx        context.Context
	ctxCancel  context.CancelFunc
	statCache  *haxmap.Map[string, *statCacheEntry]
	dirCache   *haxmap.Map[string, *dirCacheEntry]
	maxEntries int   // Maximum number of entries to keep in cache
	closed     int32 // Whether the cache has been closed
	cleanupWg  sync.WaitGroup

	// For entry limiting and pause/continue mechanism
	entryCount int32         // Current number of entries (atomic)
	entrySem   chan struct{} // Semaphore for controlling entry additions
	entrySemMu sync.Mutex    // Mutex for entrySem operations
}

// statCacheEntry with single-access optimization
type statCacheEntry struct {
	info     *VSSFileInfo
	accessed int32 // 0=never, 1=accessed (atomic)
}

// dirCacheEntry optimized for depth-first traversal
type dirCacheEntry struct {
	paths    []string
	accessed int32      // 0=never, 1=accessed (atomic)
	mutex    sync.Mutex // Protects the entry during lazy loading
	isLoaded bool
}

// BuildOptions controls the depth-first traversal
type BuildOptions struct {
	RootDir    string
	NumWorkers int
}

// NewFSCache creates a cache optimized for depth-first traversal
// where each stat is only expected to be accessed once
func NewFSCache(ctx context.Context, rootDir string, maxEntries int) *FSCache {
	childCtx, cancel := context.WithCancel(ctx)

	// Use reasonable default if not specified
	if maxEntries <= 0 {
		maxEntries = 100000
	}

	fc := &FSCache{
		ctx:        childCtx,
		ctxCancel:  cancel,
		maxEntries: maxEntries,
		entrySem:   make(chan struct{}, maxEntries), // Buffer to maxEntries
		statCache:  hashmap.New[*statCacheEntry](),
		dirCache:   hashmap.New[*dirCacheEntry](),
	}

	// Start the background DFS if rootDir is provided
	if rootDir != "" {
		numWorkers := runtime.NumCPU()
		if numWorkers > 1 {
			numWorkers = numWorkers / 2
		}
		fc.cleanupWg.Add(1)
		go func() {
			defer fc.cleanupWg.Done()
			fc.buildDepthFirst(BuildOptions{
				RootDir:    rootDir,
				NumWorkers: numWorkers,
			})
		}()
	}

	return fc
}

// Close shuts down the cache and releases resources
func (fc *FSCache) Close() {
	if !atomic.CompareAndSwapInt32(&fc.closed, 0, 1) {
		return // Already closed
	}

	fc.ctxCancel()
	fc.cleanupWg.Wait()
	fc.clearCache()
	close(fc.entrySem) // Close semaphore channel
}

// acquireEntry attempts to acquire a slot for a new cache entry
// It pauses if at capacity and continues when space is available
func (fc *FSCache) acquireEntry() bool {
	if atomic.LoadInt32(&fc.closed) == 1 {
		return false
	}

	// Fast path - if we're under limit, just increment and proceed
	current := atomic.LoadInt32(&fc.entryCount)
	if current < int32(fc.maxEntries) {
		if atomic.CompareAndSwapInt32(&fc.entryCount, current, current+1) {
			return true
		}
	}

	// Slow path - we're at or near capacity, need to use semaphore
	select {
	case fc.entrySem <- struct{}{}:
		atomic.AddInt32(&fc.entryCount, 1)
		return true
	case <-fc.ctx.Done():
		return false
	}
}

// releaseEntry releases a slot after a cache entry is removed
func (fc *FSCache) releaseEntry() {
	atomic.AddInt32(&fc.entryCount, -1)

	// Try to remove an item from semaphore to free a slot
	select {
	case <-fc.entrySem:
		// Successfully removed one item
	default:
		// Semaphore was empty, which is fine
	}
}

// Stat retrieves file info and prunes it immediately after access
func (fc *FSCache) Stat(path string) (*VSSFileInfo, error) {
	if atomic.LoadInt32(&fc.closed) == 1 {
		return nil, ErrCacheClosed
	}

	normalizedPath := filepath.Clean(path)

	// Try to get from cache
	val, ok := fc.statCache.Get(normalizedPath)
	if ok {
		// Mark as accessed - if it was already accessed, consider pruning
		if atomic.CompareAndSwapInt32(&val.accessed, 0, 1) {
			// First access - defer pruning until we return the info
			defer fc.pruneStatEntry(normalizedPath)
		}

		return val.info, nil
	}

	// Not in cache, do the real stat
	info, err := fc.stat(normalizedPath)
	if err != nil {
		return nil, err
	}

	// Cache it for potential future use, but mark as accessed immediately
	entry := &statCacheEntry{
		info:     info,
		accessed: 1, // Mark as accessed immediately
	}

	// Try to acquire an entry slot - will pause if at capacity
	if fc.acquireEntry() {
		fc.statCache.Set(normalizedPath, entry)
		// Schedule pruning after a brief delay to allow for any potential
		// quick follow-up access (though in this case we don't expect any)
		go fc.pruneStatEntry(normalizedPath)
	}

	return info, nil
}

// ReadDir reads a directory, optimized for depth-first traversal
func (fc *FSCache) ReadDir(dirPath string) (ReadDirEntries, error) {
	if atomic.LoadInt32(&fc.closed) == 1 {
		return nil, ErrCacheClosed
	}

	normalizedPath := filepath.Clean(dirPath)

	// Try to get from cache first
	val, ok := fc.dirCache.Get(normalizedPath)
	if !ok {
		// Not in cache, create new entry
		// Only proceed if we can acquire an entry slot
		if !fc.acquireEntry() {
			// We can't cache, but we can still return data
			entries, err := fc.readDir(normalizedPath)
			if err != nil {
				return nil, err
			}
			return entries, nil
		}

		entry := &dirCacheEntry{
			isLoaded: false,
		}

		actual, loaded := fc.dirCache.GetOrSet(normalizedPath, entry)
		if loaded {
			// Someone else stored it while we were waiting
			// Release our token since we don't need it
			fc.releaseEntry()
			entry = actual
		}

		// If not loaded yet, we need to load it
		if !entry.isLoaded {
			entry.mutex.Lock()
			// Check again after acquiring lock
			if !entry.isLoaded {
				// Do the real directory read
				entries, err := fc.readDir(normalizedPath)
				if err != nil {
					entry.mutex.Unlock()
					fc.dirCache.Get(normalizedPath)
					fc.releaseEntry()
					return nil, err
				}

				// Store paths for efficient memory usage
				paths := make([]string, 0, len(entries))
				for _, info := range entries {
					entryPath := filepath.Join(normalizedPath, info.Name)
					paths = append(paths, entryPath)

					// Pre-populate stat cache
					fc.preloadStatEntry(entryPath, info)
				}

				entry.paths = paths
				entry.isLoaded = true
			}
			entry.mutex.Unlock()
		}

		val = actual
	}

	// Mark as accessed - if it was already accessed, consider pruning
	if atomic.CompareAndSwapInt32(&val.accessed, 0, 1) {
		// First access - defer pruning until we return the results
		defer fc.pruneDirEntry(normalizedPath)
	}

	// Build and return the results
	return fc.buildFileInfoArray(val.paths)
}

// Preload a stat entry for later access (used during directory reads)
func (fc *FSCache) preloadStatEntry(path string, info *VSSFileInfo) {
	// Only add if we don't have it already
	if _, ok := fc.statCache.Get(path); !ok {
		// Try to acquire an entry slot - will pause if at capacity
		if fc.acquireEntry() {
			entry := &statCacheEntry{
				info:     info,
				accessed: 0,
			}
			fc.statCache.Set(path, entry)
		}
	}
}

// Prune a stat entry immediately after it's been accessed
func (fc *FSCache) pruneStatEntry(path string) {
	if _, ok := fc.statCache.GetAndDel(path); ok {
		fc.releaseEntry()
	}
}

// Prune a directory entry after it's been accessed
func (fc *FSCache) pruneDirEntry(path string) {
	val, ok := fc.dirCache.Get(path)
	if !ok {
		return
	}

	for _, childPath := range val.paths {
		if entry, ok := fc.statCache.Get(childPath); ok {
			if atomic.LoadInt32(&entry.accessed) == 1 {
				fc.pruneStatEntry(childPath)
			}
		}
	}

	if _, ok := fc.dirCache.GetAndDel(path); ok {
		fc.releaseEntry()
	}
}

// buildDepthFirst performs true depth-first traversal using a worker pool
func (fc *FSCache) buildDepthFirst(options BuildOptions) {
	if options.NumWorkers <= 0 {
		options.NumWorkers = runtime.NumCPU()
	}

	// Use a stack (LIFO) instead of a queue for depth-first semantics
	// We'll implement this with a channel but process the deepest paths first
	dirStack := make(chan string, options.NumWorkers*100)
	seenDirs := sync.Map{}
	var stackMutex sync.Mutex
	var pathsInStack int32

	// Helper to push to our stack-like structure
	pushToStack := func(path string) {
		if _, alreadySeen := seenDirs.LoadOrStore(path, true); !alreadySeen {
			stackMutex.Lock()
			select {
			case dirStack <- path:
				atomic.AddInt32(&pathsInStack, 1)
			default:
				// Stack is full, will be picked up in next sweep
			}
			stackMutex.Unlock()
		}
	}

	// Helper to pop from our stack-like structure
	popFromStack := func() (string, bool) {
		stackMutex.Lock()
		defer stackMutex.Unlock()

		select {
		case path := <-dirStack:
			atomic.AddInt32(&pathsInStack, -1)
			return path, true
		default:
			return "", false
		}
	}

	// Start with the root directory
	pushToStack(options.RootDir)

	// Use a wait group to track active workers
	var wg sync.WaitGroup
	for i := 0; i < options.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			pathsToProcess := make([]string, 0, 100) // Local stack per worker

			for {
				// Check if we should exit
				select {
				case <-fc.ctx.Done():
					return
				default:
				}

				// First try to use our local stack (deeper paths)
				var currentPath string
				if len(pathsToProcess) > 0 {
					// Pop from the end for true DFS
					currentPath = pathsToProcess[len(pathsToProcess)-1]
					pathsToProcess = pathsToProcess[:len(pathsToProcess)-1]
				} else {
					// Get a path from the shared stack
					var ok bool
					currentPath, ok = popFromStack()
					if !ok {
						// Nothing to process, check if we're done
						if atomic.LoadInt32(&pathsInStack) == 0 {
							// Double check with a small delay
							select {
							case <-fc.ctx.Done():
								return
							case <-time.After(10 * time.Millisecond):
								// Recheck to handle race conditions
								if atomic.LoadInt32(&pathsInStack) == 0 {
									p, ok := popFromStack()
									if !ok {
										return // No more work
									}
									currentPath = p
								}
							}
						}
						// Brief yield to prevent CPU spinning
						runtime.Gosched()
						continue
					}
				}

				// Process this directory
				entries, err := fc.ReadDir(currentPath)
				if err != nil {
					continue
				}

				// Add child directories to our local stack for true DFS
				for i := len(entries) - 1; i >= 0; i-- {
					entry := entries[i]
					if entry.IsDir {
						childPath := filepath.Join(currentPath, entry.Name)
						// Add to local stack for immediate depth-first processing
						if _, seen := seenDirs.LoadOrStore(childPath, true); !seen {
							pathsToProcess = append(pathsToProcess, childPath)
						}
					}
				}

				// If our local stack gets too big, move some to the shared stack
				if len(pathsToProcess) > 100 {
					// Move half to the shared stack
					midpoint := len(pathsToProcess) / 2
					for _, p := range pathsToProcess[:midpoint] {
						pushToStack(p)
					}
					pathsToProcess = pathsToProcess[midpoint:]
				}
			}
		}()
	}

	// Wait for all workers to finish
	wg.Wait()
	close(dirStack)
}

// buildFileInfoArray builds file info array from paths for directory listing
func (fc *FSCache) buildFileInfoArray(paths []string) (ReadDirEntries, error) {
	result := make([]*VSSFileInfo, 0, len(paths))

	for _, path := range paths {
		// Check in cache first
		entry, ok := fc.statCache.Get(path)
		if ok {
			// Mark as accessed if not already
			atomic.CompareAndSwapInt32(&entry.accessed, 0, 1)

			// Make a copy with just the base name for the result
			info := *entry.info // Copy the struct
			info.Name = filepath.Base(path)
			result = append(result, &info)

			// Schedule pruning
			go fc.pruneStatEntry(path)
		} else {
			// Not in cache, do a real stat
			info, err := fc.stat(path)
			if err != nil {
				// Skip this entry but continue with others
				continue
			}
			// Set just the basename
			info.Name = filepath.Base(path)
			result = append(result, info)
		}
	}

	return result, nil
}

// Clear the entire cache
func (fc *FSCache) clearCache() {
	fc.dirCache.ForEach(func(s string, dce *dirCacheEntry) bool {
		fc.dirCache.Del(s)
		fc.releaseEntry()
		return true
	})

	fc.statCache.ForEach(func(s string, sce *statCacheEntry) bool {
		fc.statCache.Del(s)
		fc.releaseEntry()
		return true
	})
}

// Errors
var (
	ErrCacheClosed = errors.New("cache has been closed")
)
