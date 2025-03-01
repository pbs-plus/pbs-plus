package vssfs

import (
	"path/filepath"
	"strings"
	"sync"
)

// DFSCache manages a stack of cached directory listings.  In a DFS
// traversal, the active branch is exactly the list of directories that
// have been entered but not yet exited.
type DFSCache struct {
	mu    sync.Mutex
	stack []dirCacheEntry
}

// Each dirCacheEntry holds the path and the cached directory listing.
type dirCacheEntry struct {
	dirPath string
	entries ReadDirEntries
}

// NewDFSCache returns a new instance of DFSCache.
func NewDFSCache() *DFSCache {
	return &DFSCache{}
}

func (cache *DFSCache) PushDir(entry dirCacheEntry) error {
	cache.mu.Lock()
	cache.stack = append(cache.stack, entry)
	cache.mu.Unlock()

	return nil
}

// isPrefix checks whether p is either equal to or a parent directory of child.
// It uses filepath.Clean so that extraneous separators are removed.
func isPrefix(p, child string) bool {
	p = filepath.Clean(p)
	child = filepath.Clean(child)
	if p == child {
		return true
	}
	// Ensure that p ends with a separator.
	pWithSep := p + string(filepath.Separator)
	return strings.HasPrefix(child, pWithSep)
}

// InvalidateForPath uses the current active full path (e.g. the directory of the file
// being accessed) to pop any directories from the DFS cache that are not in the current branch.
// In a DFS traversal, if the new active directory is not within the deepest cached directory,
// then the deeper (now stale) branches can be safely removed.
func (cache *DFSCache) invalidateForPath(activePath string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	activePath = filepath.Clean(activePath)

	// While there is something on the stack and the deepest (last) directory
	// is not a prefix of activePath, pop it.
	for len(cache.stack) > 0 {
		n := len(cache.stack) - 1
		top := cache.stack[n]
		if isPrefix(top.dirPath, activePath) {
			break
		}
		// Overwrite the popped entry with its zero value.
		cache.stack[n] = dirCacheEntry{}
		cache.stack = cache.stack[:n]
	}
}

// Lookup searches for file metadata in the currently valid DFS branch.
// It first calls InvalidateForPath to remove any stale cache entries based
// on the provided activePath. Then it scans the stack (from the deepest level)
// to see if the file is present.
func (cache *DFSCache) Lookup(activePath, fullFilePath string) (*VSSFileInfo, bool) {
	// Update the current DFS branch based on activePath.
	activePath = filepath.Clean(activePath)
	cache.invalidateForPath(activePath)

	// Clean up fullFilePath for consistency.
	fullFilePath = filepath.Clean(fullFilePath)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	// We only need to search the branch that directly contains the file.
	// Typically, that is the directory equal to filepath.Dir(fullFilePath).
	searchDir := filepath.Clean(filepath.Dir(fullFilePath))

	// Iterate from the deepest (current) directory upward.
	for i := len(cache.stack) - 1; i >= 0; i-- {
		entry := cache.stack[i]
		// If the cached directory is not the one that would contain the file,
		// skip it.
		if entry.dirPath != searchDir {
			continue
		}
		// Look for a matching file in the cached directory.
		for _, info := range entry.entries {
			if filepath.Join(entry.dirPath, info.Name) == fullFilePath {
				return info, true
			}
		}
	}
	return nil, false
}

// GetDirEntries returns the cached directory listing if there is an entry exactly
// matching dirPath.
func (cache *DFSCache) GetDirEntries(dirPath string) (ReadDirEntries, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	dirPath = filepath.Clean(dirPath)
	for _, entry := range cache.stack {
		if entry.dirPath == dirPath {
			return entry.entries, true
		}
	}
	return nil, false
}
