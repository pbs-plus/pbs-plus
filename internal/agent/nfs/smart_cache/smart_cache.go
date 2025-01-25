//go:build windows

package smart_cache

import (
	"io/fs"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/arc/v2"
	"github.com/willscott/go-nfs"
)

const (
	initialHandleCacheSize = 50000
	maxHandleCacheSize     = 200000
	verifierCacheSize      = 10000
	resizeThreshold        = 0.85 // Higher threshold
	resizeStep             = 0.15 // Smaller steps
)

func NewSmartCachingHandler(h nfs.Handler) nfs.Handler {
	cache, _ := arc.NewARC[uuid.UUID, entry](initialHandleCacheSize)
	verifiers, _ := arc.NewARC[uint64, verifier](verifierCacheSize)

	return &CachingHandler{
		Handler:         h,
		activeHandles:   cache,
		reverseHandles:  sync.Map{},
		activeVerifiers: verifiers,
		stats:           newCacheStats(),
	}
}

type CachingHandler struct {
	nfs.Handler
	activeHandles   *arc.ARCCache[uuid.UUID, entry]
	reverseHandles  sync.Map // map[reverseKey]uuid.UUID
	activeVerifiers *arc.ARCCache[uint64, verifier]
	stats           *cacheStats
	mu              sync.RWMutex
}

type reverseKey struct {
	path string
	fs   billy.Filesystem
}

type entry struct {
	f  billy.Filesystem
	p  []string
	rk reverseKey
}

type cacheStats struct {
	hits   uint64
	misses uint64
	mu     sync.Mutex
}

func newCacheStats() *cacheStats {
	return &cacheStats{}
}

func (c *CachingHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	joinedPath := f.Join(path...)
	rk := reverseKey{path: joinedPath, fs: f}

	if handle, exists := c.reverseHandles.Load(rk); exists {
		if id, ok := handle.(uuid.UUID); ok {
			if _, valid := c.activeHandles.Get(id); valid {
				c.stats.mu.Lock()
				c.stats.hits++
				c.stats.mu.Unlock()
				b, _ := id.MarshalBinary()
				return b
			}
		}
		c.reverseHandles.Delete(rk)
	}

	c.stats.mu.Lock()
	c.stats.misses++
	c.stats.mu.Unlock()

	id := uuid.New()
	newPath := make([]string, len(path))
	copy(newPath, path)

	ent := entry{f: f, p: newPath, rk: rk}
	c.activeHandles.Add(id, ent)
	c.reverseHandles.Store(rk, id)

	b, _ := id.MarshalBinary()
	return b
}

func (c *CachingHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	id, err := uuid.FromBytes(fh)
	if err != nil {
		return nil, nil, err
	}

	if f, ok := c.activeHandles.Get(id); ok {
		c.stats.mu.Lock()
		c.stats.hits++
		c.stats.mu.Unlock()
		return f.f, append([]string{}, f.p...), nil
	}

	c.stats.mu.Lock()
	c.stats.misses++
	c.stats.mu.Unlock()
	return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
}

func (c *CachingHandler) adjustCacheSize() {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()

	total := c.stats.hits + c.stats.misses
	if total == 0 {
		return
	}

	hitRate := float64(c.stats.hits) / float64(total)
	currentSize := c.activeHandles.Len()
	var newSize int

	switch {
	case hitRate < resizeThreshold:
		newSize = currentSize + int(float64(currentSize)*resizeStep)
	case hitRate > 0.9:
		newSize = currentSize - int(float64(currentSize)*0.1)
	default:
		return
	}

	newSize = clamp(newSize, 10, maxHandleCacheSize)

	if newSize != currentSize {
		c.mu.Lock()
		defer c.mu.Unlock()

		newCache, _ := arc.NewARC[uuid.UUID, entry](newSize)
		for _, k := range c.activeHandles.Keys() {
			if v, ok := c.activeHandles.Get(k); ok {
				newCache.Add(k, v)
			}
		}
		c.activeHandles = newCache
	}

	c.stats.hits = 0
	c.stats.misses = 0
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

type verifier struct {
	path     string
	contents []fs.FileInfo
}

func (c *CachingHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	id := hashPathAndContents(path, contents)
	c.activeVerifiers.Add(id, verifier{path, contents})
	return id
}

func (c *CachingHandler) DataForVerifier(id uint64) []fs.FileInfo {
	if cache, ok := c.activeVerifiers.Get(id); ok {
		return cache.contents
	}
	return nil
}

func hashPathAndContents(path string, contents []fs.FileInfo) uint64 {
	h := xxhash.New()
	h.Write([]byte(path))
	for _, c := range contents {
		h.Write([]byte(c.Name()))
		h.Write([]byte(c.Mode().String()))
		h.Write([]byte(c.ModTime().String()))
	}

	return h.Sum64()
}

func (c *CachingHandler) InvalidateHandle(_ billy.Filesystem, handle []byte) error {
	id, err := uuid.FromBytes(handle)
	if err != nil {
		return err
	}

	if entry, ok := c.activeHandles.Get(id); ok {
		c.reverseHandles.Delete(entry.rk)
		c.activeHandles.Remove(id)
	}

	return nil
}

func (c *CachingHandler) HandleLimit() int {
	return maxHandleCacheSize
}
