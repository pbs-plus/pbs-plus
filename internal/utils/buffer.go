package utils

import "sync"

// BufferPool manages a pool of byte slices to reduce allocations
type BufferPool struct {
	// We'll use multiple pools based on size classes to avoid wasting memory
	pools []*sync.Pool
	sizes []int
}

// NewBufferPool creates a new buffer pool with common size classes
func NewBufferPool() *BufferPool {
	// Define common buffer size classes (powers of 2 are common)
	sizes := []int{
		1 << 10,
		4 << 10,
		16 << 10,
		64 << 10,
		256 << 10,
		1 << 20,
		4 << 20,
	}

	p := &BufferPool{
		pools: make([]*sync.Pool, len(sizes)),
		sizes: sizes,
	}

	// Initialize pools for each size class
	for i, size := range sizes {
		size := size // Create a new variable to avoid closure issues
		p.pools[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		}
	}

	return p
}

// Get returns a buffer of at least the requested size
func (p *BufferPool) Get(size int) []byte {
	// Find the smallest buffer size that fits the requested size
	for i, bufSize := range p.sizes {
		if bufSize >= size {
			// Get the buffer from the appropriate pool
			return p.pools[i].Get().([]byte)[:size]
		}
	}

	// If the requested size is larger than our largest pool, allocate a new buffer
	return make([]byte, size)
}

// Put returns a buffer to its appropriate pool
func (p *BufferPool) Put(buf []byte) {
	bufCap := cap(buf)

	// Find the appropriate pool for this buffer capacity
	for i, bufSize := range p.sizes {
		if bufSize == bufCap {
			// Reset the buffer before returning it to the pool
			p.pools[i].Put(buf[:bufCap])
			return
		}
	}

	// If the buffer doesn't match any of our pools, let GC handle it
}
