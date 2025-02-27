package arpcfs

import (
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

// cacheLineSize is the typical size (in bytes) of a CPU cache line.
const cacheLineSize = 64

// Calculate how many bytes to pad so that each lock occupies a full cache line.
const padSize = cacheLineSize - int(unsafe.Sizeof(sync.RWMutex{}))

// paddedRWMutex wraps a sync.RWMutex along with padding to reduce false sharing.
type paddedRWMutex struct {
	mu  sync.RWMutex
	pad [padSize]byte
}

// ShardedRWMutex holds multiple locks (shards) to reduce contention.
type ShardedRWMutex struct {
	shards    []paddedRWMutex
	shardMask uint32
}

// NewShardedRWMutex creates a new ShardedRWMutex with at least shardCount shards.
func NewShardedRWMutex(shardCount int) *ShardedRWMutex {
	if shardCount <= 0 || (shardCount&(shardCount-1)) != 0 {
		shardCount--
		shardCount |= shardCount >> 1
		shardCount |= shardCount >> 2
		shardCount |= shardCount >> 4
		shardCount |= shardCount >> 8
		shardCount |= shardCount >> 16
		shardCount++
	}

	return &ShardedRWMutex{
		shards:    make([]paddedRWMutex, shardCount),
		shardMask: uint32(shardCount - 1),
	}
}

// getShard returns the pointer to the lock for the given key.
func (s *ShardedRWMutex) getShard(key string) *sync.RWMutex {
	h := xxHashValue(key)
	return &s.shards[h&s.shardMask].mu
}

// RLock locks the shard associated with the given key for reading.
func (s *ShardedRWMutex) RLock(key string) {
	s.getShard(key).RLock()
}

// RUnlock unlocks the shard associated with the given key for reading.
func (s *ShardedRWMutex) RUnlock(key string) {
	s.getShard(key).RUnlock()
}

// Lock locks the shard associated with the given key for writing.
func (s *ShardedRWMutex) Lock(key string) {
	s.getShard(key).Lock()
}

// Unlock unlocks the shard associated with the given key for writing.
func (s *ShardedRWMutex) Unlock(key string) {
	s.getShard(key).Unlock()
}

// xxHashValue calculates the 32-bit hash of a string using xxHash.
// It computes a 64-bit hash and then returns the lower 32 bits.
func xxHashValue(s string) uint32 {
	h := xxhash.Sum64String(s)
	return uint32(h)
}
