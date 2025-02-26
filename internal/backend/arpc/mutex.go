package arpcfs

import "sync"

type ShardedRWMutex struct {
	shards    []sync.RWMutex
	shardMask uint32
}

func NewShardedRWMutex(shardCount int) *ShardedRWMutex {
	// Ensure shardCount is a power of 2 for efficient masking
	if shardCount <= 0 || (shardCount&(shardCount-1)) != 0 {
		// Round up to the next power of 2
		shardCount--
		shardCount |= shardCount >> 1
		shardCount |= shardCount >> 2
		shardCount |= shardCount >> 4
		shardCount |= shardCount >> 8
		shardCount |= shardCount >> 16
		shardCount++
	}

	return &ShardedRWMutex{
		shards:    make([]sync.RWMutex, shardCount),
		shardMask: uint32(shardCount - 1),
	}
}

func (s *ShardedRWMutex) getShard(key string) *sync.RWMutex {
	h := fnv32a(key)
	return &s.shards[h&s.shardMask]
}

func (s *ShardedRWMutex) RLock(key string) {
	s.getShard(key).RLock()
}

func (s *ShardedRWMutex) RUnlock(key string) {
	s.getShard(key).RUnlock()
}

func (s *ShardedRWMutex) Lock(key string) {
	s.getShard(key).Lock()
}

func (s *ShardedRWMutex) Unlock(key string) {
	s.getShard(key).Unlock()
}

// FNV-1a 32-bit hash algorithm
func fnv32a(s string) uint32 {
	const prime32 = uint32(16777619)
	hash := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}
