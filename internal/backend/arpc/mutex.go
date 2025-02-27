package arpcfs

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

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
	h := xxhash.Sum64String(key)
	return &s.shards[uint32(h)&s.shardMask]
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
