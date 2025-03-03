package safemap

import (
	"fmt"
	"sync"

	"github.com/zeebo/xxh3"
)

// Map is a thread-safe sharded map implementation.
type Map[K comparable, V any] struct {
	shards [16]*shard[K, V]
}

// shard represents a single shard in the sharded map.
type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewMap creates a new Map with the specified number of shards.
func New[K comparable, V any]() *Map[K, V] {
	shards := [16]*shard[K, V]{}
	for i := 0; i < 16; i++ {
		shards[i] = &shard[K, V]{
			data: make(map[K]V),
		}
	}

	return &Map[K, V]{
		shards: shards,
	}
}

// getShard returns the shard corresponding to the given key.
func (sm *Map[K, V]) getShard(key K) *shard[K, V] {
	var hash uint64
	switch k := any(key).(type) {
	// Handle signed integers
	case int:
		hash = uint64(k)
	case int8:
		hash = uint64(k)
	case int16:
		hash = uint64(k)
	case int32:
		hash = uint64(k)
	case int64:
		hash = uint64(k)

	// Handle unsigned integers
	case uint:
		hash = uint64(k)
	case uint8:
		hash = uint64(k)
	case uint16:
		hash = uint64(k)
	case uint32:
		hash = uint64(k)
	case uint64:
		hash = k

	// Handle floating-point numbers
	case float32:
		hash = uint64(k) // Convert float32 to uint64
	case float64:
		hash = uint64(k) // Convert float64 to uint64

	// Handle strings
	case string:
		hash = xxh3.HashString(k)

	default:
		panic(fmt.Sprintf("unsupported key type: %T", key))
	}

	shardIndex := hash % uint64(len(sm.shards))
	return sm.shards[shardIndex]
}

// Set sets the value for a given key in the map.
func (sm *Map[K, V]) Set(key K, value V) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.data[key] = value
}

// Get retrieves the value for a given key from the map.
// The second return value indicates whether the key was found.
func (sm *Map[K, V]) Get(key K) (V, bool) {
	shard := sm.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, ok := shard.data[key]
	return value, ok
}

// GetOrSet retrieves the value for a given key if it exists.
// If the key does not exist, it sets the key with the provided value and returns it.
// The second return value indicates whether the value was loaded (true) or set (false).
func (sm *Map[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if the key already exists
	if existingValue, ok := shard.data[key]; ok {
		return existingValue, true // Value was loaded
	}

	// Key does not exist, set the value
	shard.data[key] = value
	return value, false // Value was set
}

// GetAndDel deletes the key from the map and returns the previous value if it existed.
func (sm *Map[K, V]) GetAndDel(key K) (value V, ok bool) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, ok = shard.data[key]
	if ok {
		delete(shard.data, key)
	}
	return
}

// GetOrCompute retrieves the value for a key or computes and sets it if it does not exist.
func (sm *Map[K, V]) GetOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if actual, loaded = shard.data[key]; loaded {
		return
	}

	actual = valueFn()
	shard.data[key] = actual
	return actual, false
}

// Delete removes a key from the map.
func (sm *Map[K, V]) Del(key K) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.data, key)
}

// Len returns the total number of key-value pairs in the map.
func (sm *Map[K, V]) Len() int {
	total := 0
	for _, shard := range sm.shards {
		shard.mu.RLock()
		total += len(shard.data)
		shard.mu.RUnlock()
	}
	return total
}

// ForEach iterates over all key-value pairs in the map and applies the given function.
// The iteration stops if the function returns false.
func (sm *Map[K, V]) ForEach(fn func(K, V) bool) {
	for _, shard := range sm.shards {
		shard.mu.RLock()
		for key, value := range shard.data {
			if !fn(key, value) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Clear removes all key-value pairs from the map.
func (sm *Map[K, V]) Clear() {
	for _, shard := range sm.shards {
		shard.mu.Lock()
		// Replace the old map with a new, empty map
		shard.data = make(map[K]V)
		shard.mu.Unlock()
	}
}
