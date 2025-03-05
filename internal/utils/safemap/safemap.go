package safemap

import (
	"fmt"
	"runtime"

	csmap "github.com/mhmtszr/concurrent-swiss-map"
	"github.com/zeebo/xxh3"
)

// Map is a thread-safe map implementation using concurrent-swiss-map.
type Map[K comparable, V any] struct {
	internal *csmap.CsMap[K, V]
}

// New creates a new Map instance with 64 shards and xxh3 hashing.
func New[K comparable, V any]() *Map[K, V] {
	numShards := uint64(runtime.NumCPU())
	return &Map[K, V]{
		internal: csmap.Create(
			// Set the number of shards to 64 for high-concurrency workloads.

			csmap.WithShardCount[K, V](numShards),

			// Use xxh3 for fast and efficient hashing.
			csmap.WithCustomHasher[K, V](func(key K) uint64 {
				return xxh3.HashString(fmt.Sprintf("%v", key)) // Hash the string representation of the key
			}),
		),
	}
}

// Set sets the value for a given key in the map.
func (sm *Map[K, V]) Set(key K, value V) {
	sm.internal.Store(key, value)
}

// Get retrieves the value for a given key from the map.
// The second return value indicates whether the key was found.
func (sm *Map[K, V]) Get(key K) (V, bool) {
	return sm.internal.Load(key)
}

// GetOrSet retrieves the value for a given key if it exists.
// If the key does not exist, it sets the key with the provided value and returns it.
// The second return value indicates whether the value was loaded (true) or set (false).
func (sm *Map[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	actual, loaded = sm.internal.Load(key)
	if !loaded {
		sm.internal.Store(key, value)
	}

	return actual, loaded
}

// GetAndDel deletes the key from the map and returns the previous value if it existed.
func (sm *Map[K, V]) GetAndDel(key K) (value V, ok bool) {
	value, ok = sm.internal.Load(key)
	if ok {
		sm.internal.Delete(key)
	}
	return value, ok
}

// GetOrCompute retrieves the value for a key or computes and sets it if it does not exist.
func (sm *Map[K, V]) GetOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	// Use SetIf to implement GetOrCompute
	sm.internal.SetIf(key, func(previousValue V, previousFound bool) (value V, set bool) {
		if previousFound {
			return previousValue, false // Do not overwrite
		}
		return valueFn(), true // Compute and set the value
	})
	actual, loaded = sm.internal.Load(key)
	return actual, loaded
}

// Del removes a key from the map.
func (sm *Map[K, V]) Del(key K) {
	sm.internal.Delete(key)
}

// Len returns the total number of key-value pairs in the map.
func (sm *Map[K, V]) Len() int {
	return sm.internal.Count()
}

// ForEach iterates over all key-value pairs in the map and applies the given function.
// The iteration stops if the function returns false.
func (sm *Map[K, V]) ForEach(fn func(K, V) bool) {
	sm.internal.Range(func(key K, value V) (stop bool) {
		return !fn(key, value) // Stop if fn returns false
	})
}

// Clear removes all key-value pairs from the map.
func (sm *Map[K, V]) Clear() {
	sm.internal.Clear()
}
