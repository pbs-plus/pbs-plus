package safemap

import (
	"sync"
)

// Map is a thread-safe map implementation using sync.Map.
type Map[K comparable, V any] struct {
	internal sync.Map
}

// New creates a new Map instance.
func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{}
}

// Set sets the value for a given key in the map.
func (sm *Map[K, V]) Set(key K, value V) {
	sm.internal.Store(key, value)
}

// Get retrieves the value for a given key from the map.
// The second return value indicates whether the key was found.
func (sm *Map[K, V]) Get(key K) (V, bool) {
	value, ok := sm.internal.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return value.(V), true
}

// GetOrSet retrieves the value for a given key if it exists.
// If the key does not exist, it sets the key with the provided value and returns it.
// The second return value indicates whether the value was loaded (true) or set (false).
func (sm *Map[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	actual, loaded = sm.Get(key)
	if !loaded {
		sm.Set(key, value)
		actual = value
	}
	return actual, loaded
}

// GetAndDel deletes the key from the map and returns the previous value if it existed.
func (sm *Map[K, V]) GetAndDel(key K) (value V, ok bool) {
	value, ok = sm.Get(key)
	if ok {
		sm.Del(key)
	}
	return value, ok
}

// GetOrCompute retrieves the value for a key or computes and sets it if it does not exist.
func (sm *Map[K, V]) GetOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	actual, loaded = sm.Get(key)
	if !loaded {
		actual = valueFn()
		sm.Set(key, actual)
	}
	return actual, loaded
}

// Del removes a key from the map.
func (sm *Map[K, V]) Del(key K) {
	sm.internal.Delete(key)
}

// Len returns the total number of key-value pairs in the map.
// Note: sync.Map does not provide a direct way to get the length, so we iterate over all elements.
func (sm *Map[K, V]) Len() int {
	count := 0
	sm.internal.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// ForEach iterates over all key-value pairs in the map and applies the given function.
// The iteration stops if the function returns false.
func (sm *Map[K, V]) ForEach(fn func(K, V) bool) {
	sm.internal.Range(func(key, value any) bool {
		return fn(key.(K), value.(V))
	})
}

// Clear removes all key-value pairs from the map.
// Note: sync.Map does not provide a direct way to clear all elements, so we delete them one by one.
func (sm *Map[K, V]) Clear() {
	sm.internal.Range(func(key, _ any) bool {
		sm.internal.Delete(key)
		return true
	})
}
