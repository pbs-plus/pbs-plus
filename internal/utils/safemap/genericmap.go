package safemap

import (
	"sync"
	"sync/atomic"
)

// GenericMap is a thread-safe map implementation
type GenericMap[K comparable, V any] struct {
	mu    sync.RWMutex
	m     map[K]V
	count int64
}

// New creates a new GenericMap instance.
func NewGeneric[K comparable, V any]() *GenericMap[K, V] {
	return &GenericMap[K, V]{
		m: make(map[K]V),
	}
}

// Set sets the value for a given key in the map.
func (sm *GenericMap[K, V]) Set(key K, value V) {
	sm.mu.Lock()
	if _, exists := sm.m[key]; !exists {
		atomic.AddInt64(&sm.count, 1)
	}
	sm.m[key] = value
	sm.mu.Unlock()
}

// Get retrieves the value for a given key from the map.
// The second return value indicates whether the key was found.
func (sm *GenericMap[K, V]) Get(key K) (V, bool) {
	sm.mu.RLock()
	value, ok := sm.m[key]
	sm.mu.RUnlock()
	return value, ok
}

// GetOrSet retrieves the value for a given key if it exists.
// If the key does not exist, it sets the key with the provided value and returns it.
// The second return value indicates whether the value was loaded (true) or set (false).
func (sm *GenericMap[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	actual, loaded = sm.Get(key)
	if !loaded {
		sm.Set(key, value)
		actual = value
	}
	return actual, loaded
}

// GetAndDel deletes the key from the map and returns the previous value if it existed.
func (sm *GenericMap[K, V]) GetAndDel(key K) (value V, ok bool) {
	value, ok = sm.Get(key)
	if ok {
		sm.Del(key)
	}
	return value, ok
}

// GetOrCompute retrieves the value for a key or computes and sets it if it does not exist.
func (sm *GenericMap[K, V]) GetOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	actual, loaded = sm.Get(key)
	if !loaded {
		actual = valueFn()
		sm.Set(key, actual)
	}
	return actual, loaded
}

// Del removes a key from the map.
func (sm *GenericMap[K, V]) Del(key K) {
	sm.mu.Lock()
	if _, exists := sm.m[key]; exists {
		delete(sm.m, key)
		atomic.AddInt64(&sm.count, -1)
	}
	sm.mu.Unlock()
}

// Len returns the total number of key-value pairs in the map.
func (sm *GenericMap[K, V]) Len() int {
	return int(atomic.LoadInt64(&sm.count))
}

// ForEach iterates over all key-value pairs in the map and applies the given function.
// The iteration stops if the function returns false.
func (sm *GenericMap[K, V]) ForEach(fn func(K, V) bool) {
	sm.mu.RLock()
	for key, value := range sm.m {
		if !fn(key, value) {
			break
		}
	}
	sm.mu.RUnlock()
}

// Clear removes all key-value pairs from the map.
func (sm *GenericMap[K, V]) Clear() {
	sm.mu.Lock()
	sm.m = make(map[K]V)
	atomic.StoreInt64(&sm.count, 0)
	sm.mu.Unlock()
}
