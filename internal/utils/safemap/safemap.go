package safemap

import (
	"fmt"

	"github.com/puzpuzpuz/xsync/v3"
)

// Map is a thread-safe map implementation
type Map[K string, V any] struct {
	internal *xsync.Map
}

// New creates a new Map instance.
func New[K string, V any]() *Map[K, V] {
	return &Map[K, V]{
		internal: xsync.NewMap(),
	}
}

// Set sets the value for a given key in the map.
func (sm *Map[K, V]) Set(key K, value V) {
	sm.internal.Store(string(key), value)
}

// Get retrieves the value for a given key from the map.
// The second return value indicates whether the key was found.
func (sm *Map[K, V]) Get(key K) (V, bool) {
	value, ok := sm.internal.Load(string(key))
	if !ok {
		var zero V
		return zero, false
	}
	v, ok := value.(V)
	if !ok {
		panic(fmt.Sprintf("value for key %v is not of type %T", key, v))
	}
	return v, true
}

// GetOrSet retrieves the value for a given key if it exists.
// If the key does not exist, it sets the key with the provided value and returns it.
// The second return value indicates whether the value was loaded (true) or set (false).
func (sm *Map[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	actualAny, loaded := sm.internal.LoadOrStore(string(key), value)
	if !loaded {
		return value, false
	}
	actual, ok := actualAny.(V)
	if !ok {
		panic(fmt.Sprintf("value for key %v is not of type %T", key, actual))
	}
	return actual, true
}

// GetAndDel deletes the key from the map and returns the previous value if it existed.
func (sm *Map[K, V]) GetAndDel(key K) (value V, ok bool) {
	valueAny, ok := sm.internal.LoadAndDelete(string(key))
	if !ok {
		var zero V
		return zero, false
	}
	value, ok = valueAny.(V)
	if !ok {
		panic(fmt.Sprintf("value for key %v is not of type %T", key, value))
	}
	return value, true
}

// GetOrCompute retrieves the value for a key or computes and sets it if it does not exist.
func (sm *Map[K, V]) GetOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	actualAny, loaded := sm.internal.LoadOrCompute(string(key), func() any {
		return valueFn()
	})
	actual, ok := actualAny.(V)
	if !ok {
		panic(fmt.Sprintf("value for key %v is not of type %T", key, actual))
	}
	return actual, loaded
}

// Del removes a key from the map.
func (sm *Map[K, V]) Del(key K) {
	sm.internal.Delete(string(key))
}

// Len returns the total number of key-value pairs in the map.
func (sm *Map[K, V]) Len() int {
	return sm.internal.Size()
}

// ForEach iterates over all key-value pairs in the map and applies the given function.
// The iteration stops if the function returns false.
func (sm *Map[K, V]) ForEach(fn func(K, V) bool) {
	sm.internal.Range(func(key string, value any) bool {
		v, ok := value.(V)
		if !ok {
			panic(fmt.Sprintf("value for key %v is not of type %T", key, v))
		}
		return fn(K(key), v)
	})
}

// Clear removes all key-value pairs from the map.
func (sm *Map[K, V]) Clear() {
	sm.internal.Clear()
}
