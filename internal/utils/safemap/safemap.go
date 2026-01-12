package safemap

import (
	"github.com/puzpuzpuz/xsync/v4"
)

type Map[K comparable, V any] struct {
	internal *xsync.Map[K, V]
}

func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		internal: xsync.NewMap[K, V](),
	}
}

func (sm *Map[K, V]) Set(key K, value V) {
	sm.internal.Store(key, value)
}

func (sm *Map[K, V]) Get(key K) (V, bool) {
	return sm.internal.Load(key)
}

func (sm *Map[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	return sm.internal.LoadOrStore(key, value)
}

func (sm *Map[K, V]) GetAndDel(key K) (value V, ok bool) {
	return sm.internal.LoadAndDelete(key)
}

func (sm *Map[K, V]) GetOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	return sm.internal.LoadOrCompute(key, func() (newValue V, cancel bool) {
		return valueFn(), false
	})
}

func (sm *Map[K, V]) Del(key K) {
	sm.internal.Delete(key)
}

func (sm *Map[K, V]) Len() int {
	return sm.internal.Size()
}

func (sm *Map[K, V]) ForEach(fn func(K, V) bool) {
	sm.internal.Range(fn)
}

func (sm *Map[K, V]) Clear() {
	sm.internal.Clear()
}
