package utils

import (
	"container/list"
)

type entry struct {
	key   string
	value interface{}
}

type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	list     *list.List
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (l *LRUCache) Get(key string) (interface{}, bool) {
	if elem, found := l.cache[key]; found {
		l.list.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}
	return nil, false
}

func (l *LRUCache) Set(key string, value interface{}) {
	if elem, found := l.cache[key]; found {
		l.list.MoveToFront(elem)
		elem.Value.(*entry).value = value
		return
	}

	if l.list.Len() >= l.capacity {
		oldest := l.list.Back()
		if oldest != nil {
			oldestEntry := oldest.Value.(*entry)
			delete(l.cache, oldestEntry.key)
			l.list.Remove(oldest)
		}
	}

	newEntry := &entry{key: key, value: value}
	frontElem := l.list.PushFront(newEntry)
	l.cache[key] = frontElem
}
