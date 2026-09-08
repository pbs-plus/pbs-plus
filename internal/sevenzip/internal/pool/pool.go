// Package pool implements the reader pooling.
package pool

import (
	"container/list"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/sevenzip/internal/util"
)

// Pooler is the interface implemented by a pool.
type Pooler interface {
	Get(offset int64) (util.SizeReadSeekCloser, bool)
	Put(offset int64, rc util.SizeReadSeekCloser) (bool, error)
}

// Constructor is the function prototype used to instantiate a pool.
type Constructor func() (Pooler, error)

type noopPool struct{}

// NewNoopPool returns a Pooler that doesn't actually pool anything.
func NewNoopPool() (Pooler, error) {
	return new(noopPool), nil
}

func (noopPool) Get(_ int64) (util.SizeReadSeekCloser, bool) {
	return nil, false
}

func (noopPool) Put(_ int64, rc util.SizeReadSeekCloser) (bool, error) {
	return false, rc.Close()
}

// poolSize caps pooled folder decoders: every entry holds a live LZMA
// dictionary (16MB+ at default 7z settings), so NumCPU entries can pin
// hundreds of MB per solid folder. Two entries cover sequential reads;
// strictly reverse-order walks restart decoders more often as a result.
const poolSize = 2

type pool struct {
	mutex     sync.Mutex
	size      int
	evictList *list.List
	items     map[int64]*list.Element
}

type entry struct {
	key   int64
	value util.SizeReadSeekCloser
}

// NewPool returns a Pooler that uses a LRU strategy to maintain a fixed pool
// of util.SizeReadSeekCloser's keyed by their stream offset.
func NewPool() (Pooler, error) {
	return &pool{
		size:      poolSize,
		evictList: list.New(),
		items:     make(map[int64]*list.Element),
	}, nil
}

// Get returns the pooled reader at exactly offset, or the one with the
// largest key below it. The pool holds at most poolSize entries, so a
// linear scan avoids the alloc+sort a sorted-key lookup would need.
func (p *pool) Get(offset int64) (util.SizeReadSeekCloser, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if ent, ok := p.items[offset]; ok {
		_ = p.removeElement(ent, false)
		return ent.Value.(*entry).value, true
	}

	var best *list.Element
	for _, ent := range p.items {
		k := ent.Value.(*entry).key
		if k >= offset {
			continue
		}
		if best == nil || k > best.Value.(*entry).key {
			best = ent
		}
	}
	if best == nil {
		return nil, false
	}
	_ = p.removeElement(best, false)
	return best.Value.(*entry).value, true
}

func (p *pool) Put(offset int64, rc util.SizeReadSeekCloser) (bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, ok := p.items[offset]; ok {
		return false, nil
	}

	ent := &entry{offset, rc}
	entry := p.evictList.PushFront(ent)
	p.items[offset] = entry

	var err error

	evict := p.evictList.Len() > p.size
	if evict {
		err = p.removeOldest()
	}

	return evict, err
}

func (p *pool) removeOldest() error {
	if ent := p.evictList.Back(); ent != nil {
		return p.removeElement(ent, true)
	}

	return nil
}

func (p *pool) removeElement(e *list.Element, cb bool) error {
	p.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(p.items, kv.key)

	if cb {
		return kv.value.Close()
	}

	return nil
}
