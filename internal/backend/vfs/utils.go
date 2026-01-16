package vfs

import (
	"strings"
	"sync"
)

type MemcachedKeyPool struct {
	pool sync.Pool
}

func NewMemcachedKeyPool() *MemcachedKeyPool {
	return &MemcachedKeyPool{
		pool: sync.Pool{
			New: func() any {
				b := &strings.Builder{}
				b.Grow(250)
				return b
			},
		},
	}
}

func (p *MemcachedKeyPool) Get() *strings.Builder {
	return p.pool.Get().(*strings.Builder)
}

func (p *MemcachedKeyPool) Put(b *strings.Builder) {
	b.Reset()
	p.pool.Put(b)
}
