//go:build linux

package arpcfs

import (
	"context"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	readaheadBlock  = 1 << 20
	readaheadWindow = 8
	readaheadStreak = 2
	readaheadRamp   = 8
)

// readaheadTimeout bounds each prefetch RPC; arpc enforces context deadlines on
// the stream, so a stalled agent cannot pin a fetch goroutine past it.
var readaheadTimeout = 2 * time.Minute

type raBlock struct {
	buf     []byte
	idx     int64
	n       int
	err     error
	refs    int
	fetched bool
	dropped bool
}

var raBlockPool = sync.Pool{New: func() any {
	return &raBlock{buf: make([]byte, readaheadBlock)}
}}

// readahead overlaps agent round trips for sequential reads; without it each
// 128 KiB kernel read waits a full round trip, so streams are latency-bound.
type readahead struct {
	file *ARPCFile

	mu     sync.Mutex
	cond   sync.Cond
	slots  [readaheadWindow]*raBlock
	next   int64
	streak int
	end    int64
	closed bool
}

func (r *readahead) init(file *ARPCFile) {
	r.file = file
	r.cond.L = &r.mu
	r.end = -1
}

// read returns ok=false to make the caller read directly, which keeps error handling unchanged.
func (r *readahead) read(p []byte, off int64) (n int, ok bool) {
	if r.file == nil || len(p) == 0 || off < 0 {
		return 0, false
	}
	first := off / readaheadBlock

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, false
	}
	switch {
	case off == r.next:
		r.streak++
	case r.streak >= readaheadStreak && r.block(first) != nil:
	default:
		r.reset()
		r.streak = 1
	}
	if e := off + int64(len(p)); e > r.next {
		r.next = e
	}
	if r.streak < readaheadStreak {
		return 0, false
	}
	ahead := min(int64(readaheadWindow), int64(1)<<min(r.streak/readaheadRamp, 30))
	for idx := first; idx < first+ahead; idx++ {
		if r.end >= 0 && idx*readaheadBlock >= r.end {
			break
		}
		if r.block(idx) == nil {
			r.start(idx)
		}
	}

	for n < len(p) {
		pos := off + int64(n)
		idx := pos / readaheadBlock
		b := r.block(idx)
		if b == nil {
			if r.end >= 0 && pos >= r.end {
				break
			}
			r.start(idx)
			b = r.block(idx)
		}
		b.refs++
		for !b.fetched {
			r.cond.Wait()
		}
		if b.err != nil {
			b.refs--
			r.recycle(b)
			r.reset()
			return 0, false
		}
		within := int(pos - idx*readaheadBlock)
		if within >= b.n {
			b.refs--
			r.recycle(b)
			break
		}
		r.mu.Unlock()
		c := copy(p[n:], b.buf[within:b.n])
		r.mu.Lock()
		b.refs--
		r.recycle(b)
		n += c
	}
	return n, true
}

// block requires r.mu.
func (r *readahead) block(idx int64) *raBlock {
	if b := r.slots[idx%readaheadWindow]; b != nil && b.idx == idx {
		return b
	}
	return nil
}

// start requires r.mu.
func (r *readahead) start(idx int64) {
	slot := idx % readaheadWindow
	if old := r.slots[slot]; old != nil {
		r.drop(slot)
	}
	b := raBlockPool.Get().(*raBlock)
	b.idx, b.n, b.err, b.refs, b.fetched, b.dropped = idx, 0, nil, 0, false, false
	r.slots[slot] = b
	go r.fetch(b)
}

func (r *readahead) fetch(b *raBlock) {
	var n int
	err := context.Canceled
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if !closed {
		fs := r.file.fs
		ctx, cancel := context.WithTimeout(fs.Ctx, readaheadTimeout)
		n, err = fs.fetchRemote(ctx, r.file.handleID, b.buf, b.idx*readaheadBlock)
		cancel()
		if err == nil {
			fs.TotalBytes.Add(int64(n))
		}
	}
	r.mu.Lock()
	b.n, b.err, b.fetched = n, err, true
	if err == nil && n < readaheadBlock {
		if e := b.idx*readaheadBlock + int64(n); r.end < 0 || e < r.end {
			r.end = e
		}
	}
	closed = r.closed
	r.recycle(b)
	r.cond.Broadcast()
	r.mu.Unlock()
	if err != nil && !closed && r.file.fs.Ctx.Err() == nil {
		log.Warn("readahead fetch failed, reading directly",
			"error", err, "offset", b.idx*readaheadBlock, "path", r.file.name)
	}
}

// drop requires r.mu.
func (r *readahead) drop(slot int64) {
	b := r.slots[slot]
	r.slots[slot] = nil
	b.dropped = true
	r.recycle(b)
}

// recycle requires r.mu.
func (r *readahead) recycle(b *raBlock) {
	if b.dropped && b.fetched && b.refs == 0 {
		b.err = nil
		raBlockPool.Put(b)
	}
}

// reset requires r.mu; in-flight fetches finish and recycle themselves.
func (r *readahead) reset() {
	for slot := range r.slots {
		if r.slots[slot] != nil {
			r.drop(int64(slot))
		}
	}
	r.streak = 0
}

// close stops new fetches without waiting: the agent finishes reads in flight
// before it closes the handle, and each fetch recycles its block when it ends.
func (r *readahead) close() {
	r.mu.Lock()
	r.closed = true
	r.reset()
	r.mu.Unlock()
}
