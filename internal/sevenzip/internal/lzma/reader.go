package lzma

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const inputWindowSize = 256 << 10

// readCloser adapts the decoder to the sevenzip decompressor interface,
// closing the single upstream reader on Close.
type readCloser struct {
	c io.Closer
	d *core
}

func (rc *readCloser) Read(p []byte) (int, error) {
	if rc.d == nil {
		return 0, errAlreadyClosed
	}
	return rc.d.Read(p)
}

func (rc *readCloser) Close() error {
	if rc.c == nil || rc.d == nil {
		return errAlreadyClosed
	}
	if err := rc.c.Close(); err != nil {
		return fmt.Errorf("lzma: error closing: %w", err)
	}
	rc.d.release()
	rc.c, rc.d = nil, nil
	return nil
}

// NewReader returns a new LZMA io.ReadCloser. p holds the 7z coder
// properties (one props byte, dictSize uint32 LE); s is the uncompressed
// stream size.
func NewReader(p []byte, s uint64, readers []io.ReadCloser) (io.ReadCloser, error) {
	if len(readers) != 1 {
		return nil, errNeedOneReader
	}
	if len(p) != 5 {
		return nil, fmt.Errorf("lzma: expected 5 property bytes, got %d", len(p))
	}
	pr, err := propsForCode(p[0])
	if err != nil {
		return nil, fmt.Errorf("lzma: %w", err)
	}
	dictSize := int64(binary.LittleEndian.Uint32(p[1:5]))
	size := int64(s)
	if s == 1<<64-1 {
		size = -1
	}
	dictCap, err := pickDictCap(dictSize, size)
	if err != nil {
		return nil, err
	}
	d, err := newCore(pr, dictCap, size)
	if err != nil {
		return nil, err
	}
	d.r = readers[0]
	if err := d.initRangeDecoder(); err != nil {
		return nil, fmt.Errorf("lzma: %w", err)
	}
	return &readCloser{c: readers[0], d: d}, nil
}

// pickDictCap clamps the header dictionary size the same way upstream does:
// floor at MinDictCap, shrink to a smaller known stream size.
func pickDictCap(dictSize, size int64) (int, error) {
	if dictSize > maxDictCap {
		return 0, errDictSize
	}
	if dictSize < minDictCap {
		dictSize = minDictCap
	}
	if size >= 0 && size < dictSize && size > 0 {
		dictSize = size
	}
	if dictSize < minDictCap {
		dictSize = minDictCap
	}
	return int(dictSize), nil
}

func newCore(p props, dictCap int, size int64) (*core, error) {
	d := &core{
		in:   windowPool.Get().([]byte),
		dict: make([]byte, dictCap+1),
		size: size,
	}
	d.initModel(p)
	return d, nil
}

// release returns the pooled input window; the core must not be read after.
func (d *core) release() {
	if d.in != nil {
		windowPool.Put(d.in)
		d.in = nil
	}
}

var windowPool = sync.Pool{New: func() any { return make([]byte, inputWindowSize) }}
