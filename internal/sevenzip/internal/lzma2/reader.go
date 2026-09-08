// Package lzma2 implements the LZMA2 decompressor.
package lzma2

import (
	"errors"
	"fmt"
	"io"

	lzma "github.com/pbs-plus/pbs-plus/internal/sevenzip/internal/lzma"
)

var (
	errAlreadyClosed          = errors.New("lzma2: already closed")
	errNeedOneReader          = errors.New("lzma2: need exactly one reader")
	errInsufficientProperties = errors.New("lzma2: not enough properties")
	errInvalidProperties      = errors.New("lzma2: invalid properties")
)

type readCloser struct {
	c io.Closer
	r *lzma.Reader2
}

func (rc *readCloser) Read(p []byte) (int, error) {
	if rc.r == nil {
		return 0, errAlreadyClosed
	}
	return rc.r.Read(p)
}

func (rc *readCloser) Close() error {
	if rc.c == nil || rc.r == nil {
		return errAlreadyClosed
	}
	if err := rc.c.Close(); err != nil {
		return fmt.Errorf("lzma2: error closing: %w", err)
	}
	rc.c, rc.r = nil, nil
	return nil
}

// NewReader returns a new LZMA2 io.ReadCloser; p[0] encodes the dictionary
// capacity per the LZMA2 spec.
func NewReader(p []byte, _ uint64, readers []io.ReadCloser) (io.ReadCloser, error) {
	if len(readers) != 1 {
		return nil, errNeedOneReader
	}
	if len(p) != 1 {
		return nil, errInsufficientProperties
	}
	if p[0] > 40 {
		return nil, errInvalidProperties
	}
	dictCap := (2 | (int(p[0]) & 1)) << (p[0]/2 + 11)
	r2, err := lzma.NewReader2(dictCap, readers[0])
	if err != nil {
		return nil, fmt.Errorf("lzma2: error creating reader: %w", err)
	}
	return &readCloser{c: readers[0], r: r2}, nil
}
