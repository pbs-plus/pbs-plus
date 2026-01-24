package pxar

import (
	"context"
	"io"
)

type rangeReader struct {
	ctx          context.Context
	client       *Client
	contentStart uint64
	contentEnd   uint64
	totalSize    uint64
	offset       uint64
}

func (r *rangeReader) Read(p []byte) (int, error) {
	if r.offset >= r.totalSize {
		return 0, io.EOF
	}

	readSize := uint(len(p))
	if remain := r.totalSize - r.offset; remain < uint64(readSize) {
		readSize = uint(remain)
	}

	n, err := r.client.Read(r.ctx, r.contentStart, r.contentEnd, r.offset, readSize, p)
	if err != nil {
		return n, err
	}
	if n == 0 && readSize > 0 {
		return 0, io.EOF
	}

	r.offset += uint64(n)
	return n, nil
}
