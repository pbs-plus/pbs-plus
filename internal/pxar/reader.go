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

	buf       []byte
	bufOffset uint64
	bufValid  int

	consecutiveReads int
	currentBufSize   int
}

func (r *rangeReader) Read(p []byte) (int, error) {
	if r.offset >= r.totalSize {
		return 0, io.EOF
	}

	totalRead := 0

	for totalRead < len(p) && r.offset < r.totalSize {
		if r.buf == nil || r.offset < r.bufOffset || r.offset >= r.bufOffset+uint64(r.bufValid) {
			if err := r.fillBuffer(); err != nil {
				if totalRead > 0 {
					return totalRead, nil
				}
				return 0, err
			}
		}

		bufPos := int(r.offset - r.bufOffset)
		available := r.bufValid - bufPos
		toCopy := min(len(p)-totalRead, available)

		remaining := r.totalSize - r.offset
		if uint64(toCopy) > remaining {
			toCopy = int(remaining)
		}

		copy(p[totalRead:], r.buf[bufPos:bufPos+toCopy])
		totalRead += toCopy
		r.offset += uint64(toCopy)
	}

	if totalRead == 0 && r.offset >= r.totalSize {
		return 0, io.EOF
	}

	return totalRead, nil
}

func (r *rangeReader) Close() error {
	r.buf = nil
	return nil
}

const (
	minReadAheadSize = 256 * 1024
	maxReadAheadSize = 4 * 1024 * 1024
)

func (r *rangeReader) fillBuffer() error {
	if r.currentBufSize == 0 {
		r.currentBufSize = minReadAheadSize
	} else if r.consecutiveReads > 2 && r.currentBufSize < maxReadAheadSize {
		r.currentBufSize = min(r.currentBufSize*2, maxReadAheadSize)
	}

	r.consecutiveReads++

	if r.buf == nil || len(r.buf) < r.currentBufSize {
		r.buf = make([]byte, r.currentBufSize)
	}

	remaining := r.totalSize - r.offset
	readSize := uint(r.currentBufSize)
	if uint64(readSize) > remaining {
		readSize = uint(remaining)
	}

	if readSize == 0 {
		return io.EOF
	}

	n, err := r.client.Read(r.ctx, r.contentStart, r.contentEnd, r.offset, readSize, r.buf[:readSize])
	if err != nil {
		return err
	}
	if n == 0 {
		return io.EOF
	}

	r.bufOffset = r.offset
	r.bufValid = n

	return nil
}
