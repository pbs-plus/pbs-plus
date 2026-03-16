package native

import (
	"fmt"
	"io"
	"sync"
)

type BufferedReader struct {
	store       *ChunkStore
	index       *DynamicIndexReader
	archiveSize uint64
	offset      uint64

	mu           sync.Mutex
	currentIdx   int
	currentData  []byte
	currentStart uint64
}

func NewBufferedReader(index *DynamicIndexReader, store *ChunkStore) *BufferedReader {
	return &BufferedReader{
		store:       store,
		index:       index,
		archiveSize: index.ArchiveSize(),
		offset:      0,
		currentIdx:  -1,
	}
}

func (r *BufferedReader) Size() uint64 {
	return r.archiveSize
}

func (r *BufferedReader) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if uint64(offset) >= r.archiveSize {
		return 0, io.EOF
	}

	totalRead := 0
	remaining := len(p)

	for remaining > 0 && uint64(offset) < r.archiveSize {
		if r.currentData == nil || uint64(offset) < r.currentStart || uint64(offset) >= r.currentStart+uint64(len(r.currentData)) {
			if err := r.loadChunkForOffset(uint64(offset)); err != nil {
				return totalRead, err
			}
		}

		chunkOffset := uint64(offset) - r.currentStart
		available := len(r.currentData) - int(chunkOffset)
		toCopy := min(remaining, available)

		copy(p[totalRead:totalRead+toCopy], r.currentData[chunkOffset:chunkOffset+uint64(toCopy)])
		totalRead += toCopy
		offset += int64(toCopy)
		remaining -= toCopy
	}

	if totalRead == 0 && uint64(offset) >= r.archiveSize {
		return 0, io.EOF
	}

	return totalRead, nil
}

func (r *BufferedReader) loadChunkForOffset(offset uint64) error {
	if offset == r.archiveSize {
		r.currentData = nil
		r.currentIdx = -1
		return nil
	}

	if r.currentIdx >= 0 && r.currentData != nil {
		currentEnd := r.currentStart + uint64(len(r.currentData))
		if offset >= currentEnd && r.currentIdx+1 < r.index.EntryCount() {
			nextInfo := r.index.ChunkInfo(r.currentIdx + 1)
			if nextInfo != nil && offset < nextInfo.End {
				data, err := r.store.ReadChunk(nextInfo.Digest)
				if err != nil {
					return fmt.Errorf("read chunk %d: %w", r.currentIdx+1, err)
				}

				r.currentIdx++
				r.currentData = data
				r.currentStart = nextInfo.Start
				return nil
			}
		}
	}

	idx, err := r.index.BinarySearch(offset)
	if err != nil {
		return fmt.Errorf("locate chunk for offset %d: %w", offset, err)
	}

	info := r.index.ChunkInfo(idx)
	if info == nil {
		return fmt.Errorf("chunk info not found for index %d", idx)
	}

	data, err := r.store.ReadChunk(info.Digest)
	if err != nil {
		return fmt.Errorf("read chunk %d: %w", idx, err)
	}

	r.currentIdx = idx
	r.currentData = data
	r.currentStart = info.Start

	return nil
}

func (r *BufferedReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, int64(r.offset))
	r.offset += uint64(n)
	return n, err
}

func (r *BufferedReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = int64(r.offset) + offset
	case io.SeekEnd:
		newOffset = int64(r.archiveSize) + offset
	}

	if newOffset < 0 || newOffset > int64(r.archiveSize) {
		return 0, fmt.Errorf("seek out of range: %d", newOffset)
	}

	r.offset = uint64(newOffset)

	if r.currentData != nil {
		if r.offset < r.currentStart || r.offset >= r.currentStart+uint64(len(r.currentData)) {
			r.currentData = nil
			r.currentIdx = -1
		}
	}

	return newOffset, nil
}

var _ io.Reader = (*BufferedReader)(nil)
var _ io.ReaderAt = (*BufferedReader)(nil)
var _ io.Seeker = (*BufferedReader)(nil)
