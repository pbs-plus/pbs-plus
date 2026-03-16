package native

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type DynamicEntry struct {
	End    uint64
	Digest [32]byte
}

type ChunkInfo struct {
	Start  uint64
	End    uint64
	Digest [32]byte
}

type DynamicIndexReader struct {
	file        *os.File
	uuid        [16]byte
	ctime       int64
	indexCsum   [32]byte
	entries     []DynamicEntry
	archiveSize uint64
}

func OpenDynamicIndex(path string) (*DynamicIndexReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dynamic index: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat dynamic index: %w", err)
	}

	if stat.Size() < DynamicIndexHeaderSize {
		f.Close()
		return nil, fmt.Errorf("index too small: %d bytes", stat.Size())
	}

	header := make([]byte, DynamicIndexHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	var magic [8]byte
	copy(magic[:], header[0:8])
	if magic != DynamicSizedChunkIndex10 {
		f.Close()
		return nil, fmt.Errorf("invalid magic: %x", magic)
	}

	r := &DynamicIndexReader{
		file: f,
	}
	copy(r.uuid[:], header[8:24])
	r.ctime = int64(binary.LittleEndian.Uint64(header[24:32]))
	copy(r.indexCsum[:], header[32:64])

	indexSize := stat.Size() - DynamicIndexHeaderSize
	numEntries := int(indexSize / DynamicIndexEntrySize)
	if int64(numEntries)*DynamicIndexEntrySize != indexSize {
		f.Close()
		return nil, fmt.Errorf("invalid index size")
	}

	r.entries = make([]DynamicEntry, numEntries)
	entryData := make([]byte, DynamicIndexEntrySize)
	for i := 0; i < numEntries; i++ {
		if _, err := io.ReadFull(f, entryData); err != nil {
			f.Close()
			return nil, fmt.Errorf("read entry %d: %w", i, err)
		}
		r.entries[i].End = binary.LittleEndian.Uint64(entryData[0:8])
		copy(r.entries[i].Digest[:], entryData[8:40])
	}

	if numEntries > 0 {
		r.archiveSize = r.entries[numEntries-1].End
	}

	return r, nil
}

func (r *DynamicIndexReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

func (r *DynamicIndexReader) EntryCount() int {
	return len(r.entries)
}

func (r *DynamicIndexReader) ArchiveSize() uint64 {
	return r.archiveSize
}

func (r *DynamicIndexReader) ChunkEnd(pos int) uint64 {
	if pos < 0 || pos >= len(r.entries) {
		return 0
	}
	return r.entries[pos].End
}

func (r *DynamicIndexReader) ChunkDigest(pos int) *[32]byte {
	if pos < 0 || pos >= len(r.entries) {
		return nil
	}
	return &r.entries[pos].Digest
}

func (r *DynamicIndexReader) ChunkInfo(pos int) *ChunkInfo {
	if pos < 0 || pos >= len(r.entries) {
		return nil
	}

	start := uint64(0)
	if pos > 0 {
		start = r.entries[pos-1].End
	}

	return &ChunkInfo{
		Start:  start,
		End:    r.entries[pos].End,
		Digest: r.entries[pos].Digest,
	}
}

func (r *DynamicIndexReader) BinarySearch(offset uint64) (int, error) {
	if len(r.entries) == 0 {
		return -1, fmt.Errorf("empty index")
	}

	startIdx := 0
	endIdx := len(r.entries) - 1
	start := uint64(0)
	end := r.entries[endIdx].End

	if offset >= end || offset < start {
		return -1, fmt.Errorf("offset out of range: %d", offset)
	}

	for startIdx <= endIdx {
		midIdx := (startIdx + endIdx) / 2
		midEnd := r.entries[midIdx].End

		if offset < midEnd {
			endIdx = midIdx - 1
		} else if offset >= midEnd && midIdx+1 < len(r.entries) && offset < r.entries[midIdx+1].End {
			return midIdx + 1, nil
		} else {
			startIdx = midIdx + 1
		}
	}

	if startIdx < len(r.entries) {
		return startIdx, nil
	}

	return -1, fmt.Errorf("offset not found: %d", offset)
}

func (r *DynamicIndexReader) ChunkFromOffset(offset uint64) (int, uint64, error) {
	idx, err := r.BinarySearch(offset)
	if err != nil {
		return -1, 0, err
	}

	info := r.ChunkInfo(idx)
	if info == nil {
		return -1, 0, fmt.Errorf("chunk info not found")
	}

	return idx, offset - info.Start, nil
}

func (r *DynamicIndexReader) UUID() [16]byte {
	return r.uuid
}

func (r *DynamicIndexReader) CTime() int64 {
	return r.ctime
}

func (r *DynamicIndexReader) IndexChecksum() [32]byte {
	return r.indexCsum
}

func (r *DynamicIndexReader) ComputeChecksum() ([32]byte, uint64) {
	h := NewSha256Hasher()
	var chunkEnd uint64
	for i := 0; i < len(r.entries); i++ {
		info := r.ChunkInfo(i)
		chunkEnd = info.End
		var endBytes [8]byte
		binary.LittleEndian.PutUint64(endBytes[:], info.End)
		h.Write(endBytes[:])
		h.Write(info.Digest[:])
	}
	return h.Sum256(), chunkEnd
}

func (r *DynamicIndexReader) VerifyChecksum() error {
	computed, _ := r.ComputeChecksum()
	if !bytes.Equal(computed[:], r.indexCsum[:]) {
		return fmt.Errorf("index checksum mismatch")
	}
	return nil
}
