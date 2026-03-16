package native

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type DynamicIndexWriter struct {
	file         *os.File
	uuid         [16]byte
	ctime        int64
	entries      []DynamicEntry
	entriesMu    sync.Mutex
	headerSize   int64
	finalized    bool
	archiveSize  uint64
	indexCsum    [32]byte
	reuseCsum    bool
	reuseCsumVal [32]byte
}

func NewDynamicIndexWriter(path string) (*DynamicIndexWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create dynamic index: %w", err)
	}

	id := uuid.New()
	now := time.Now().Unix()

	writer := &DynamicIndexWriter{
		file:       f,
		uuid:       id,
		ctime:      now,
		headerSize: DynamicIndexHeaderSize,
	}

	if err := writer.writeHeader(); err != nil {
		f.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}

	return writer, nil
}

func (w *DynamicIndexWriter) writeHeader() error {
	header := make([]byte, DynamicIndexHeaderSize)
	copy(header[0:8], DynamicSizedChunkIndex10[:])
	copy(header[8:24], w.uuid[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(w.ctime))

	if _, err := w.file.WriteAt(header, 0); err != nil {
		return err
	}

	return nil
}

func (w *DynamicIndexWriter) AddChunk(end uint64, digest [32]byte) error {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()

	if w.finalized {
		return fmt.Errorf("index already finalized")
	}

	w.entries = append(w.entries, DynamicEntry{
		End:    end,
		Digest: digest,
	})
	w.archiveSize = end

	return nil
}

func (w *DynamicIndexWriter) Finalize() error {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()

	if w.finalized {
		return nil
	}

	offset := w.headerSize
	entryData := make([]byte, DynamicIndexEntrySize)

	for _, entry := range w.entries {
		binary.LittleEndian.PutUint64(entryData[0:8], entry.End)
		copy(entryData[8:40], entry.Digest[:])

		if _, err := w.file.WriteAt(entryData, offset); err != nil {
			return fmt.Errorf("write entry: %w", err)
		}
		offset += DynamicIndexEntrySize
	}

	w.indexCsum = w.computeChecksum()

	checksumOffset := int64(32)
	checksumData := make([]byte, 32)
	copy(checksumData, w.indexCsum[:])
	if _, err := w.file.WriteAt(checksumData, checksumOffset); err != nil {
		return fmt.Errorf("write checksum: %w", err)
	}

	w.finalized = true
	return nil
}

func (w *DynamicIndexWriter) computeChecksum() [32]byte {
	h := sha256.New()
	for _, entry := range w.entries {
		var endBytes [8]byte
		binary.LittleEndian.PutUint64(endBytes[:], entry.End)
		h.Write(endBytes[:])
		h.Write(entry.Digest[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func (w *DynamicIndexWriter) Close() error {
	if w.file == nil {
		return nil
	}

	if !w.finalized {
		if err := w.Finalize(); err != nil {
			w.file.Close()
			return err
		}
	}

	err := w.file.Close()
	w.file = nil
	return err
}

func (w *DynamicIndexWriter) EntryCount() int {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()
	return len(w.entries)
}

func (w *DynamicIndexWriter) ArchiveSize() uint64 {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()
	return w.archiveSize
}

func (w *DynamicIndexWriter) UUID() [16]byte {
	return w.uuid
}

func (w *DynamicIndexWriter) CTime() int64 {
	return w.ctime
}

type ChunkWriter struct {
	store        *ChunkStoreWriter
	index        *DynamicIndexWriter
	offset       uint64
	chunker      *Chunker
	currentBuf   []byte
	bufOffset    int
	maxChunkSize int
}

type ChunkStoreWriter struct {
	root     string
	crypt    *CryptConfig
	compress bool
	mu       sync.Mutex
}

func NewChunkStoreWriter(root string, crypt *CryptConfig, compress bool) *ChunkStoreWriter {
	return &ChunkStoreWriter{
		root:     root,
		crypt:    crypt,
		compress: compress,
	}
}

func (s *ChunkStoreWriter) WriteChunk(data []byte) ([32]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	digest := Sha256Sum(data)

	blob, err := EncodeDataBlob(data, s.crypt, s.compress)
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode blob: %w", err)
	}

	hexDigest := fmt.Sprintf("%x", digest[:])
	dir4 := hexDigest[:4]
	dirPath := fmt.Sprintf("%s/.chunks/%s", s.root, dir4)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return [32]byte{}, fmt.Errorf("create chunk dir: %w", err)
	}

	chunkPath := fmt.Sprintf("%s/%s", dirPath, hexDigest)

	if _, err := os.Stat(chunkPath); err == nil {
		return digest, nil
	}

	tmpPath := chunkPath + ".tmp"
	if err := os.WriteFile(tmpPath, blob.RawData(), 0644); err != nil {
		os.Remove(tmpPath)
		return [32]byte{}, fmt.Errorf("write chunk: %w", err)
	}

	if err := os.Rename(tmpPath, chunkPath); err != nil {
		os.Remove(tmpPath)
		return [32]byte{}, fmt.Errorf("rename chunk: %w", err)
	}

	return digest, nil
}

func NewChunkWriter(store *ChunkStoreWriter, index *DynamicIndexWriter, maxChunkSize int) *ChunkWriter {
	if maxChunkSize <= 0 {
		maxChunkSize = 4 * 1024 * 1024
	}

	return &ChunkWriter{
		store:        store,
		index:        index,
		chunker:      NewChunker(maxChunkSize),
		currentBuf:   make([]byte, 0, maxChunkSize),
		maxChunkSize: maxChunkSize,
	}
}

func (w *ChunkWriter) Write(data []byte) (int, error) {
	w.currentBuf = append(w.currentBuf, data...)

	for len(w.currentBuf) >= w.maxChunkSize {
		chunkData := w.currentBuf[:w.maxChunkSize]
		if err := w.writeChunk(chunkData); err != nil {
			return 0, err
		}
		w.currentBuf = w.currentBuf[w.maxChunkSize:]
	}

	return len(data), nil
}

func (w *ChunkWriter) writeChunk(data []byte) error {
	digest, err := w.store.WriteChunk(data)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}

	w.offset += uint64(len(data))

	return w.index.AddChunk(w.offset, digest)
}

func (w *ChunkWriter) Flush() error {
	if len(w.currentBuf) == 0 {
		return nil
	}

	return w.writeChunk(w.currentBuf)
}

func (w *ChunkWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.index.Close()
}

type Chunker struct {
	minChunkSize int
	maxChunkSize int
}

func NewChunker(maxSize int) *Chunker {
	minSize := maxSize / 4
	if minSize < 64*1024 {
		minSize = 64 * 1024
	}

	return &Chunker{
		minChunkSize: minSize,
		maxChunkSize: maxSize,
	}
}

func (c *Chunker) ChunkBoundaries(data []byte) []int {
	if len(data) <= c.maxChunkSize {
		return []int{len(data)}
	}

	boundaries := make([]int, 0)
	offset := 0

	for offset < len(data) {
		remaining := len(data) - offset
		if remaining <= c.maxChunkSize {
			boundaries = append(boundaries, len(data))
			break
		}

		chunkSize := c.findChunkBoundary(data[offset:])
		boundaries = append(boundaries, offset+chunkSize)
		offset += chunkSize
	}

	return boundaries
}

func (c *Chunker) findChunkBoundary(data []byte) int {
	if len(data) <= c.minChunkSize {
		return len(data)
	}

	searchStart := c.minChunkSize
	searchEnd := c.maxChunkSize
	if searchEnd > len(data) {
		searchEnd = len(data)
	}

	for i := searchStart; i < searchEnd; i++ {
		if c.isChunkBoundary(data, i) {
			return i
		}
	}

	return searchEnd
}

func (c *Chunker) isChunkBoundary(data []byte, pos int) bool {
	if pos < 48 {
		return false
	}

	window := data[pos-48 : pos]
	hash := uint32(0)
	for _, b := range window {
		hash = hash*31 + uint32(b)
	}

	return (hash & 0xFFFF) == 0
}

var _ io.Writer = (*ChunkWriter)(nil)
