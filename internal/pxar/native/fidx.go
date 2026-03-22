package native

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const FixedIndexHeaderSize = 4096
const FixedIndexEntrySize = 32

type FixedIndexWriter struct {
	file       *os.File
	uuid       [16]byte
	ctime      int64
	size       uint64
	chunkSize  uint64
	entries    [][32]byte
	entriesMu  sync.Mutex
	headerSize int64
	finalized  bool
	indexCsum  [32]byte
}

func NewFixedIndexWriter(path string, size uint64, chunkSize uint64) (*FixedIndexWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create fixed index: %w", err)
	}

	id := uuid.New()
	now := time.Now().Unix()

	writer := &FixedIndexWriter{
		file:       f,
		uuid:       id,
		ctime:      now,
		size:       size,
		chunkSize:  chunkSize,
		headerSize: FixedIndexHeaderSize,
	}

	numChunks := (size + chunkSize - 1) / chunkSize
	writer.entries = make([][32]byte, numChunks)

	if err := writer.writeHeader(); err != nil {
		f.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}

	return writer, nil
}

func (w *FixedIndexWriter) writeHeader() error {
	header := make([]byte, FixedIndexHeaderSize)
	copy(header[0:8], FixedSizedChunkIndex10[:])
	copy(header[8:24], w.uuid[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(w.ctime))
	binary.LittleEndian.PutUint64(header[32:40], w.size)
	binary.LittleEndian.PutUint64(header[40:48], w.chunkSize)

	if _, err := w.file.WriteAt(header, 0); err != nil {
		return err
	}

	return nil
}

func (w *FixedIndexWriter) AddChunk(index int, digest [32]byte) error {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()

	if w.finalized {
		return fmt.Errorf("index already finalized")
	}

	if index < 0 || index >= len(w.entries) {
		return fmt.Errorf("chunk index out of range: %d", index)
	}

	w.entries[index] = digest
	return nil
}

func (w *FixedIndexWriter) Finalize() error {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()

	if w.finalized {
		return nil
	}

	offset := w.headerSize
	entryData := make([]byte, FixedIndexEntrySize)

	for _, digest := range w.entries {
		copy(entryData[0:32], digest[:])

		if _, err := w.file.WriteAt(entryData, offset); err != nil {
			return fmt.Errorf("write entry: %w", err)
		}
		offset += FixedIndexEntrySize
	}

	w.indexCsum = w.computeChecksum()

	checksumOffset := int64(48)
	checksumData := make([]byte, 32)
	copy(checksumData, w.indexCsum[:])
	if _, err := w.file.WriteAt(checksumData, checksumOffset); err != nil {
		return fmt.Errorf("write checksum: %w", err)
	}

	w.finalized = true
	return nil
}

func (w *FixedIndexWriter) computeChecksum() [32]byte {
	h := sha256.New()
	for _, digest := range w.entries {
		h.Write(digest[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func (w *FixedIndexWriter) Close() error {
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

func (w *FixedIndexWriter) ChunkCount() int {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()
	return len(w.entries)
}

func (w *FixedIndexWriter) Size() uint64 {
	return w.size
}

func (w *FixedIndexWriter) ChunkSize() uint64 {
	return w.chunkSize
}

func (w *FixedIndexWriter) UUID() [16]byte {
	return w.uuid
}

func (w *FixedIndexWriter) CTime() int64 {
	return w.ctime
}

type FixedIndexReader struct {
	file       *os.File
	uuid       [16]byte
	ctime      int64
	size       uint64
	chunkSize  uint64
	entries    [][32]byte
	headerSize int64
}

func OpenFixedIndex(path string) (*FixedIndexReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open fixed index: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat fixed index: %w", err)
	}

	if stat.Size() < FixedIndexHeaderSize {
		f.Close()
		return nil, fmt.Errorf("index too small: %d bytes", stat.Size())
	}

	header := make([]byte, FixedIndexHeaderSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	var magic [8]byte
	copy(magic[:], header[0:8])
	if magic != FixedSizedChunkIndex10 {
		f.Close()
		return nil, fmt.Errorf("invalid magic: %x", magic)
	}

	r := &FixedIndexReader{
		file:       f,
		headerSize: FixedIndexHeaderSize,
	}

	copy(r.uuid[:], header[8:24])
	r.ctime = int64(binary.LittleEndian.Uint64(header[24:32]))
	r.size = binary.LittleEndian.Uint64(header[32:40])
	r.chunkSize = binary.LittleEndian.Uint64(header[40:48])

	numChunks := (r.size + r.chunkSize - 1) / r.chunkSize
	r.entries = make([][32]byte, numChunks)

	entryData := make([]byte, FixedIndexEntrySize)
	for i := uint64(0); i < numChunks; i++ {
		offset := int64(FixedIndexHeaderSize + i*FixedIndexEntrySize)
		if _, err := f.ReadAt(entryData, offset); err != nil {
			f.Close()
			return nil, fmt.Errorf("read entry %d: %w", i, err)
		}
		copy(r.entries[i][:], entryData[0:32])
	}

	return r, nil
}

func (r *FixedIndexReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

func (r *FixedIndexReader) ChunkCount() int {
	return len(r.entries)
}

func (r *FixedIndexReader) Size() uint64 {
	return r.size
}

func (r *FixedIndexReader) ChunkSize() uint64 {
	return r.chunkSize
}

func (r *FixedIndexReader) ChunkDigest(index int) *[32]byte {
	if index < 0 || index >= len(r.entries) {
		return nil
	}
	return &r.entries[index]
}

func (r *FixedIndexReader) ChunkOffset(index int) uint64 {
	if index < 0 || index >= len(r.entries) {
		return 0
	}
	return uint64(index) * r.chunkSize
}

func (r *FixedIndexReader) UUID() [16]byte {
	return r.uuid
}

func (r *FixedIndexReader) CTime() int64 {
	return r.ctime
}
