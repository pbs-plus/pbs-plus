package native

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

const CatalogHeaderSize = 4096
const CatalogEntrySize = 40

type CatalogEntry struct {
	EntryStart uint64
	EntryEnd   uint64
	Hash       [8]byte
	NameOffset uint64
	NameLen    uint32
	_          [4]byte
}

type CatalogWriter struct {
	file       *os.File
	uuid       [16]byte
	ctime      int64
	entries    []CatalogEntry
	nameBuffer bytes.Buffer
	entriesMu  sync.Mutex
	finalized  bool
	indexCsum  [32]byte
}

func NewCatalogWriter(path string) (*CatalogWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create catalog: %w", err)
	}

	id := uuid.New()
	now := time.Now().Unix()

	writer := &CatalogWriter{
		file:    f,
		uuid:    id,
		ctime:   now,
		entries: make([]CatalogEntry, 0),
	}

	if err := writer.writeHeader(); err != nil {
		f.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}

	return writer, nil
}

func (w *CatalogWriter) writeHeader() error {
	header := make([]byte, CatalogHeaderSize)
	copy(header[0:8], ProxmoxCatalogFileMagic10[:])
	copy(header[8:24], w.uuid[:])
	binary.LittleEndian.PutUint64(header[24:32], uint64(w.ctime))

	_, err := w.file.WriteAt(header, 0)
	return err
}

func (w *CatalogWriter) AddEntry(entryStart, entryEnd uint64, name string) error {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()

	if w.finalized {
		return fmt.Errorf("catalog already finalized")
	}

	nameBytes := []byte(name)
	hash := HashFilename(nameBytes)
	var hashArr [8]byte
	binary.LittleEndian.PutUint64(hashArr[:], hash)

	nameOffset := uint64(w.nameBuffer.Len())
	w.nameBuffer.Write(nameBytes)

	entry := CatalogEntry{
		EntryStart: entryStart,
		EntryEnd:   entryEnd,
		Hash:       hashArr,
		NameOffset: nameOffset,
		NameLen:    uint32(len(nameBytes)),
	}

	w.entries = append(w.entries, entry)
	return nil
}

func (w *CatalogWriter) Finalize() error {
	w.entriesMu.Lock()
	defer w.entriesMu.Unlock()

	if w.finalized {
		return nil
	}

	nameData := w.nameBuffer.Bytes()

	entryOffset := int64(CatalogHeaderSize)
	entryBuf := make([]byte, CatalogEntrySize)

	for _, entry := range w.entries {
		binary.LittleEndian.PutUint64(entryBuf[0:8], entry.EntryStart)
		binary.LittleEndian.PutUint64(entryBuf[8:16], entry.EntryEnd)
		copy(entryBuf[16:24], entry.Hash[:])
		binary.LittleEndian.PutUint64(entryBuf[24:32], entry.NameOffset)
		binary.LittleEndian.PutUint32(entryBuf[32:36], entry.NameLen)

		if _, err := w.file.WriteAt(entryBuf, entryOffset); err != nil {
			return fmt.Errorf("write entry: %w", err)
		}
		entryOffset += CatalogEntrySize
	}

	nameOffset := entryOffset
	if _, err := w.file.WriteAt(nameData, nameOffset); err != nil {
		return fmt.Errorf("write names: %w", err)
	}

	w.finalized = true
	return nil
}

func (w *CatalogWriter) Close() error {
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

type CatalogReader struct {
	file     *os.File
	uuid     [16]byte
	ctime    int64
	entries  []CatalogEntry
	nameData []byte
}

func OpenCatalog(path string) (*CatalogReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat catalog: %w", err)
	}

	if stat.Size() < CatalogHeaderSize {
		f.Close()
		return nil, fmt.Errorf("catalog too small: %d bytes", stat.Size())
	}

	header := make([]byte, CatalogHeaderSize)
	if _, err := io.ReadFull(io.NewSectionReader(f, 0, CatalogHeaderSize), header); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	var magic [8]byte
	copy(magic[:], header[0:8])
	if magic != ProxmoxCatalogFileMagic10 {
		f.Close()
		return nil, fmt.Errorf("invalid catalog magic: %x", magic)
	}

	r := &CatalogReader{
		file: f,
	}

	copy(r.uuid[:], header[8:24])
	r.ctime = int64(binary.LittleEndian.Uint64(header[24:32]))

	numEntries := int((stat.Size() - CatalogHeaderSize) / CatalogEntrySize)
	if numEntries < 0 {
		numEntries = 0
	}

	entryData := make([]byte, numEntries*CatalogEntrySize)
	if numEntries > 0 {
		n, err := f.ReadAt(entryData, CatalogHeaderSize)
		if err != nil && err != io.EOF {
			f.Close()
			return nil, fmt.Errorf("read entries: %w", err)
		}
		entryData = entryData[:n]
	}

	r.entries = make([]CatalogEntry, 0, numEntries)
	for i := 0; i+CatalogEntrySize <= len(entryData); i += CatalogEntrySize {
		entry := CatalogEntry{}
		entry.EntryStart = binary.LittleEndian.Uint64(entryData[i : i+8])
		entry.EntryEnd = binary.LittleEndian.Uint64(entryData[i+8 : i+16])
		copy(entry.Hash[:], entryData[i+16:i+24])
		entry.NameOffset = binary.LittleEndian.Uint64(entryData[i+24 : i+32])
		entry.NameLen = binary.LittleEndian.Uint32(entryData[i+32 : i+36])
		r.entries = append(r.entries, entry)
	}

	if len(r.entries) > 0 {
		lastEntry := r.entries[len(r.entries)-1]
		nameStart := int64(CatalogHeaderSize + int64(len(r.entries))*CatalogEntrySize)
		nameEnd := int64(lastEntry.NameOffset + uint64(lastEntry.NameLen))
		nameSize := int(nameEnd)

		if nameSize > 0 {
			r.nameData = make([]byte, nameSize)
			if _, err := f.ReadAt(r.nameData, nameStart); err != nil && err != io.EOF {
				f.Close()
				return nil, fmt.Errorf("read names: %w", err)
			}
		}
	}

	return r, nil
}

func (r *CatalogReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

func (r *CatalogReader) EntryCount() int {
	return len(r.entries)
}

func (r *CatalogReader) Entry(i int) *CatalogEntry {
	if i < 0 || i >= len(r.entries) {
		return nil
	}
	return &r.entries[i]
}

func (r *CatalogReader) Name(entry *CatalogEntry) string {
	if entry == nil || r.nameData == nil {
		return ""
	}
	start := entry.NameOffset
	end := start + uint64(entry.NameLen)
	if int(end) > len(r.nameData) {
		return ""
	}
	return string(r.nameData[start:end])
}

func (r *CatalogReader) Lookup(path string) *CatalogEntry {
	nameBytes := []byte(path)
	hash := HashFilename(nameBytes)
	var hashArr [8]byte
	binary.LittleEndian.PutUint64(hashArr[:], hash)

	for i := range r.entries {
		if r.entries[i].Hash == hashArr {
			name := r.Name(&r.entries[i])
			if name == path {
				return &r.entries[i]
			}
		}
	}

	return nil
}

func (r *CatalogReader) UUID() [16]byte {
	return r.uuid
}

func (r *CatalogReader) CTime() int64 {
	return r.ctime
}

type CatalogBuilder struct {
	writer *CatalogWriter
	root   string
}

func NewCatalogBuilder(outputPath, rootPath string) (*CatalogBuilder, error) {
	writer, err := NewCatalogWriter(outputPath)
	if err != nil {
		return nil, err
	}

	return &CatalogBuilder{
		writer: writer,
		root:   rootPath,
	}, nil
}

func (b *CatalogBuilder) AddEntry(entryStart, entryEnd uint64, relativePath string) error {
	return b.writer.AddEntry(entryStart, entryEnd, relativePath)
}

func (b *CatalogBuilder) BuildFromAccessor(accessor *Accessor, basePath string) error {
	root, err := accessor.OpenRoot()
	if err != nil {
		return fmt.Errorf("open root: %w", err)
	}

	return b.buildRecursive(accessor, root, basePath)
}

func (b *CatalogBuilder) buildRecursive(accessor *Accessor, entry *FileEntry, currentPath string) error {
	relativePath := filepath.Join(currentPath, entry.Name())

	if err := b.writer.AddEntry(
		entry.EntryRange().EntryStart,
		entry.EntryRange().EntryEnd,
		relativePath,
	); err != nil {
		return err
	}

	if entry.IsDir() {
		entries, err := accessor.ReadDir(entry.EntryRange().EntryEnd)
		if err != nil {
			return err
		}

		for _, child := range entries {
			if err := b.buildRecursive(accessor, &child, relativePath); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *CatalogBuilder) Close() error {
	return b.writer.Close()
}
