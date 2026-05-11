//go:build linux

package pxar

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/puzpuzpuz/xsync/v4"
)

// TaskWriter is the interface for logging task progress.
type TaskWriter interface {
	WriteString(string)
}

// PxarReader provides random access to a pxar archive backed by a PBS datastore
// using the Go pxar library (datastore + accessor + transfer).
type PxarReader struct {
	reader *transfer.SplitArchiveReader

	// entryCache maps entry file offsets to pxar.Entry for quick attribute lookback.
	entryCache    map[uint64]*pxar.Entry
	contentCache  map[uint64]*pxar.Entry // maps ContentOffset -> Entry for Read
	rangeToOffset map[uint64]uint64      // maps EntryRangeEnd -> ContentOffset for ReadDir
	cacheMu       sync.RWMutex

	FileCount   *xsync.Counter
	FolderCount *xsync.Counter
	TotalBytes  *xsync.Counter

	task   TaskWriter
	closed bool
}

// PxarReaderStats holds read performance statistics.
type PxarReaderStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}

// GetStats returns the current reader statistics.
func (r *PxarReader) GetStats() PxarReaderStats {
	return PxarReaderStats{
		FilesAccessed:   r.FileCount.Value(),
		FoldersAccessed: r.FolderCount.Value(),
		TotalAccessed:   r.FileCount.Value() + r.FolderCount.Value(),
		TotalBytes:      uint64(r.TotalBytes.Value()),
	}
}

// NewPxarReader creates a PxarReader for the given snapshot using the Go pxar library.
func NewPxarReader(_ context.Context, _, pbsStore, namespace, snapshot string, task TaskWriter) (*PxarReader, error) {
	dsInfo, err := proxmox.GetDatastoreInfo(pbsStore)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastore: %w", err)
	}

	snapSplit := strings.Split(snapshot, "/")
	if len(snapSplit) != 3 {
		return nil, fmt.Errorf("invalid snapshot string (expected type/id/time): %s", snapshot)
	}

	backupType := snapSplit[0]
	snapshotID := snapSplit[1]
	timestampRaw := snapSplit[2]

	unixTime, err := strconv.ParseInt(timestampRaw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid unix timestamp in snapshot: %w", err)
	}

	t := time.Unix(unixTime, 0).UTC()
	snapshotTime := t.Format(time.RFC3339)

	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(
		dsInfo.Path,
		namespace,
		backupType,
		snapshotID,
		snapshotTime,
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build pxar paths: %w", err)
	}

	// Open the chunk store
	store, err := datastore.NewChunkStore(dsInfo.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk store: %w", err)
	}

	chunkSource := datastore.NewChunkStoreSource(store)

	var archiveReader *transfer.SplitArchiveReader

	if isSplit {
		metaData, err := os.ReadFile(mpxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata index %s: %w", mpxarPath, err)
		}
		payloadData, err := os.ReadFile(ppxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload index %s: %w", ppxarPath, err)
		}

		archiveReader, err = transfer.NewSplitArchiveReader(metaData, payloadData, chunkSource)
		if err != nil {
			return nil, fmt.Errorf("failed to create split archive reader: %w", err)
		}

		pr := &PxarReader{
			reader:        archiveReader,
			entryCache:    make(map[uint64]*pxar.Entry),
			contentCache:  make(map[uint64]*pxar.Entry),
			rangeToOffset: make(map[uint64]uint64),
			FileCount:     xsync.NewCounter(),
			FolderCount:   xsync.NewCounter(),
			TotalBytes:    xsync.NewCounter(),
			task:          task,
		}

		syslog.L.Info().
			WithMessage("pxar: native Go reader created").
			WithField("datastore", pbsStore).
			WithField("split", true).
			Write()

		return pr, nil
	}

	// Non-split (.pxar.didx) — read as a chunked archive
	idxData, err := os.ReadFile(mpxarPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index %s: %w", mpxarPath, err)
	}

	if _, err := datastore.ReadDynamicIndex(idxData); err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	if _, err := transfer.NewChunkedArchiveReader(idxData, chunkSource); err != nil {
		return nil, fmt.Errorf("failed to create chunked archive reader: %w", err)
	}

	return nil, fmt.Errorf(".pxar.didx found, only split archives are supported for now")
}

// Close releases all resources held by the reader.
func (r *PxarReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}

// cacheEntry stores an entry in the lookup cache.
func (r *PxarReader) cacheEntry(e *pxar.Entry) {
	r.cacheMu.Lock()
	r.entryCache[e.FileOffset] = e
	if e.IsRegularFile() && e.ContentOffset > 0 {
		r.contentCache[e.ContentOffset] = e
	}
	if e.IsDir() && e.ContentOffset > 0 {
		r.rangeToOffset[e.FileOffset+e.FileSize] = e.ContentOffset
	}
	r.cacheMu.Unlock()
}

// getCachedEntry retrieves an entry from the lookup cache by offset.
func (r *PxarReader) getCachedEntry(offset uint64) *pxar.Entry {
	r.cacheMu.RLock()
	e := r.entryCache[offset]
	r.cacheMu.RUnlock()
	return e
}

// getCachedContentEntry retrieves a file entry by content offset.
func (r *PxarReader) getCachedContentEntry(contentOffset uint64) *pxar.Entry {
	r.cacheMu.RLock()
	e := r.contentCache[contentOffset]
	r.cacheMu.RUnlock()
	return e
}

// resolveContentOffset converts a ReadDir parameter (which may be
// ContentOffset or legacy EntryRangeEnd) to ContentOffset by checking
// the cache.
func (r *PxarReader) resolveContentOffset(offset uint64) uint64 {
	r.cacheMu.RLock()
	co, ok := r.rangeToOffset[offset]
	r.cacheMu.RUnlock()
	if ok {
		return co
	}
	// Not found in cache — assume it's already a ContentOffset
	return offset
}

// GetRoot returns the root entry of the archive.
func (r *PxarReader) GetRoot(ctx context.Context) (*EntryInfo, error) {
	if r.task != nil {
		r.task.WriteString("get root of source")
	}

	entry, err := r.reader.ReadRoot()
	if err != nil {
		return nil, err
	}

	r.cacheEntry(entry)
	return entryToEntryInfo(entry), nil
}

// LookupByPath finds an entry by archive-internal path.
func (r *PxarReader) LookupByPath(ctx context.Context, path string) (*EntryInfo, error) {
	if r.task != nil {
		r.task.WriteString(fmt.Sprintf("looking up path: %s", path))
	}

	entry, err := r.reader.Lookup(path)
	if err != nil {
		return nil, err
	}

	r.cacheEntry(entry)
	return entryToEntryInfo(entry), nil
}

// ReadDir lists the entries in a directory.
// Accepts dirOffset (ContentOffset) or legacy EntryRangeEnd —
// resolves to ContentOffset via the content cache.
func (r *PxarReader) ReadDir(ctx context.Context, dirOffset uint64) ([]EntryInfo, error) {
	// Try to resolve to ContentOffset: dirOffset might be EntryRangeEnd
	// (legacy agent) or ContentOffset (new code). Look up in cache.
	offset := r.resolveContentOffset(dirOffset)

	entries := make([]EntryInfo, 0, 64)
	err := r.reader.ListDirectory(int64(offset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		info := entryToEntryInfo(e)
		entries = append(entries, *info)
		r.cacheEntry(e)

		if e.IsDir() {
			r.FolderCount.Add(1)
		} else {
			r.FileCount.Add(1)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// GetAttr returns attributes for an entry identified by its byte range.
// entryStart is the file offset (pxar.Entry.FileOffset), entryEnd is unused.
func (r *PxarReader) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (*EntryInfo, error) {
	// Try cache first
	if e := r.getCachedEntry(entryStart); e != nil {
		if e.IsDir() {
			r.FolderCount.Add(1)
		} else {
			r.FileCount.Add(1)
		}
		return entryToEntryInfo(e), nil
	}

	// Read entry by archive byte offset using the accessor
	entry, err := r.reader.ReadEntryAt(int64(entryStart))
	if err != nil {
		return nil, fmt.Errorf("entry at offset %d: %w", entryStart, err)
	}
	r.cacheEntry(entry)
	return entryToEntryInfo(entry), nil
}

// Read reads raw file content from the archive.
func (r *PxarReader) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	targetEntry := r.getCachedContentEntry(contentStart)
	if targetEntry == nil {
		return nil, fmt.Errorf("entry with content offset %d not found in cache", contentStart)
	}

	rc, err := r.reader.ReadFileContentReader(targetEntry)
	if err != nil {
		return nil, fmt.Errorf("open content reader: %w", err)
	}
	defer rc.Close()

	if offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, int64(offset)); err != nil {
			return nil, fmt.Errorf("seek to offset: %w", err)
		}
	}

	buf := make([]byte, size)
	n, err := io.ReadFull(rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, fmt.Errorf("read content: %w", err)
	}

	r.TotalBytes.Add(int64(n))
	return buf[:n], nil
}

// ReadFileContentReader returns a streaming reader for an entire file,
// looked up by content offset. The caller must close the reader.
func (r *PxarReader) ReadFileContentReader(ctx context.Context, contentStart, contentEnd uint64) (io.ReadCloser, error) {
	targetEntry := r.getCachedContentEntry(contentStart)
	if targetEntry == nil {
		return nil, fmt.Errorf("entry with content offset %d not found in cache", contentStart)
	}
	return r.reader.ReadFileContentReader(targetEntry)
}

// ReadLink returns the target of a symlink identified by its byte range.
func (r *PxarReader) ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error) {
	// Try cache first
	if e := r.getCachedEntry(entryStart); e != nil {
		if e.LinkTarget != "" {
			return []byte(e.LinkTarget), nil
		}
	}

	return nil, fmt.Errorf("symlink entry at offset %d not found or has no target", entryStart)
}

// ListXAttrs returns extended attributes for an entry.
// If the cached entry was loaded with Minimal decoding, falls back to
// ReadEntryAt for full metadata.
func (r *PxarReader) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	e := r.getCachedEntry(entryStart)
	if e == nil {
		return nil, fmt.Errorf("entry at offset %d not found", entryStart)
	}

	// If the cached entry has no xattrs but might have them (loaded with Minimal),
	// re-read the full entry.
	if len(e.Metadata.XAttrs) == 0 && e.Metadata.FCaps == nil {
		full, err := r.reader.ReadEntryAt(int64(entryStart))
		if err != nil {
			return nil, nil // entry has genuinely no xattrs
		}
		e = full
		r.cacheEntry(e)
	}

	nx := len(e.Metadata.XAttrs)
	if nx == 0 && e.Metadata.FCaps == nil {
		return nil, nil
	}
	xattrs := make(map[string][]byte, nx+1)
	for _, xa := range e.Metadata.XAttrs {
		xattrs[string(xa.Name())] = xa.Value()
	}
	if e.Metadata.FCaps != nil {
		xattrs["security.capability"] = e.Metadata.FCaps
	}
	return xattrs, nil
}

// entryToEntryInfo converts a pxar.Entry to our EntryInfo format.
// The returned EntryInfo must be copied if the caller needs to retain it
// beyond the lifetime of the source entry.
func entryToEntryInfo(e *pxar.Entry) *EntryInfo {
	var ft FileType
	switch e.Kind {
	case pxar.KindDirectory:
		ft = FileTypeDirectory
	case pxar.KindSymlink:
		ft = FileTypeSymlink
	case pxar.KindHardlink:
		ft = FileTypeHardlink
	case pxar.KindDevice:
		ft = FileTypeDevice
	case pxar.KindFifo:
		ft = FileTypeFifo
	case pxar.KindSocket:
		ft = FileTypeSocket
	default:
		ft = FileTypeFile
	}

	var xattrs map[string][]byte
	if nx := len(e.Metadata.XAttrs); nx > 0 || e.Metadata.FCaps != nil {
		xattrs = make(map[string][]byte, nx+1) // +1 for fcaps
		for _, xa := range e.Metadata.XAttrs {
			xattrs[string(xa.Name())] = xa.Value()
		}
		if e.Metadata.FCaps != nil {
			xattrs["security.capability"] = e.Metadata.FCaps
		}
	}

	info := &EntryInfo{
		FileName:        []byte(e.FileName()),
		FileType:        ft,
		EntryRangeStart: e.FileOffset,
		EntryRangeEnd:   e.FileOffset + e.FileSize,
		ContentOffset:   e.ContentOffset,
		Mode:            e.Metadata.Stat.Mode,
		UID:             e.Metadata.Stat.UID,
		GID:             e.Metadata.Stat.GID,
		Size:            e.FileSize,
		MtimeSecs:       e.Metadata.Stat.Mtime.Secs,
		MtimeNsecs:      e.Metadata.Stat.Mtime.Nanos,
		LinkTarget:      e.LinkTarget,
		Xattrs:          xattrs,
	}

	if e.IsRegularFile() && e.ContentOffset > 0 {
		info.ContentRange = []uint64{
			e.ContentOffset,
			e.ContentOffset + e.FileSize,
		}
	}

	return info
}
