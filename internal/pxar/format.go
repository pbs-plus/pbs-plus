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
	reader     *transfer.SplitArchiveReader
	store      *datastore.ChunkStore
	source     datastore.ChunkSource
	metaIdx    *datastore.DynamicIndexReader
	payloadIdx *datastore.DynamicIndexReader

	// entryCache maps entry file offsets to pxar.Entry for quick attribute lookback.
	entryCache map[uint64]*pxar.Entry
	cacheMu    sync.RWMutex

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

		metaIdx, err := datastore.ReadDynamicIndex(metaData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse metadata index: %w", err)
		}
		payloadIdx, err := datastore.ReadDynamicIndex(payloadData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse payload index: %w", err)
		}

		archiveReader, err = transfer.NewSplitArchiveReader(metaData, payloadData, chunkSource)
		if err != nil {
			return nil, fmt.Errorf("failed to create split archive reader: %w", err)
		}

		pr := &PxarReader{
			reader:      archiveReader,
			store:       store,
			source:      chunkSource,
			metaIdx:     metaIdx,
			payloadIdx:  payloadIdx,
			entryCache:  make(map[uint64]*pxar.Entry),
			FileCount:   xsync.NewCounter(),
			FolderCount: xsync.NewCounter(),
			TotalBytes:  xsync.NewCounter(),
			task:        task,
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

	metaIdx, err := datastore.ReadDynamicIndex(idxData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	chunkedReader, err := transfer.NewChunkedArchiveReader(idxData, chunkSource)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunked archive reader: %w", err)
	}

	// Wrap chunked reader as a split archive reader via the accessor
	// Use the inner FileArchiveReader directly
	pr := &PxarReader{
		store:       store,
		source:      chunkSource,
		metaIdx:     metaIdx,
		entryCache:  make(map[uint64]*pxar.Entry),
		FileCount:   xsync.NewCounter(),
		FolderCount: xsync.NewCounter(),
		TotalBytes:  xsync.NewCounter(),
		task:        task,
	}
	_ = chunkedReader
	_ = pr

	syslog.L.Info().
		WithMessage("pxar: native Go reader created (non-split)").
		WithField("datastore", pbsStore).
		Write()

	return pr, nil
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
	r.cacheMu.Unlock()
}

// getCachedEntry retrieves an entry from the lookup cache by offset.
func (r *PxarReader) getCachedEntry(offset uint64) *pxar.Entry {
	r.cacheMu.RLock()
	e := r.entryCache[offset]
	r.cacheMu.RUnlock()
	return e
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

// ReadDir lists the entries in a directory. entryEnd is the directory content offset.
func (r *PxarReader) ReadDir(ctx context.Context, entryEnd uint64) ([]EntryInfo, error) {
	var entries []EntryInfo
	err := r.reader.ListDirectory(int64(entryEnd), accessor.ListOption{}, func(e *pxar.Entry) error {
		info := entryToEntryInfo(e)
		entries = append(entries, *info)
		r.cacheEntry(e)

		if e.IsDir() {
			r.FolderCount.Add(1)
			if r.task != nil {
				r.task.WriteString(fmt.Sprintf("restoring entries of dir: %s", e.FileName()))
			}
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
// entryStart is the file offset, entryEnd is not used.
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

	// Read from the accessor using the offset
	entry, err := r.reader.Lookup(fmt.Sprintf("@%d", entryStart))
	if err == nil {
		r.cacheEntry(entry)
		return entryToEntryInfo(entry), nil
	}

	return nil, fmt.Errorf("entry at offset %d not found: %w", entryStart, err)
}

// Read reads raw file content from the archive.
func (r *PxarReader) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	// Try to find the entry by content offset
	// contentStart is the ContentOffset of the file entry
	r.cacheMu.RLock()
	var targetEntry *pxar.Entry
	for _, e := range r.entryCache {
		if e.IsRegularFile() && e.ContentOffset == contentStart {
			targetEntry = e
			break
		}
	}
	r.cacheMu.RUnlock()

	if targetEntry == nil {
		return nil, fmt.Errorf("entry with content offset %d not found in cache", contentStart)
	}

	rc, err := r.reader.ReadFileContentReader(targetEntry)
	if err != nil {
		return nil, fmt.Errorf("open content reader: %w", err)
	}
	defer rc.Close()

	if offset > 0 {
		// Seek by reading and discarding
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
func (r *PxarReader) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	if e := r.getCachedEntry(entryStart); e != nil {
		xattrs := make(map[string][]byte)
		for _, xa := range e.Metadata.XAttrs {
			xattrs[string(xa.Name())] = xa.Value()
		}
		if e.Metadata.FCaps != nil {
			xattrs["security.capability"] = e.Metadata.FCaps
		}
		return xattrs, nil
	}

	return nil, fmt.Errorf("entry at offset %d not found", entryStart)
}

// entryToEntryInfo converts a pxar.Entry to our EntryInfo format.
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

	xattrs := make(map[string][]byte)
	for _, xa := range e.Metadata.XAttrs {
		xattrs[string(xa.Name())] = xa.Value()
	}

	info := &EntryInfo{
		FileName:        []byte(e.FileName()),
		FileType:        ft,
		EntryRangeStart: e.FileOffset,
		EntryRangeEnd:   e.FileOffset + e.FileSize,
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
