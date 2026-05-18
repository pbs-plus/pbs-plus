//go:build linux

package pxar

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

// TaskWriter is the interface for logging task progress.
type TaskWriter interface {
	WriteString(string)
}

// PxarReader wraps a vfs.LocalOffsetFS with PBS-specific bookkeeping
// (task logging). All archive access and stats tracking is delegated to
// the embedded LocalOffsetFS.
type PxarReader struct {
	ofs *vfs.LocalOffsetFS

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
	stats := r.ofs.Stats()
	return PxarReaderStats{
		FilesAccessed:   stats.FilesAccessed,
		FoldersAccessed: stats.FoldersAccessed,
		TotalAccessed:   stats.FilesAccessed + stats.FoldersAccessed,
		TotalBytes:      uint64(stats.TotalBytes),
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
			ofs:  vfs.NewLocalOffsetFS(archiveReader),
			task: task,
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

	if _, err := datastore.ParseDynamicIndex(idxData); err != nil {
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
	return r.ofs.Close()
}

// OffsetFS returns the underlying vfs.LocalOffsetFS for direct use.
func (r *PxarReader) OffsetFS() *vfs.LocalOffsetFS {
	return r.ofs
}

// GetRoot returns the root entry of the archive.
func (r *PxarReader) GetRoot(ctx context.Context) (*pxar.FileInfo, error) {
	if r.task != nil {
		r.task.WriteString("get root of source")
	}
	return r.ofs.Root()
}

// LookupByPath finds an entry by archive-internal path.
func (r *PxarReader) LookupByPath(ctx context.Context, path string) (*pxar.FileInfo, error) {
	if r.task != nil {
		r.task.WriteString(fmt.Sprintf("looking up path: %s", path))
	}
	return r.ofs.Lookup(path)
}

// ReadDir lists the entries in a directory.
func (r *PxarReader) ReadDir(ctx context.Context, dirOffset uint64) ([]pxar.FileInfo, error) {
	return r.ofs.ReadDir(dirOffset)
}

// GetAttr returns attributes for an entry identified by its byte range.
func (r *PxarReader) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (*pxar.FileInfo, error) {
	return r.ofs.GetAttr(entryStart)
}

// Read reads raw file content from the archive.
func (r *PxarReader) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	return r.ofs.Read(contentStart, contentEnd, offset, size)
}

// ReadFileContentReader returns a streaming reader for an entire file.
func (r *PxarReader) ReadFileContentReader(ctx context.Context, contentStart, contentEnd uint64) (io.ReadCloser, error) {
	return r.ofs.ReadContentReader(contentStart, contentEnd)
}

// ReadLink returns the target of a symlink.
func (r *PxarReader) ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error) {
	return r.ofs.ReadLink(entryStart)
}

// ListXAttrs returns extended attributes for an entry.
func (r *PxarReader) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	return r.ofs.ListXAttrs(entryStart)
}
