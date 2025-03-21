//go:build linux

package pxar

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fxamacker/cbor/v2"
	native "github.com/pbs-plus/pbs-plus/internal/pxar/native"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/tasks"
	"github.com/puzpuzpuz/xsync/v4"
)

var _ Reader = (*NativePxarReader)(nil)

type NativePxarReader struct {
	accessor      *native.Accessor
	metaReader    *native.BufferedReader
	payloadReader *native.BufferedReader
	task          *tasks.RestoreTask
	closed        atomic.Bool

	FileCount   *xsync.Counter
	FolderCount *xsync.Counter
	TotalBytes  *xsync.Counter

	lastAccessTime  int64
	lastBytesTime   int64
	lastFileCount   int64
	lastFolderCount int64
	lastTotalBytes  int64

	enc cbor.EncMode
	dec cbor.DecMode
}

func NewNativePxarReader(pbsStore, namespace, snapshot string, proxmoxTask *tasks.RestoreTask) (*NativePxarReader, error) {
	dsInfo, err := proxmox.GetDatastoreInfo(pbsStore)
	if err != nil {
		return nil, fmt.Errorf("get datastore: %w", err)
	}

	snapSplit := strings.Split(snapshot, "/")
	if len(snapSplit) != 3 {
		return nil, fmt.Errorf("invalid snapshot string (expected type/id/time): %s", snapshot)
	}

	backupType := snapSplit[0]
	snapshotId := snapSplit[1]
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
		snapshotId,
		snapshotTime,
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("build pxar paths: %w", err)
	}

	if !isSplit {
		return nil, fmt.Errorf("only split archives are supported")
	}

	metaIndex, err := native.OpenDynamicIndex(mpxarPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata index: %w", err)
	}

	payloadIndex, err := native.OpenDynamicIndex(ppxarPath)
	if err != nil {
		metaIndex.Close()
		return nil, fmt.Errorf("open payload index: %w", err)
	}

	var cryptConfig *native.CryptConfig
	chunkStore := native.NewChunkStore(dsInfo.Path, cryptConfig, false)

	metaReader := native.NewBufferedReader(metaIndex, chunkStore)
	payloadReader := native.NewBufferedReader(payloadIndex, chunkStore)

	accessor, err := native.NewAccessor(metaReader, metaIndex.ArchiveSize(), payloadReader, payloadIndex.ArchiveSize())
	if err != nil {
		metaIndex.Close()
		payloadIndex.Close()
		return nil, fmt.Errorf("create accessor: %w", err)
	}

	encMode, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		return nil, fmt.Errorf("create CBOR encoder: %w", err)
	}

	decMode, err := cbor.DecOptions{
		MaxArrayElements: math.MaxInt32,
	}.DecMode()
	if err != nil {
		return nil, fmt.Errorf("create CBOR decoder: %w", err)
	}

	return &NativePxarReader{
		accessor:      accessor,
		metaReader:    metaReader,
		payloadReader: payloadReader,
		task:          proxmoxTask,
		FileCount:     xsync.NewCounter(),
		FolderCount:   xsync.NewCounter(),
		TotalBytes:    xsync.NewCounter(),
		enc:           encMode,
		dec:           decMode,
	}, nil
}

func (r *NativePxarReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	return nil
}

func (r *NativePxarReader) GetRoot(ctx context.Context) (*EntryInfo, error) {
	r.task.WriteString("get root of source")

	root, err := r.accessor.OpenRoot()
	if err != nil {
		return nil, fmt.Errorf("open root: %w", err)
	}

	r.FolderCount.Add(1)
	return fileEntryToEntryInfo(root, r.accessor.MetaSize()), nil
}

func (r *NativePxarReader) LookupByPath(ctx context.Context, path string) (*EntryInfo, error) {
	r.task.WriteString(fmt.Sprintf("looking up path: %s", path))

	entry, err := r.accessor.Lookup(path)
	if err != nil {
		return nil, err
	}

	entry, err = r.accessor.FollowHardlink(entry)
	if err != nil {
		return nil, err
	}

	return fileEntryToEntryInfo(entry, 0), nil
}

func (r *NativePxarReader) ReadDir(ctx context.Context, entryEnd uint64) ([]EntryInfo, error) {
	entries, err := r.accessor.ReadDir(entryEnd)
	if err != nil {
		return nil, err
	}

	result := make([]EntryInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			r.FolderCount.Add(1)
		} else {
			r.FileCount.Add(1)
		}
		result = append(result, *fileEntryToEntryInfo(&entry, 0))
	}

	return result, nil
}

func (r *NativePxarReader) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (*EntryInfo, error) {
	entry, err := r.accessor.OpenFileAtRange(&native.EntryRangeInfo{
		EntryStart: entryStart,
		EntryEnd:   entryEnd,
	})
	if err != nil {
		return nil, err
	}

	entry, err = r.accessor.FollowHardlink(entry)
	if err != nil {
		return nil, err
	}

	if entry.IsDir() {
		r.FolderCount.Add(1)
	} else {
		r.FileCount.Add(1)
	}

	return fileEntryToEntryInfo(entry, 0), nil
}

func (r *NativePxarReader) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	contents := &nativeFileContents{
		reader: r.payloadReader,
		start:  contentStart,
		end:    contentEnd,
	}

	data := make([]byte, size)
	n, err := contents.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		return nil, err
	}

	r.TotalBytes.Add(int64(n))
	return data[:n], nil
}

func (r *NativePxarReader) ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error) {
	entry, err := r.accessor.OpenFileAtRange(&native.EntryRangeInfo{
		EntryStart: entryStart,
		EntryEnd:   entryEnd,
	})
	if err != nil {
		return nil, err
	}

	target, err := entry.GetSymlink()
	if err != nil {
		return nil, err
	}

	return []byte(target), nil
}

func (r *NativePxarReader) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	entry, err := r.accessor.OpenFileAtRange(&native.EntryRangeInfo{
		EntryStart: entryStart,
		EntryEnd:   entryEnd,
	})
	if err != nil {
		return nil, err
	}

	metadata := entry.Metadata()
	if metadata == nil {
		return nil, nil
	}

	result := make(map[string][]byte)
	for _, xattr := range metadata.Xattrs() {
		result[string(xattr.Name())] = xattr.Value()
	}

	if len(metadata.FCaps) > 0 {
		result["security.capability"] = metadata.FCaps
	}

	return result, nil
}

func (r *NativePxarReader) GetStats() PxarReaderStats {
	currentTime := time.Now().UnixNano()

	currentFileCount := r.FileCount.Value()
	currentFolderCount := r.FolderCount.Value()
	totalAccessed := currentFileCount + currentFolderCount

	elapsed := float64(currentTime-r.lastAccessTime) / 1e9
	var accessSpeed float64
	if elapsed > 0 && r.lastAccessTime > 0 {
		accessDelta := (currentFileCount + currentFolderCount) - (r.lastFileCount + r.lastFolderCount)
		accessSpeed = float64(accessDelta) / elapsed
	}

	r.lastAccessTime = currentTime
	r.lastFileCount = currentFileCount
	r.lastFolderCount = currentFolderCount

	currentTotalBytes := r.TotalBytes.Value()
	secDiff := float64(currentTime-r.lastBytesTime) / 1e9
	var bytesSpeed float64
	if secDiff > 0 && r.lastBytesTime > 0 {
		bytesSpeed = float64(currentTotalBytes-r.lastTotalBytes) / secDiff
	}

	r.lastBytesTime = currentTime
	r.lastTotalBytes = currentTotalBytes

	return PxarReaderStats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      uint64(currentTotalBytes),
		ByteReadSpeed:   bytesSpeed,
	}
}

type nativeFileContents struct {
	reader *native.BufferedReader
	start  uint64
	end    uint64
}

func (c *nativeFileContents) ReadAt(p []byte, offset int64) (int, error) {
	size := c.end - c.start
	if uint64(offset) >= size {
		return 0, io.EOF
	}

	readEnd := uint64(offset) + uint64(len(p))
	if readEnd > size {
		readEnd = size
		p = p[:readEnd-uint64(offset)]
	}

	actualOffset := c.start + uint64(offset)
	return c.reader.ReadAt(p, int64(actualOffset))
}

func fileEntryToEntryInfo(entry *native.FileEntry, dirEndHint uint64) *EntryInfo {
	metadata := entry.Metadata()
	var stat native.Stat
	if metadata != nil {
		stat = metadata.Stat
	}

	var fileType FileType
	var contentRange []uint64

	er := entry.EntryRange()

	switch entry.Kind() {
	case native.EntryKindFile:
		fileType = FileTypeFile
		if er.ContentInfo != nil {
			contentRange = []uint64{er.ContentInfo.Start, er.ContentInfo.End}
		}
	case native.EntryKindDirectory:
		fileType = FileTypeDirectory
	case native.EntryKindSymlink:
		fileType = FileTypeSymlink
	case native.EntryKindHardlink:
		fileType = FileTypeHardlink
	case native.EntryKindDevice:
		fileType = FileTypeDevice
	case native.EntryKindFifo:
		fileType = FileTypeFifo
	case native.EntryKindSocket:
		fileType = FileTypeSocket
	}

	return &EntryInfo{
		FileName:        []byte(entry.Name()),
		FileType:        fileType,
		EntryRangeStart: er.EntryStart,
		EntryRangeEnd:   er.EntryEnd,
		ContentRange:    contentRange,
		Mode:            stat.Mode,
		UID:             stat.UID,
		GID:             stat.GID,
		Size:            entry.FileSize(),
		MtimeSecs:       stat.Mtime.Secs,
		MtimeNsecs:      stat.Mtime.Nanos,
	}
}
