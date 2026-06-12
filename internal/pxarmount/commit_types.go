package pxarmount

import (
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"

	pxar "github.com/pbs-plus/pxar"
)

const maxPendingRefs = 512

const chunkPaddingThreshold = 0.1

type commitEntry struct {
	name        string
	node        *GraphNode
	pxarSlim    *dirEntrySlim
	sortKey     uint64
	cachedEntry *pxar.Entry
}

func (e *commitEntry) rangeEnd() uint64 {
	if e.cachedEntry != nil {
		return e.sortKey + e.cachedEntry.FileSize + format.HeaderSize
	}
	if e.pxarSlim != nil {
		return e.sortKey + e.pxarSlim.fileSize + format.HeaderSize
	}
	return e.sortKey
}

type deferredDir struct {
	name       string
	node       *GraphNode
	entryStart uint64
	pxarIno    uint64
}

type reusableChunk struct {
	size      uint64
	padding   uint64
	digest    [32]byte
	endOffset uint64
}

func (c *reusableChunk) sameIndexedChunkAs(other *reusableChunk) bool {
	return c.digest == other.digest && c.endOffset == other.endOffset
}

type commitWalkState struct {
	mfs          *MutableFS
	writer       transfer.ArchiveWriter
	prog         CommitProgress
	xattrCache   map[int64][]format.XAttr
	backedHashes map[string]uint64
	mutableFiles int

	redirectCache map[string]*pxar.Entry
	prevRefOffset uint64
	hasPrevRef    bool

	pendingRefs    []commitEntry
	entryCache     map[uint64]*pxar.Entry
	origChunkIndex *datastore.DynamicIndexReader

	batchRangeEnd uint64
	savedChunk    reusableChunk
	hasSavedChunk bool

	entryBuf pxar.Entry
}

func (ow *commitWalkState) ensureXAttrs(nodeID int64) []format.XAttr {
	if xattrs, ok := ow.xattrCache[nodeID]; ok {
		return xattrs
	}
	if ow.mfs.journal == nil {
		return nil
	}
	xattrs, _ := ow.mfs.journal.XAttrsForNode(nodeID)
	ow.xattrCache[nodeID] = xattrs
	return xattrs
}

func (ow *commitWalkState) allocEntry() *pxar.Entry {
	ow.entryBuf = pxar.Entry{}
	return &ow.entryBuf
}

func (ow *commitWalkState) buildPath(parent, name string) string {
	return joinPath(parent, name)
}

func (ow *commitWalkState) clonePxarEntryBuf(e *pxar.Entry, name string) *pxar.Entry {
	ow.entryBuf = *e
	ow.entryBuf.Path = name
	return &ow.entryBuf
}
