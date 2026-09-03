package pxarmount

import (
	"fmt"
	"io"
	"sort"

	"github.com/zeebo/xxh3"

	"github.com/pbs-plus/pbs-plus/internal/log"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/datastore"
)

func (ow *commitWalkState) addToPendingRefs(e *commitEntry) error {
	if e.pxarSlim != nil {
		if e.pxarSlim.isReg {
			if e.sortKey == 0 {
				e.sortKey = e.pxarSlim.payloadOffset
			}
		} else {
			e.sortKey = e.pxarSlim.entryStart
		}
	} else {
		if e.node != nil && e.node.RedirectTo != "" {
			if pxEntry, err := ow.resolvePxarEntryCached(e.node.RedirectTo); err == nil {
				e.sortKey = pxEntry.PayloadOffset
				e.cachedEntry = pxEntry
			} else {
				e.sortKey = 0
			}
		}
	}

	if ow.origChunkIndex != nil && ow.batchRangeEnd != 0 && e.sortKey > ow.batchRangeEnd {
		if err := ow.flushPendingRefs(true); err != nil {
			return err
		}
	}

	ow.pendingRefs = append(ow.pendingRefs, *e)

	entryEnd := e.rangeEnd()
	if entryEnd > ow.batchRangeEnd {
		ow.batchRangeEnd = entryEnd
	}

	if len(ow.pendingRefs) >= maxPendingRefs {
		return ow.flushPendingRefs(true)
	}
	return nil
}

func insertionSortPendingRefs(s []commitEntry) {
	n := len(s)
	if n <= 1 {
		return
	}
	inv := 0
	threshold := max(n/4, 1)
	for i := 1; i < n; i++ {
		if s[i].sortKey < s[i-1].sortKey {
			inv++
			if inv >= threshold {
				sort.Slice(s, func(i, j int) bool {
					return s[i].sortKey < s[j].sortKey
				})
				return
			}
		}
	}
	for i := 1; i < n; i++ {
		key := s[i]
		j := i - 1
		for j >= 0 && s[j].sortKey > key.sortKey {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = key
	}
}

func pendingRefsRange(refs []commitEntry) (start, end uint64) {
	if len(refs) == 0 {
		return 0, 0
	}
	start = refs[0].sortKey
	end = refs[0].rangeEnd()
	for i := 1; i < len(refs); i++ {
		re := refs[i].rangeEnd()
		if re > end {
			end = re
		}
	}
	return start, end
}

// flushPendingRefs keeps a final chunk only when no payload write can intervene.
func (ow *commitWalkState) flushPendingRefs(keepLastChunk bool) error {
	if len(ow.pendingRefs) == 0 {
		if keepLastChunk || ow.reusePlanner == nil {
			return nil
		}
		return ow.injectChunks(ow.reusePlanner.FlushRange())
	}
	defer func() {
		ow.pendingRefs = ow.pendingRefs[:0]
		ow.batchRangeEnd = 0
	}()

	previousFiles := ow.unchangedFiles
	ow.unchangedFiles += int64(len(ow.pendingRefs))
	if ow.prog != nil && (previousFiles == 0 || previousFiles/4096 != ow.unchangedFiles/4096) {
		ow.prog.SetMsg(fmt.Sprintf("Processing unchanged files (%d scanned)", ow.unchangedFiles))
	}

	insertionSortPendingRefs(ow.pendingRefs)
	if ow.reusePlanner == nil && ow.origChunkIndex != nil {
		ow.reusePlanner = datastore.NewChunkReusePlanner(ow.origChunkIndex)
	}
	if ow.reusePlanner == nil {
		return ow.reencodeAll()
	}

	rangeStart, rangeEnd := pendingRefsRange(ow.pendingRefs)
	plan := ow.reusePlanner.PlanRange(rangeStart, rangeEnd, keepLastChunk)
	if !plan.Reusable {
		if err := ow.injectChunks(plan); err != nil {
			return err
		}
		return ow.reencodeAll()
	}

	baseOffset := ow.writer.Encoder().PayloadPosition() + plan.PrefixSize + plan.StartPadding
	deferred, err := ow.encodeRefs(baseOffset)
	if err != nil {
		return err
	}
	if err := ow.injectChunks(plan); err != nil {
		return err
	}
	if len(deferred) > 0 {
		if err := ow.injectChunks(ow.reusePlanner.FlushRange()); err != nil {
			return err
		}
	}
	return ow.reencodeAt(deferred)
}

// encodeRefs writes no payload bytes; it returns non-monotonic entries to defer.
func (ow *commitWalkState) encodeRefs(baseOffset uint64) ([]int, error) {
	var deferred []int
	batchStart := ow.pendingRefs[0].sortKey

	for i := range ow.pendingRefs {
		e := &ow.pendingRefs[i]

		refOff := e.sortKey
		if baseOffset != 0 {
			refOff = baseOffset + (e.sortKey - batchStart)
		}

		if ow.hasPrevRef && refOff <= ow.prevRefOffset {
			ow.mfs.debugf("ref %q offset=%d <= prevRef=%d, deferring re-encode", e.name, refOff, ow.prevRefOffset)
			deferred = append(deferred, i)
			continue
		}

		var err error
		if e.node != nil {
			err = ow.emitJournalRefAt(e, refOff)
		} else {
			err = ow.emitPxarRefAt(e, refOff)
		}
		if err != nil {
			return nil, err
		}
	}
	return deferred, nil
}

func (ow *commitWalkState) reencodeAll() error {
	for i := range ow.pendingRefs {
		if err := ow.reencodeOne(&ow.pendingRefs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (ow *commitWalkState) reencodeAt(idxs []int) error {
	for _, i := range idxs {
		if err := ow.reencodeOne(&ow.pendingRefs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (ow *commitWalkState) reencodeOne(e *commitEntry) error {
	if e.node != nil {
		return ow.emitJournalReencode(e)
	}
	return ow.emitPxarReencode(e)
}

const injectBatchSize = 128

func (ow *commitWalkState) injectChunks(plan datastore.ChunkReuseRange) error {
	for offset := 0; offset < plan.ChunkCount(); offset += injectBatchSize {
		count := min(injectBatchSize, plan.ChunkCount()-offset)
		refs := make([]backupproxy.KnownChunkRef, count)
		for i := range count {
			chunk, ok := plan.Chunk(offset + i)
			if !ok {
				return fmt.Errorf("missing reused chunk %d", offset+i)
			}
			refs[i] = backupproxy.KnownChunkRef{
				Digest: chunk.Digest,
				Size:   chunk.End - chunk.Start,
			}
		}
		if err := ow.writer.InjectChunks(refs); err != nil {
			return err
		}
	}
	return nil
}

func (ow *commitWalkState) emitJournalRefAt(e *commitEntry, refOffset uint64) error {
	node := e.node
	xattrs := ow.ensureXAttrs(node.ID)
	meta := nodeToMetadata(node, xattrs)

	pxarEntry, err := ow.resolvePxarEntryCached(node.RedirectTo)
	if err != nil {
		return fmt.Errorf("resolve redirect %q for %q: %w", node.RedirectTo, e.name, err)
	}
	mergedMeta := mergeMetaWithPxar(meta, pxarEntry)

	entry := ow.allocEntry()
	entry.Path = e.name
	entry.Kind = pxar.KindFile
	entry.Metadata = mergedMeta
	entry.FileSize = node.Size
	if entry.FileSize == 0 {
		entry.FileSize = pxarEntry.FileSize
	}

	return ow.writeRef(entry, e.name, refOffset)
}

func (ow *commitWalkState) emitPxarRefAt(e *commitEntry, refOffset uint64) error {
	slim := e.pxarSlim
	if slim == nil {
		return nil
	}

	pxarEntry := e.cachedEntry
	if pxarEntry == nil {
		var err error
		pxarEntry, err = ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
		if err != nil {
			return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
		}
	}

	clone := ow.clonePxarEntryBuf(pxarEntry, e.name)
	return ow.writeRef(clone, e.name, refOffset)
}

func (ow *commitWalkState) emitJournalReencode(e *commitEntry) error {
	node := e.node
	xattrs := ow.ensureXAttrs(node.ID)
	meta := nodeToMetadata(node, xattrs)

	pxarEntry, err := ow.resolvePxarEntryCached(node.RedirectTo)
	if err != nil {
		return fmt.Errorf("resolve redirect %q for re-encode %q: %w", node.RedirectTo, e.name, err)
	}
	mergedMeta := mergeMetaWithPxar(meta, pxarEntry)

	entry := ow.allocEntry()
	entry.Path = e.name
	entry.Kind = pxar.KindFile
	entry.Metadata = mergedMeta
	entry.FileSize = node.Size
	if entry.FileSize == 0 {
		entry.FileSize = pxarEntry.FileSize
	}

	return ow.writeReencoded(pxarEntry, entry, e.name)
}

func (ow *commitWalkState) emitPxarReencode(e *commitEntry) error {
	slim := e.pxarSlim
	if slim == nil {
		return nil
	}

	pxarEntry := e.cachedEntry
	if pxarEntry == nil {
		var err error
		pxarEntry, err = ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
		if err != nil {
			return fmt.Errorf("read pxar entry at %d for re-encode: %w", slim.entryStart, err)
		}
	}

	clone := ow.clonePxarEntryBuf(pxarEntry, e.name)
	return ow.writeReencoded(pxarEntry, clone, e.name)
}

func (ow *commitWalkState) writeBackedFile(name, childPath string, meta pxar.Metadata) error {
	f, fileSize, err := ow.mfs.openBackedFile(childPath, nil)
	if err != nil {
		return fmt.Errorf("open backed file %q: %w", childPath, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	entry := ow.allocEntry()
	entry.Path = name
	entry.Kind = pxar.KindFile
	entry.Metadata = meta
	entry.FileSize = uint64(fileSize)

	h := xxh3.New()
	tee := io.TeeReader(f, h)

	if ow.prog != nil {
		ow.prog.SetMsg(childPath)
	}

	if err := ow.writer.WriteEntryReader(entry, tee, uint64(fileSize)); err != nil {
		return fmt.Errorf("write backed file %q: %w", name, err)
	}

	ow.backedHashes[childPath] = h.Sum64()
	ow.mutableFiles++

	if ow.prog != nil {
		ow.prog.AddFile(fileSize)
	}
	return nil
}
