package pxarmount

import (
	"fmt"
	"io"
	"os"
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

func lookupDynamicEntries(idx *datastore.DynamicIndexReader, rangeStart, rangeEnd uint64) ([]reusableChunk, uint64, uint64) {
	if idx == nil || idx.Count() == 0 || rangeStart >= rangeEnd {
		return nil, 0, 0
	}

	startIdx, ok := idx.ChunkFromOffset(rangeStart)
	if !ok {
		return nil, 0, 0
	}

	var prevEnd uint64
	if startIdx > 0 {
		info, _ := idx.ChunkInfo(startIdx - 1)
		prevEnd = info.End
	}
	startPadding := rangeStart - prevEnd

	var endPadding uint64
	var chunks []reusableChunk

	for i := startIdx; i < idx.Count(); i++ {
		info, ok := idx.ChunkInfo(i)
		if !ok {
			break
		}

		chunk := reusableChunk{
			size:      info.End - prevEnd,
			digest:    info.Digest,
			endOffset: info.End,
		}
		prevEnd = info.End

		if rangeEnd < info.End {
			endPadding = info.End - rangeEnd
		}
		chunks = append(chunks, chunk)

		if rangeEnd < info.End {
			break
		}
	}

	if len(chunks) > 0 {
		chunks[0].padding += startPadding
	}
	if len(chunks) > 0 {
		chunks[len(chunks)-1].padding += endPadding
	}

	return chunks, startPadding, endPadding
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

func (ow *commitWalkState) shouldReuse(refs []commitEntry) bool {
	if ow.origChunkIndex == nil || len(refs) == 0 {
		return true
	}

	rangeStart, rangeEnd := pendingRefsRange(refs)
	if rangeEnd <= rangeStart {
		return true
	}

	chunks, startPadding, endPadding := lookupDynamicEntries(ow.origChunkIndex, rangeStart, rangeEnd)
	if len(chunks) == 0 {
		return true
	}

	padding := startPadding + endPadding
	if ow.hasSavedChunk && chunks[0].sameIndexedChunkAs(&ow.savedChunk) {
		used := ow.savedChunk.size - ow.savedChunk.padding
		if used > padding {
			padding = 0
		} else {
			padding -= used
		}
	}

	totalSize := (rangeEnd - rangeStart) + padding
	if totalSize == 0 {
		return true
	}

	return float64(padding)/float64(totalSize) <= chunkPaddingThreshold
}

func (ow *commitWalkState) flushPendingRefs(keepLastChunk bool) error {
	if len(ow.pendingRefs) == 0 {
		return nil
	}

	insertionSortPendingRefs(ow.pendingRefs)

	if ow.origChunkIndex == nil || len(ow.pendingRefs) == 0 {
		return ow.encodeEntries(0, true)
	}

	rangeStart, rangeEnd := pendingRefsRange(ow.pendingRefs)

	if rangeEnd <= rangeStart {
		if ow.hasSavedChunk {
			if err := ow.injectChunk(ow.savedChunk); err != nil {
				return err
			}
			ow.hasSavedChunk = false
		}
		return ow.encodeEntries(0, false)
	}

	prevLast := ow.savedChunk
	hasPrev := ow.hasSavedChunk
	ow.hasSavedChunk = false

	indices, startPadding, endPadding := lookupDynamicEntries(ow.origChunkIndex, rangeStart, rangeEnd)
	if len(indices) == 0 {
		if hasPrev {
			if err := ow.injectChunk(prevLast); err != nil {
				return err
			}
		}
		return ow.encodeEntries(0, false)
	}

	padding := startPadding + endPadding
	totalSize := (rangeEnd - rangeStart) + padding

	if hasPrev && indices[0].sameIndexedChunkAs(&prevLast) {
		used := prevLast.size - prevLast.padding
		if used > padding {
			padding = 0
		} else {
			padding -= used
		}
	}

	if totalSize == 0 {
		if hasPrev {
			if err := ow.injectChunk(prevLast); err != nil {
				return err
			}
		}
		return ow.encodeEntries(0, false)
	}

	ratio := float64(padding) / float64(totalSize)

	if ratio > chunkPaddingThreshold {
		if hasPrev {
			if err := ow.injectChunk(prevLast); err != nil {
				return err
			}
		}
		return ow.encodeEntries(0, false)
	}

	if hasPrev {
		if !prevLast.sameIndexedChunkAs(&indices[0]) {
			if err := ow.injectChunk(prevLast); err != nil {
				return err
			}
		} else {
			used := prevLast.size - prevLast.padding
			indices[0].padding -= used
		}
	}

	baseOffset := ow.writer.Encoder().PayloadPosition() + startPadding

	if err := ow.encodeEntries(baseOffset, true); err != nil {
		return err
	}

	if keepLastChunk && len(indices) > 0 {
		ow.savedChunk = indices[len(indices)-1]
		ow.hasSavedChunk = true
		indices = indices[:len(indices)-1]
	}

	return ow.injectChunks(indices)
}

func (ow *commitWalkState) encodeEntries(baseOffset uint64, reuse bool) error {
	for i := range ow.pendingRefs {
		e := &ow.pendingRefs[i]
		var err error
		if reuse {
			var refOff uint64
			if baseOffset != 0 {
				refOff = baseOffset + (e.sortKey - ow.pendingRefs[0].sortKey)
			} else {
				refOff = e.sortKey
			}
			if e.node != nil {
				err = ow.emitJournalRefAt(e, refOff)
			} else {
				err = ow.emitPxarRefAt(e, refOff)
			}
		} else {
			if e.node != nil {
				err = ow.emitJournalReencode(e)
			} else {
				err = ow.emitPxarReencode(e)
			}
		}
		if err != nil {
			return err
		}
	}

	ow.pendingRefs = ow.pendingRefs[:0]
	ow.batchRangeEnd = 0
	return nil
}

const injectBatchSize = 128

func (ow *commitWalkState) injectChunk(c reusableChunk) error {
	return ow.writer.InjectChunks([]backupproxy.KnownChunkRef{{
		Digest: c.digest,
		Size:   c.size,
	}})
}

func (ow *commitWalkState) injectChunks(chunks []reusableChunk) error {
	for len(chunks) > 0 {
		batch := chunks
		if len(batch) > injectBatchSize {
			batch = batch[:injectBatchSize]
		}
		refs := make([]backupproxy.KnownChunkRef, len(batch))
		for i := range batch {
			refs[i] = backupproxy.KnownChunkRef{
				Digest: batch[i].digest,
				Size:   batch[i].size,
			}
		}
		if err := ow.writer.InjectChunks(refs); err != nil {
			return err
		}
		chunks = chunks[len(batch):]
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

	return ow.writeRefOrReencode(entry, pxarEntry, e.name, refOffset)
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
	return ow.writeRefOrReencode(clone, pxarEntry, e.name, refOffset)
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
	abs := ow.mfs.mutablePath(childPath)
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("open backed file %q: %w", childPath, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat backed file %q: %w", childPath, err)
	}

	entry := ow.allocEntry()
	entry.Path = name
	entry.Kind = pxar.KindFile
	entry.Metadata = meta
	entry.FileSize = uint64(fi.Size())

	h := xxh3.New()
	tee := io.TeeReader(f, h)

	if ow.prog != nil {
		ow.prog.SetMsg(childPath)
	}

	if err := ow.writer.WriteEntryReader(entry, tee, uint64(fi.Size())); err != nil {
		return fmt.Errorf("write backed file %q: %w", name, err)
	}

	ow.backedHashes[childPath] = h.Sum64()
	ow.mutableFiles++

	if ow.prog != nil {
		ow.prog.AddFile(fi.Size())
	}
	return nil
}
