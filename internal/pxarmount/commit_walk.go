package pxarmount

import (
	"fmt"
	"sort"

	"github.com/pbs-plus/pbs-plus/internal/log"
	pxar "github.com/pbs-plus/pxar"
)

func (ow *commitWalkState) commitWalk(journalParentID int64, pxarInode uint64, relPath string) error {
	savedPending := ow.pendingRefs
	ow.pendingRefs = ow.pendingRefs[:0]
	defer func() { ow.pendingRefs = savedPending }()

	var journalEdges []GraphEdge
	var whiteoutSet map[string]bool
	if journalParentID > 0 {
		var err error
		journalEdges, err = ow.mfs.journal.ListEdges(journalParentID)
		if err != nil {
			return fmt.Errorf("list edges for node %d: %w", journalParentID, err)
		}
		whiteouts, err := ow.mfs.journal.ListWhiteouts(journalParentID)
		if err != nil {
			return fmt.Errorf("list whiteouts for node %d: %w", journalParentID, err)
		}
		if len(whiteouts) > 0 {
			whiteoutSet = make(map[string]bool, len(whiteouts))
			for _, w := range whiteouts {
				whiteoutSet[w] = true
			}
		}
	}

	var pxarEntries []dirEntrySlim
	if pxarInode != 0 {
		var err error
		pxarEntries, err = ow.mfs.pxar.ReadDirFull(pxarInode, ow.entryCache)
		if err != nil {
			pxarEntries = nil
		}
	}

	var edgeNames map[string]bool
	if len(journalEdges) > 0 {
		edgeNames = make(map[string]bool, len(journalEdges))
		for _, e := range journalEdges {
			edgeNames[e.Name] = true
		}
	}

	filtered := 0
	for i := range pxarEntries {
		pe := &pxarEntries[i]
		if whiteoutSet != nil && whiteoutSet[pe.name] {
			continue
		}
		if edgeNames != nil && edgeNames[pe.name] {
			if !pe.isDir {
				continue
			}
		}
		if filtered != i {
			pxarEntries[filtered] = *pe
		}
		filtered++
	}
	pxarEntries = pxarEntries[:filtered]

	sort.Slice(pxarEntries, func(i, j int) bool {
		return pxarEntries[i].name < pxarEntries[j].name
	})
	sort.Slice(journalEdges, func(i, j int) bool {
		return journalEdges[i].Name < journalEdges[j].Name
	})

	var deferredDirs []deferredDir

	pxarIdx, journalIdx := 0, 0
	for pxarIdx < len(pxarEntries) || journalIdx < len(journalEdges) {
		var entry commitEntry
		if pxarIdx >= len(pxarEntries) {
			edge := &journalEdges[journalIdx]
			node, err := ow.mfs.journal.GetNode(edge.ChildID)
			if err != nil {
				log.Error(err, "")
			}
			if node == nil {
				journalIdx++
				continue
			}
			entry = commitEntry{name: edge.Name, node: node}
			journalIdx++
		} else if journalIdx >= len(journalEdges) {
			entry = commitEntry{name: pxarEntries[pxarIdx].name, pxarSlim: &pxarEntries[pxarIdx]}
			pxarIdx++
		} else if pxarEntries[pxarIdx].name < journalEdges[journalIdx].Name {
			entry = commitEntry{name: pxarEntries[pxarIdx].name, pxarSlim: &pxarEntries[pxarIdx]}
			pxarIdx++
		} else if pxarEntries[pxarIdx].name > journalEdges[journalIdx].Name {
			edge := &journalEdges[journalIdx]
			node, err := ow.mfs.journal.GetNode(edge.ChildID)
			if err != nil {
				log.Error(err, "")
			}
			if node == nil {
				journalIdx++
				continue
			}
			entry = commitEntry{name: edge.Name, node: node}
			journalIdx++
		} else {
			edge := &journalEdges[journalIdx]
			node, err := ow.mfs.journal.GetNode(edge.ChildID)
			if err != nil {
				log.Error(err, "")
			}
			if node != nil {
				entry = commitEntry{name: edge.Name, node: node}
				if node.Kind == NodeDir && pxarIdx < len(pxarEntries) && pxarEntries[pxarIdx].name == edge.Name {
					entry.pxarSlim = &pxarEntries[pxarIdx]
				}
			}
			pxarIdx++
			journalIdx++
			if entry.node == nil {
				continue
			}
		}

		isDir := (entry.node != nil && entry.node.Kind == NodeDir) ||
			(entry.node == nil && entry.pxarSlim != nil && entry.pxarSlim.isDir)

		if isDir {
			if err := ow.flushPendingRefs(true); err != nil {
				return err
			}

			dd := deferredDir{name: entry.name}
			if entry.node != nil {
				dd.node = entry.node
				if entry.pxarSlim != nil {
					dd.pxarIno = entry.pxarSlim.inode
					dd.entryStart = entry.pxarSlim.entryStart
				}
			} else {
				dd.entryStart = entry.pxarSlim.entryStart
			}
			deferredDirs = append(deferredDirs, dd)
		} else {
			var err error
			if entry.node != nil {
				err = ow.emitJournalEntry(&entry, relPath)
			} else {
				err = ow.emitPxarEntry(&entry, relPath)
			}
			if err != nil {
				return err
			}
		}
	}

	if err := ow.flushPendingRefs(false); err != nil {
		return err
	}

	pxarEntries = nil
	_ = pxarEntries

	for i := range deferredDirs {
		if err := ow.processDeferredDir(&deferredDirs[i], relPath); err != nil {
			return err
		}
	}

	return nil
}

func (ow *commitWalkState) emitJournalEntry(e *commitEntry, parentRelPath string) error {
	node := e.node

	switch node.Kind {
	case NodeDir:
		if err := ow.flushPendingRefs(true); err != nil {
			return err
		}
		return ow.emitJournalDir(e, parentRelPath)

	case NodeFile:
		if node.HasData {
			if err := ow.flushPendingRefs(false); err != nil {
				return err
			}
			childPath := ow.buildPath(parentRelPath, e.name)
			xattrs := ow.ensureXAttrs(node.ID)
			meta := nodeToMetadata(node, xattrs)
			if node.RedirectTo != "" {
				if pxEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
					meta = mergeMetaWithPxar(meta, pxEntry)
				}
			}
			return ow.writeBackedFile(e.name, childPath, meta)
		}
		if node.RedirectTo != "" {
			if err := ow.addToPendingRefs(e); err != nil {
				return err
			}
			return nil
		}
		if err := ow.flushPendingRefs(false); err != nil {
			return err
		}
		xattrs := ow.ensureXAttrs(node.ID)
		meta := nodeToMetadata(node, xattrs)
		entry := ow.allocEntry()
		entry.Path = e.name
		entry.Kind = pxar.KindFile
		entry.Metadata = meta
		entry.FileSize = node.Size
		return ow.writer.WriteEntry(entry, nil)

	case NodeSymlink:
		if err := ow.flushPendingRefs(true); err != nil {
			return err
		}
		xattrs := ow.ensureXAttrs(node.ID)
		meta := nodeToMetadata(node, xattrs)
		if node.RedirectTo != "" {
			if pxEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
				meta = mergeMetaWithPxar(meta, pxEntry)
			}
		}
		entry := ow.allocEntry()
		entry.Path = e.name
		entry.Kind = pxar.KindSymlink
		entry.Metadata = meta
		entry.LinkTarget = node.SymlinkTgt
		return ow.writer.WriteEntry(entry, nil)
	}
	return nil
}

func (ow *commitWalkState) emitPxarEntry(e *commitEntry, parentRelPath string) error {
	slim := e.pxarSlim
	if slim == nil {
		return nil
	}

	if slim.isDir {
		if err := ow.flushPendingRefs(true); err != nil {
			return err
		}
		return ow.emitPxarDir(e, parentRelPath)
	}

	if slim.isSymlink {
		if err := ow.flushPendingRefs(true); err != nil {
			return err
		}
		return ow.emitPxarSymlink(e)
	}

	if cached, ok := ow.entryCache[slim.entryStart]; ok {
		e.cachedEntry = cached
		e.sortKey = cached.PayloadOffset
	} else {
		pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
		if err != nil {
			return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
		}
		e.cachedEntry = pxarEntry
		e.sortKey = pxarEntry.PayloadOffset
	}

	return ow.addToPendingRefs(e)
}

func (ow *commitWalkState) emitJournalDir(e *commitEntry, parentRelPath string) error {
	node := e.node
	childPath := ow.buildPath(parentRelPath, e.name)
	xattrs := ow.ensureXAttrs(node.ID)
	meta := nodeToMetadata(node, xattrs)

	var pxarChildIno uint64
	if node.RedirectTo != "" {
		if pxDirEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
			meta = mergeMetaWithPxar(meta, pxDirEntry)
			pxarChildIno = ToInode(pxDirEntry)
			ow.registerPxarDir(pxDirEntry)
		}
	} else if e.pxarSlim != nil {
		pxarChildIno = e.pxarSlim.inode
	}

	if err := ow.writer.BeginDirectory(e.name, &meta); err != nil {
		return fmt.Errorf("begin dir %q: %w", e.name, err)
	}
	if err := ow.commitWalk(node.ID, pxarChildIno, childPath); err != nil {
		return err
	}
	return ow.writer.EndDirectory()
}

func (ow *commitWalkState) emitPxarDir(e *commitEntry, parentRelPath string) error {
	slim := e.pxarSlim
	childPath := ow.buildPath(parentRelPath, e.name)

	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
	}

	childIno := ToInode(pxarEntry)

	ow.registerPxarDir(pxarEntry)

	meta := buildMetaFromPxarEntry(pxarEntry)

	if err := ow.writer.BeginDirectory(e.name, &meta); err != nil {
		return fmt.Errorf("begin pxar dir %q: %w", e.name, err)
	}
	if err := ow.commitWalk(0, childIno, childPath); err != nil {
		return err
	}
	return ow.writer.EndDirectory()
}

func (ow *commitWalkState) emitPxarSymlink(e *commitEntry) error {
	slim := e.pxarSlim

	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
	}

	clone := ow.clonePxarEntryBuf(pxarEntry, e.name)
	return ow.writer.WriteEntry(clone, nil)
}

func (ow *commitWalkState) registerPxarDir(pxarEntry *pxar.Entry) {
	childIno := ToInode(pxarEntry)
	slim := dirEntrySlim{
		name:          pxarEntry.FileName(),
		inode:         childIno,
		entryStart:    pxarEntry.FileOffset,
		contentOffset: pxarEntry.ContentOffset,
		payloadOffset: pxarEntry.PayloadOffset,
		fileSize:      pxarEntry.FileSize,
		mode:          statMode(pxarEntry.Metadata.Stat.Mode),
		uid:           pxarEntry.Metadata.Stat.UID,
		gid:           pxarEntry.Metadata.Stat.GID,
		mtimeSecs:     pxarEntry.Metadata.Stat.Mtime.Secs,
		mtimeNanos:    pxarEntry.Metadata.Stat.Mtime.Nanos,
		isDir:         pxarEntry.IsDir(),
		isSymlink:     pxarEntry.IsSymlink(),
		isReg:         pxarEntry.IsRegularFile(),
	}
	ow.mfs.pxar.RegisterSlimNode(&slim, RootInode)
}

func (ow *commitWalkState) processDeferredDir(dd *deferredDir, parentRelPath string) error {
	childPath := ow.buildPath(parentRelPath, dd.name)

	if dd.node != nil {
		node := dd.node
		xattrs := ow.ensureXAttrs(node.ID)
		meta := nodeToMetadata(node, xattrs)

		var pxarChildIno uint64
		if node.RedirectTo != "" {
			if pxDirEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
				meta = mergeMetaWithPxar(meta, pxDirEntry)
				pxarChildIno = ToInode(pxDirEntry)
				ow.registerPxarDir(pxDirEntry)
			}
		} else if dd.pxarIno != 0 {
			pxarChildIno = dd.pxarIno
			ow.mfs.pxar.mu.RLock()
			_, cached := ow.mfs.pxar.nodes[pxarChildIno]
			ow.mfs.pxar.mu.RUnlock()
			if !cached {
				pxarEntry, rerr := ow.mfs.pxar.Reader().ReadEntryAt(int64(dd.entryStart))
				if rerr == nil {
					ow.registerPxarDir(pxarEntry)
				}
			}
		}

		if err := ow.writer.BeginDirectory(dd.name, &meta); err != nil {
			return fmt.Errorf("begin dir %q: %w", dd.name, err)
		}
		if err := ow.commitWalk(node.ID, pxarChildIno, childPath); err != nil {
			return err
		}
		return ow.writer.EndDirectory()
	}

	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(dd.entryStart))
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", dd.entryStart, err)
	}

	childIno := ToInode(pxarEntry)

	ow.registerPxarDir(pxarEntry)

	meta := buildMetaFromPxarEntry(pxarEntry)

	if err := ow.writer.BeginDirectory(dd.name, &meta); err != nil {
		return fmt.Errorf("begin pxar dir %q: %w", dd.name, err)
	}
	if err := ow.commitWalk(0, childIno, childPath); err != nil {
		return err
	}
	return ow.writer.EndDirectory()
}

func (ow *commitWalkState) resolvePxarEntryCached(relPath string) (*pxar.Entry, error) {
	if entry, ok := ow.redirectCache[relPath]; ok {
		return entry, nil
	}

	entry, err := ow.resolvePxarEntryUncached(relPath)
	if err != nil {
		return nil, err
	}
	ow.redirectCache[relPath] = entry
	return entry, nil
}

func (ow *commitWalkState) resolvePxarEntryUncached(relPath string) (*pxar.Entry, error) {
	if relPath == "/" || relPath == "" {
		return ow.mfs.pxar.Reader().ReadRoot()
	}

	slim, err := ow.mfs.pxar.Reader().Lookup(relPath)
	if err != nil {
		return nil, fmt.Errorf("lookup %q: %w", relPath, err)
	}

	full, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.FileOffset))
	if err != nil {
		return nil, fmt.Errorf("read entry at %d: %w", slim.FileOffset, err)
	}
	return full, nil
}

func (ow *commitWalkState) writeRefOrReencode(entry *pxar.Entry, pxarEntry *pxar.Entry, name string, refOffset uint64) error {
	if ow.hasPrevRef && refOffset <= ow.prevRefOffset {
		ow.mfs.debugf("ref %q offset=%d <= prevRef=%d, re-encoding", name, refOffset, ow.prevRefOffset)
		return ow.writeReencoded(pxarEntry, entry, name)
	}

	if err := ow.writer.WriteEntryRef(entry, refOffset); err != nil {
		ow.mfs.debugf("ref %q offset=%d writer rejected: %v, re-encoding", name, refOffset, err)
		return ow.writeReencoded(pxarEntry, entry, name)
	}

	ow.prevRefOffset = refOffset
	ow.hasPrevRef = true
	return nil
}

func (ow *commitWalkState) writeReencoded(pxarEntry *pxar.Entry, entry *pxar.Entry, name string) error {
	rc, err := ow.mfs.pxar.reader.ReadFileContentReader(pxarEntry)
	if err != nil {
		return fmt.Errorf("read pxar content for re-encode %q: %w", name, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
	}()
	if err := ow.writer.WriteEntryReader(entry, rc, pxarEntry.FileSize); err != nil {
		return fmt.Errorf("write re-encoded %q: %w", name, err)
	}
	return nil
}
