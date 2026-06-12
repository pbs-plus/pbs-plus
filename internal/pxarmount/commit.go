package pxarmount

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

var commitMu sync.Mutex

var lastCommitTime int64

type CommitRequest struct {
	PBSURL     string
	Datastore  string
	AuthToken  string
	Namespace  string
	BackupID   string
	BackupType string
	SkipTLS    bool
}

func ParseCommitLine(line string) (*CommitRequest, error) {
	parts := strings.SplitN(line, " ", 7)
	if len(parts) < 1 || parts[0] != "COMMIT" {
		return nil, fmt.Errorf("invalid COMMIT format")
	}
	req := &CommitRequest{}
	if len(parts) > 1 {
		req.PBSURL = parts[1]
	}
	if len(parts) > 2 {
		req.Datastore = parts[2]
	}
	if len(parts) > 3 {
		req.AuthToken = parts[3]
	}
	if len(parts) > 4 && parts[4] != "-" {
		req.Namespace = parts[4]
	}
	if len(parts) > 5 {
		req.BackupType = parts[5]
	}
	if len(parts) > 6 {
		req.BackupID = parts[6]
	}
	if req.BackupType == "" {
		req.BackupType = "host"
	}
	return req, nil
}

func ReadLocalToken() string {
	candidates := []string{
		filepath.Join("/var/lib/proxmox-backup", "pbs-plus-token.json"),
		filepath.Join("/etc/proxmox-backup", "pbs-plus-token.json"),
		filepath.Join("/var/lib/pbs-plus", "pbs-plus-token.json"),
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var tok struct {
			TokenID string `json:"tokenid"`
			Value   string `json:"value"`
		}
		if err := json.Unmarshal(data, &tok); err != nil {
			continue
		}
		if tok.Value != "" {
			return tok.TokenID + ":" + tok.Value
		}
	}
	return ""
}

func ResolveDatastoreName(pbsStore string) string {
	out, err := exec.Command("proxmox-backup-manager", "datastore", "list", "--output-format", "json").Output()
	if err != nil {
		return filepath.Base(pbsStore)
	}
	var dss []struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(out, &dss); err != nil {
		return filepath.Base(pbsStore)
	}
	cleanPath := filepath.Clean(pbsStore)
	for _, ds := range dss {
		if filepath.Clean(ds.Path) == cleanPath {
			return ds.Name
		}
	}
	return filepath.Base(pbsStore)
}

func StartCommitListener(sockPath string, mfs *MutableFS) (net.Listener, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sockPath, 0o660); err != nil {
		_ = l.Close()
		return nil, err
	}

	hub, err := newCommitHub(sockPath, mfs.verbose)
	if err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("start monitor hub: %w", err)
	}
	globalCommitHub = hub

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleCommitConn(mfs, conn)
		}
	}()
	return l, nil
}

func handleCommitConn(mfs *MutableFS, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return
	}
	line := scanner.Text()
	req, err := ParseCommitLine(line)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "ERR %v\n", err)
		return
	}

	detached := false
	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if scanner.Scan() {
		detached = scanner.Text() == "DETACH"
	}
	_ = conn.SetReadDeadline(time.Time{})

	if detached {
		jobID := globalCommitHub.startJob()
		_, _ = fmt.Fprintf(conn, "JOB %d\n", jobID)
		_ = conn.Close()

		go func() {
			defer globalCommitHub.endJob()
			prog := newHubProgressReporter()
			if err := CommitSnapshot(mfs, req, prog); err != nil {
				prog.Error(err.Error())
				return
			}
		}()
	} else {
		globalCommitHub.startJob()
		defer globalCommitHub.endJob()

		primary := NewProgressReporter(conn)
		prog := &fanoutReporter{primary: primary, hub: globalCommitHub, started: time.Now()}
		if err := CommitSnapshot(mfs, req, prog); err != nil {
			prog.Error(err.Error())
			return
		}
	}
}

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

const maxPendingRefs = 512

const chunkPaddingThreshold = 0.1

type reusableChunk struct {
	size      uint64
	padding   uint64
	digest    [32]byte
	endOffset uint64
}

func (c *reusableChunk) sameIndexedChunkAs(other *reusableChunk) bool {
	return c.digest == other.digest && c.endOffset == other.endOffset
}

type CommitProgress interface {
	SetPhase(ProgressPhase)
	SetMsg(msg string)
	Done(msg string)
	Error(msg string)
	AddFile(bytes int64)
	SetTotals(files, bytes int64)
	State() ProgressState
}

var _ CommitProgress = (*ProgressReporter)(nil)

func CommitSnapshot(mfs *MutableFS, req *CommitRequest, prog CommitProgress) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	mfs.freezeMu.Lock()
	mfs.frozen = true
	mfs.freezeMu.Unlock()

	if err := mfs.journal.Sync(); err != nil {
		mfs.freezeMu.Lock()
		mfs.frozen = false
		mfs.freezeMu.Unlock()
		mfs.freezeCond.Broadcast()
		return fmt.Errorf("sync journal before commit: %w", err)
	}

	defer func() {
		mfs.freezeMu.Lock()
		mfs.frozen = false
		mfs.freezeMu.Unlock()
		mfs.freezeCond.Broadcast()
	}()

	prog.SetPhase(PhasePrepare)
	prog.SetMsg("Resolving PBS connection")

	pbsURL := req.PBSURL
	if pbsURL == "" {
		pbsURL = "https://localhost:8007/api2/json"
		req.SkipTLS = true
	}
	datastoreName := req.Datastore
	if datastoreName == "" {
		datastoreName = ResolveDatastoreName(mfs.pbsStore)
	}
	authToken := req.AuthToken
	if authToken == "" {
		authToken = ReadLocalToken()
	}

	backupID := req.BackupID
	if backupID == "" {
		backupID = mfs.origSnapshot.BackupID
	}
	if backupID == "" {
		backupID, _ = os.Hostname()
	}
	backupType := req.BackupType
	if backupType == "" {
		backupType = mfs.origSnapshot.BackupType
	}
	namespace := req.Namespace
	if namespace == "" && mfs.origSnapshot.Namespace != "" {
		namespace = mfs.origSnapshot.Namespace
	}
	archiveName := mfs.origSnapshot.ArchiveName
	if archiveName == "" {
		archiveName = backupID
	}
	now := time.Now().Unix()
	if now <= lastCommitTime {
		now = lastCommitTime + 1
	}
	lastCommitTime = now
	backupTime := now

	if err := ensureNamespaceDir(mfs.pbsStore, namespace); err != nil {
		return fmt.Errorf("ensure namespace dir: %w", err)
	}

	bt, err := datastore.ParseBackupType(backupType)
	if err != nil {
		return fmt.Errorf("invalid backup type %q: %w", backupType, err)
	}

	var prev *backupproxy.PreviousBackupRef
	if mfs.origSnapshot.BackupID != "" && mfs.origSnapshot.BackupTime > 0 {
		prev = &backupproxy.PreviousBackupRef{
			BackupType: bt,
			BackupID:   mfs.origSnapshot.BackupID,
			BackupTime: mfs.origSnapshot.BackupTime,
			Namespace:  mfs.origSnapshot.Namespace,
		}
	}

	store := backupproxy.NewPBSStore(backupproxy.PBSConfig{
		BaseURL:       pbsURL,
		Datastore:     datastoreName,
		AuthToken:     authToken,
		Namespace:     namespace,
		SkipTLSVerify: req.SkipTLS,
	}, func() buzhash.Config {
		cfg, _ := buzhash.NewConfig(4 << 20)
		return cfg
	}(), false)

	ctx := context.Background()
	session, err := store.StartSession(ctx, backupproxy.BackupConfig{
		BackupType:     bt,
		BackupID:       backupID,
		BackupTime:     backupTime,
		Namespace:      namespace,
		PreviousBackup: prev,
		CryptMode:      datastore.CryptModeNone,
	})
	if err != nil {
		return fmt.Errorf("start PBS session: %w", err)
	}

	metaName := archiveName + ".mpxar.didx"
	payloadName := archiveName + ".ppxar.didx"

	var origPayloadIdx []byte
	if mfs.origPpxarDidx != "" {
		origPayloadIdx, _ = os.ReadFile(mfs.origPpxarDidx)
	}

	writer, err := transfer.NewRemoteDedupWriter(ctx, session, metaName, payloadName)
	if err != nil {
		return fmt.Errorf("create dedup writer: %w", err)
	}

	rootNode, _ := mfs.journal.GetNode(1)
	var rootMeta pxar.Metadata
	if rootNode != nil {
		rootMeta = nodeToMetadata(rootNode, nil)
	} else {
		rootEntry, err := mfs.pxar.GetPxarEntry(RootInode)
		if err != nil {
			return fmt.Errorf("read pxar root: %w", err)
		}
		rootMeta = buildMetaFromPxarEntry(rootEntry)
	}

	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		return fmt.Errorf("begin archive: %w", err)
	}

	prog.SetPhase(PhaseWalk)
	prog.SetMsg("Archiving files")

	ow := &commitWalkState{
		mfs:           mfs,
		writer:        writer,
		prog:          prog,
		xattrCache:    make(map[int64][]format.XAttr),
		backedHashes:  make(map[string]uint64),
		redirectCache: make(map[string]*pxar.Entry),
		pendingRefs:   make([]commitEntry, 0, 64),
		entryCache:    make(map[uint64]*pxar.Entry, 4096),
	}

	if len(origPayloadIdx) > 0 {
		if idx, err := datastore.ParseDynamicIndex(origPayloadIdx); err == nil {
			ow.origChunkIndex = idx
		}
	}

	ow.mfs.pxar.readerMu.RLock()
	defer ow.mfs.pxar.readerMu.RUnlock()

	if err := ow.commitWalk(1, RootInode, "/"); err != nil {
		return fmt.Errorf("walk overlay: %w", err)
	}

	prog.SetPhase(PhaseUpload)
	prog.SetMsg(fmt.Sprintf("Flushing upload (%d new/modified files)", ow.mutableFiles))

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}

	if _, err := session.Finish(ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}

	prog.SetPhase(PhaseVerify)
	prog.SetMsg(fmt.Sprintf("Verifying %d backed files", len(ow.backedHashes)))

	if len(ow.backedHashes) > 0 {
		if err := verifyBackedFileHashes(mfs, ow.backedHashes, prog); err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
	}

	prog.SetPhase(PhaseFinalize)
	prog.SetMsg("Swapping snapshot")

	if err := postCommit(mfs, backupID, backupType, namespace, archiveName, backupTime); err != nil {
		return fmt.Errorf("post-commit: %w", err)
	}

	prog.Done(fmt.Sprintf("committed %s/%s (%d new files)", namespace, backupID, ow.mutableFiles))
	return nil
}

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
			node, _ := ow.mfs.journal.GetNode(edge.ChildID)
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
			node, _ := ow.mfs.journal.GetNode(edge.ChildID)
			if node == nil {
				journalIdx++
				continue
			}
			entry = commitEntry{name: edge.Name, node: node}
			journalIdx++
		} else {
			edge := &journalEdges[journalIdx]
			node, _ := ow.mfs.journal.GetNode(edge.ChildID)
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

func (ow *commitWalkState) writeBackedFile(name, childPath string, meta pxar.Metadata) error {
	abs := ow.mfs.mutablePath(childPath)
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("open backed file %q: %w", childPath, err)
	}
	defer func() { _ = f.Close() }()

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
	defer func() { _ = rc.Close() }()
	if err := ow.writer.WriteEntryReader(entry, rc, pxarEntry.FileSize); err != nil {
		return fmt.Errorf("write re-encoded %q: %w", name, err)
	}
	return nil
}

func nodeToMetadata(n *GraphNode, xattrs []format.XAttr) pxar.Metadata {
	var modeFormat uint64
	switch n.Kind {
	case NodeDir:
		modeFormat = format.ModeIFDIR
	case NodeSymlink:
		modeFormat = format.ModeIFLNK
	default:
		modeFormat = format.ModeIFREG
	}

	return pxar.Metadata{
		Stat: format.Stat{
			Mode: modeFormat | uint64(n.Mode&0o7777),
			UID:  n.UID,
			GID:  n.GID,
			Mtime: format.StatxTimestamp{
				Secs:  n.MtimeNs / 1e9,
				Nanos: uint32(n.MtimeNs % 1e9),
			},
		},
		XAttrs: xattrs,
	}
}

func buildMetaFromPxarEntry(e *pxar.Entry) pxar.Metadata {
	return pxar.Metadata{
		Stat:   e.Metadata.Stat,
		XAttrs: e.Metadata.XAttrs,
		FCaps:  e.Metadata.FCaps,
		ACL:    e.Metadata.ACL,
	}
}

func mergeMetaWithPxar(journalMeta pxar.Metadata, pxarEntry *pxar.Entry) pxar.Metadata {
	out := journalMeta
	out.Stat.Flags = pxarEntry.Metadata.Stat.Flags
	out.QuotaProjectID = pxarEntry.Metadata.QuotaProjectID
	out.FCaps = pxarEntry.Metadata.FCaps
	out.ACL = pxarEntry.Metadata.ACL
	return out
}

func (ow *commitWalkState) clonePxarEntryBuf(e *pxar.Entry, name string) *pxar.Entry {
	ow.entryBuf = *e
	ow.entryBuf.Path = name
	return &ow.entryBuf
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

var xxh3Pool = sync.Pool{
	New: func() any { return xxh3.New() },
}

func verifyBackedFileHashes(mfs *MutableFS, hashes map[string]uint64, prog CommitProgress) error {
	workers := min(min(runtime.NumCPU(), 16), len(hashes))

	type item struct {
		relPath  string
		expected uint64
	}
	items := make([]item, 0, len(hashes))
	for p, h := range hashes {
		items = append(items, item{p, h})
	}
	total := len(items)

	var (
		idx      atomic.Int64
		verified atomic.Int64
		firstErr error
		errOnce  sync.Once
	)

	lastProgress := time.Now()

	hasher := func() {
		buf := make([]byte, 64*1024)
		h := xxh3Pool.Get().(*xxh3.Hasher)
		defer xxh3Pool.Put(h)
		for {
			i := int(idx.Add(1) - 1)
			if i >= total {
				return
			}
			it := &items[i]

			if firstErr != nil {
				return
			}

			abs := mfs.mutablePath(it.relPath)
			f, err := os.Open(abs)
			if err != nil {
				errOnce.Do(func() { firstErr = fmt.Errorf("open backed file %q for verification: %w", it.relPath, err) })
				return
			}
			h.Reset()
			_, err = io.CopyBuffer(h, f, buf)
			_ = f.Close()
			if err != nil {
				errOnce.Do(func() { firstErr = fmt.Errorf("hash backed file %q: %w", it.relPath, err) })
				return
			}
			if h.Sum64() != it.expected {
				errOnce.Do(func() { firstErr = fmt.Errorf("backed file %q content hash differs", it.relPath) })
				return
			}

			done := verified.Add(1)
			if time.Since(lastProgress) >= 200*time.Millisecond || int(done) == total {
				pct := int(done) * 100 / total
				prog.SetMsg(fmt.Sprintf("%s (%d/%d, %d%%)", it.relPath, int(done), total, pct))
				lastProgress = time.Now()
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			hasher()
		}()
	}
	wg.Wait()

	return firstErr
}

func ensureNamespaceDir(pbsStore, namespace string) error {
	if pbsStore == "" || namespace == "" {
		return nil
	}
	parts := strings.Split(namespace, "/")
	cur := pbsStore
	for _, p := range parts {
		if p == "" {
			continue
		}
		cur = filepath.Join(cur, "ns", p)
		if err := os.MkdirAll(cur, 0o755); err != nil {
			return err
		}
		_ = os.Chown(cur, 34, 34)
	}
	return nil
}

func postCommit(mfs *MutableFS, backupID, backupType, namespace, archiveName string, backupTime int64) error {
	var groupDir string
	if mfs.origPpxarDidx != "" {
		origDir := filepath.Dir(mfs.origPpxarDidx)
		groupDir = filepath.Dir(origDir)
	} else {
		groupDir = snapshotGroupDir(mfs.pbsStore, backupType, backupID, namespace)
	}

	newTimeISO := time.Unix(backupTime, 0).UTC().Format("2006-01-02T15:04:05Z")
	snapDir := filepath.Join(groupDir, newTimeISO)

	mpxarPath := filepath.Join(snapDir, archiveName+".mpxar.didx")
	ppxarPath := filepath.Join(snapDir, archiveName+".ppxar.didx")

	metaData, err := mmapFile(mpxarPath)
	if err != nil {
		return fmt.Errorf("mmap new mpxar: %w", err)
	}
	payloadData, err := mmapFile(ppxarPath)
	if err != nil {
		_ = munmap(metaData)
		return fmt.Errorf("mmap new ppxar: %w", err)
	}

	store, err := datastore.NewChunkStore(mfs.pbsStore)
	if err != nil {
		_ = munmap(metaData)
		_ = munmap(payloadData)
		return fmt.Errorf("open chunk store: %w", err)
	}
	source := datastore.NewChunkStoreSource(store)

	newReader, err := transfer.NewSplitReader(metaData, payloadData, source)
	if err != nil {
		_ = munmap(metaData)
		_ = munmap(payloadData)
		return fmt.Errorf("create new reader: %w", err)
	}

	if err := mfs.journal.Clear(); err != nil {
		return fmt.Errorf("clear journal: %w", err)
	}
	if err := mfs.journal.Sync(); err != nil {
		return fmt.Errorf("sync journal after clear: %w", err)
	}

	for _, d := range mfs.mmapData {
		_ = munmap(d)
	}

	mfs.pxar.HotSwap(newReader)
	mfs.mmapData = nil
	if len(metaData) > 0 {
		mfs.mmapData = append(mfs.mmapData, metaData)
	}
	if len(payloadData) > 0 {
		mfs.mmapData = append(mfs.mmapData, payloadData)
	}

	mfs.resetAfterCommit()

	mfs.origSnapshot = snapshotRef{
		BackupType:  backupType,
		BackupID:    backupID,
		BackupTime:  backupTime,
		Namespace:   namespace,
		ArchiveName: archiveName,
	}
	mfs.origPpxarDidx = ppxarPath

	entries, err := os.ReadDir(mfs.mutableDir)
	if err != nil {
		return fmt.Errorf("read mutable dir: %w", err)
	}
	for _, e := range entries {
		if e.Name() == JournalDir {
			continue
		}
		if err := os.RemoveAll(filepath.Join(mfs.mutableDir, e.Name())); err != nil {
			return fmt.Errorf("remove mutable entry %s: %w", e.Name(), err)
		}
	}

	return nil
}

func snapshotGroupDir(pbsStore, backupType, backupID, namespace string) string {
	base := pbsStore
	if namespace != "" {
		parts := strings.SplitSeq(namespace, "/")
		for p := range parts {
			if p != "" {
				base = filepath.Join(base, "ns", p)
			}
		}
	}
	return filepath.Join(base, backupType, backupID)
}

func ParseOrigSnapshot(pbsStore, ppxarDidx string) snapshotRef {
	rel := strings.TrimPrefix(ppxarDidx, pbsStore)
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")

	var ref snapshotRef
	if len(parts) >= 4 {
		filename := parts[len(parts)-1]
		ref.ArchiveName = strings.TrimSuffix(filename, ".didx")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".mpxar")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".ppxar")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".pxar")

		_, _ = fmt.Sscanf(parts[len(parts)-2], "%d", &ref.BackupTime)
		ref.BackupID = parts[len(parts)-3]
		ref.BackupType = parts[len(parts)-4]
		if len(parts) > 4 {
			nsParts := parts[:len(parts)-4]
			var clean []string
			for i := 0; i < len(nsParts); i++ {
				if nsParts[i] == "ns" && i+1 < len(nsParts) {
					i++
					clean = append(clean, nsParts[i])
				}
			}
			ref.Namespace = strings.Join(clean, "/")
		}
	}
	if ref.BackupType == "" {
		ref.BackupType = "host"
	}
	if ref.ArchiveName == "" {
		ref.ArchiveName = ref.BackupID
	}
	return ref
}

func RunCommitSubcommand() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")
	pbsURL := fs.String("pbs-url", "", "PBS server URL")
	datastoreName := fs.String("datastore", "", "PBS datastore name")
	authToken := fs.String("token", "", "PBS API token")
	namespace := fs.String("ns", "", "PBS namespace")
	backupType := fs.String("backup-type", "host", "Backup type")
	backupID := fs.String("backup-id", "", "Backup ID")
	detach := fs.Bool("detach", false, "Run commit in background; use 'attach' to watch progress")

	fs.Parse(os.Args[2:]) //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	token := *authToken
	if token == "" {
		token = ReadLocalToken()
	}

	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  \u2717 error connecting to socket %s: %v\n", *socketPath, err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s\n",
		*pbsURL, *datastoreName, token, *namespace, *backupType, *backupID)
	if _, err := fmt.Fprint(conn, cmd); err != nil {
		fmt.Fprintf(os.Stderr, "  \u2717 error sending command: %v\n", err)
		os.Exit(1)
	}

	if *detach {
		if _, err := fmt.Fprintln(conn, "DETACH"); err != nil {
			fmt.Fprintf(os.Stderr, "  \u2717 error sending detach: %v\n", err)
			os.Exit(1)
		}

		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			fmt.Fprintf(os.Stderr, "  \u2717 error: no response from daemon\n")
			os.Exit(1)
		}
		resp := scanner.Text()
		if after, ok := strings.CutPrefix(resp, "ERR "); ok {
			fmt.Fprintf(os.Stderr, "  \u2717 %s\n", after)
			os.Exit(1)
		}
		if after, ok := strings.CutPrefix(resp, "JOB "); ok {
			jobID := after
			fmt.Fprintf(os.Stderr, "  Commit started in background (job %s)\n", jobID)
			fmt.Fprintf(os.Stderr, "  Use 'pxar-mount attach --socket %s' to watch progress\n", *socketPath)
			return
		}
		fmt.Fprintf(os.Stderr, "  \u2717 unexpected response: %s\n", resp)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	display := NewProgressDisplay(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Committing snapshot...\n")

	go func() {
		<-sigCh
		display.stop()
		_, _ = fmt.Fprintln(conn, "DETACH")
		fmt.Fprintf(os.Stderr, "\r  ↗ Commit detached — use 'pxar-mount attach --socket %s' to watch\n", *socketPath)
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "PROGRESS ") {
			display.Update(line)
			continue
		}
		if strings.HasPrefix(line, "OK ") {
			display.Done(line)
			return
		}
		if strings.HasPrefix(line, "ERR ") {
			display.Error(line)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "  \u2717 error: no response from daemon\n")
	os.Exit(1)
}

func RunAttachSubcommand() {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")

	fs.Parse(os.Args[2:]) //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount attach --socket <path>\n\n")
		fmt.Fprintf(os.Stderr, "Connects to a running background commit and streams progress.\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	monPath := *socketPath + ".monitor"
	conn, err := net.DialTimeout("unix", monPath, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Nothing to attach to. No commit in progress.\n")
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		fmt.Fprintf(os.Stderr, "  Nothing to attach to. No commit in progress.\n")
		return
	}
	first := scanner.Text()
	if first == "IDLE" {
		fmt.Fprintf(os.Stderr, "  Nothing to attach to. No commit in progress.\n")
		fmt.Fprintf(os.Stderr, "  Use 'pxar-mount logs --socket %s' to view the last commit.\n", *socketPath)
		return
	}

	display := NewProgressDisplay(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Attached to commit progress...\n")

	for processMonitorLine(first, display) {
		if !scanner.Scan() {
			break
		}
	}
}

func processMonitorLine(line string, display *ProgressDisplay) bool {
	if strings.HasPrefix(line, "PROGRESS ") {
		display.Update(line)
		return true
	}
	if strings.HasPrefix(line, "OK ") {
		display.Done(line)
		return false
	}
	if strings.HasPrefix(line, "ERR ") {
		display.Error(line)
		return false
	}
	fmt.Fprintf(os.Stderr, "  %s\n", line)
	return true
}

func RunLogsSubcommand() {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")

	fs.Parse(os.Args[2:]) //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount logs --socket <path>\n\n")
		fmt.Fprintf(os.Stderr, "Shows the output of the last commit.\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	logPath := *socketPath + ".log"
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "  No commit logs available.\n")
		} else {
			fmt.Fprintf(os.Stderr, "  \u2717 error reading log file %s: %v\n", logPath, err)
		}
		os.Exit(1)
	}

	if len(data) == 0 {
		fmt.Fprintf(os.Stderr, "  No commit logs available.\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "  --- last commit ---\n")
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	display := NewProgressDisplay(os.Stderr)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "PROGRESS ") {
			display.Update(line)
			continue
		}
		if strings.HasPrefix(line, "OK ") {
			display.Done(line)
			continue
		}
		if strings.HasPrefix(line, "ERR ") {
			display.Error(line)
			continue
		}
	}
}
