package pxarmount

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// commitMu serializes commit operations globally.
var commitMu sync.Mutex

// CommitRequest holds parameters for a re-snapshot commit.
type CommitRequest struct {
	PBSURL     string
	Datastore  string
	AuthToken  string
	Namespace  string
	BackupID   string
	BackupType string
	SkipTLS    bool
}

// ParseCommitLine parses a COMMIT line from the socket protocol.
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

// ReadLocalToken reads the PBS API token from pbs-plus-token.json.
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

// ResolveDatastoreName finds the PBS datastore name by path.
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

// StartCommitListener listens on a Unix socket for commit requests.
func StartCommitListener(sockPath string, mfs *MutableFS) (net.Listener, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sockPath, 0o660); err != nil {
		l.Close()
		return nil, err
	}
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
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return
	}
	line := scanner.Text()
	req, err := ParseCommitLine(line)
	if err != nil {
		fmt.Fprintf(conn, "ERR %v\n", err)
		return
	}
	prog := NewProgressReporter(conn)
	if err := CommitSnapshot(mfs, req, prog); err != nil {
		prog.Error(err.Error())
		return
	}
}

// --- commit core ---

// commitEntry represents one item in the merged directory view during commit walk.
type commitEntry struct {
	name       string
	node       *GraphNode    // journal node (nil for pure pxar)
	pxarSlim   *dirEntrySlim // pxar slim entry (nil for journal-only)
	pxarOffset uint64        // sort key: pxar payload/entry offset
}

// commitWalkState holds state for the recursive commit walk.
type commitWalkState struct {
	mfs          *MutableFS
	writer       transfer.ArchiveWriter
	dedup        *transfer.RemoteDedupSplitArchiveWriter
	prog         *ProgressReporter
	pxarDirCache map[uint64][]dirEntrySlim // pxar inode → cached dir entries
	xattrCache   map[int64][]format.XAttr  // journal node ID → xattrs
	backedHashes map[string][32]byte       // relPath → SHA256 of uploaded files
	mutableFiles int                       // count of new/modified files
}

// CommitSnapshot creates a new pxar snapshot from the current journal state.
//
// The algorithm:
//  1. Freeze FUSE writes to get a consistent view
//  2. Snapshot journal state (edges, whiteouts, xattrs) into memory
//  3. Start a PBS backup session with previous-archive dedup
//  4. Walk the merged tree (journal overlay + pxar base):
//     - Journal nodes with HasData → stream from mutable dir
//     - Journal nodes with RedirectTo → emit pxar chunk ref (dedup)
//     - Whiteout → skip pxar entry
//     - Pure pxar entries → emit chunk ref (dedup)
//  5. Finish upload, hot-swap pxar reader, reset journal
func CommitSnapshot(mfs *MutableFS, req *CommitRequest, prog *ProgressReporter) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	// --- Phase 0: Freeze FUSE mutations ---
	mfs.freezeMu.Lock()
	mfs.frozen = true
	mfs.freezeMu.Unlock()
	defer func() {
		mfs.freezeMu.Lock()
		mfs.frozen = false
		mfs.freezeMu.Unlock()
		mfs.freezeCond.Broadcast()
	}()

	// --- Phase 1: Resolve PBS connection parameters ---
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
	backupTime := time.Now().Unix()

	// Ensure namespace directory exists on disk.
	if err := ensureNamespaceDir(mfs.pbsStore, namespace); err != nil {
		return fmt.Errorf("ensure namespace dir: %w", err)
	}

	bt, err := datastore.ParseBackupType(backupType)
	if err != nil {
		return fmt.Errorf("invalid backup type %q: %w", backupType, err)
	}

	// --- Phase 2: Start PBS session with previous backup for dedup ---
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
		cfg, _ := buzhash.NewConfig(4096)
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

	// --- Phase 3: Create dedup writer ---
	metaName := archiveName + ".mpxar.didx"
	payloadName := archiveName + ".ppxar.didx"

	var origPayloadIdx []byte
	if mfs.origPpxarDidx != "" {
		origPayloadIdx, _ = os.ReadFile(mfs.origPpxarDidx)
	}

	writer, err := transfer.NewRemoteDedupSplitArchiveWriter(ctx, session, metaName, payloadName, origPayloadIdx)
	if err != nil {
		return fmt.Errorf("create dedup writer: %w", err)
	}

	// --- Phase 4: Begin archive with root metadata ---
	rootNode, _ := mfs.journal.GetNode(1) // root journal node (may be nil)
	var rootMeta pxar.Metadata
	if rootNode != nil {
		rootMeta = nodeToMetadata(rootNode, nil)
	} else {
		// Use pxar root metadata.
		rootEntry, err := mfs.pxar.GetPxarEntry(RootInode)
		if err != nil {
			return fmt.Errorf("read pxar root: %w", err)
		}
		rootMeta = buildMetaFromPxarEntry(rootEntry)
	}

	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		return fmt.Errorf("begin archive: %w", err)
	}

	// --- Phase 5: Walk merged tree ---
	prog.SetPhase(PhaseWalk)
	prog.SetMsg("Scanning overlay")

	ow := &commitWalkState{
		mfs:          mfs,
		writer:       writer,
		dedup:        writer,
		prog:         prog,
		pxarDirCache: make(map[uint64][]dirEntrySlim),
		xattrCache:   make(map[int64][]format.XAttr),
		backedHashes: make(map[string][32]byte),
	}

	// Pre-load all journal xattrs for batch access.
	allXAttrs, _ := mfs.journal.AllXAttrs()
	for nodeID, xmap := range allXAttrs {
		var xattrs []format.XAttr
		for name, val := range xmap {
			xattrs = append(xattrs, format.NewXAttr([]byte(name), val))
		}
		ow.xattrCache[nodeID] = xattrs
	}

	if err := ow.commitWalk(1, RootInode, "/"); err != nil {
		return fmt.Errorf("walk overlay: %w", err)
	}

	// --- Phase 6: Finish upload ---
	prog.SetPhase(PhaseUpload)
	prog.SetMsg(fmt.Sprintf("Uploading (%d new/modified files)", ow.mutableFiles))

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}

	if _, err := session.Finish(ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}

	// --- Phase 7: Verify backed files ---
	prog.SetPhase(PhaseVerify)
	prog.SetMsg(fmt.Sprintf("Verifying %d backed files", len(ow.backedHashes)))

	if len(ow.backedHashes) > 0 {
		if err := verifyBackedFileHashes(mfs, ow.backedHashes); err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
	}

	// --- Phase 8: Hot-swap and reset ---
	prog.SetPhase(PhaseFinalize)
	prog.SetMsg("Swapping snapshot")

	if err := postCommit(mfs, backupID, backupType, namespace, archiveName, backupTime); err != nil {
		return fmt.Errorf("post-commit: %w", err)
	}

	prog.Done(fmt.Sprintf("committed %s/%s (%d new files)", namespace, backupID, ow.mutableFiles))
	return nil
}

// commitWalk recursively walks the merged filesystem tree rooted at the given
// journal node ID and pxar inode, writing entries to the archive writer.
//
// Merge semantics match the runtime FUSE behavior:
//   - Journal edges always take priority over pxar entries
//   - Whiteouts hide pxar entries
//   - Once off the journal, entries come from pure pxar
func (ow *commitWalkState) commitWalk(journalParentID int64, pxarInode uint64, relPath string) error {
	// Get journal children.
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
		whiteoutSet = make(map[string]bool, len(whiteouts))
		for _, w := range whiteouts {
			whiteoutSet[w] = true
		}
	}

	// Get pxar children (cached).
	var pxarEntries []dirEntrySlim
	if pxarInode != 0 {
		var ok bool
		pxarEntries, ok = ow.pxarDirCache[pxarInode]
		if !ok {
			var err error
			pxarEntries, err = ow.mfs.pxar.ReadDirRaw(pxarInode)
			if err != nil {
				pxarEntries = nil
			}
			ow.pxarDirCache[pxarInode] = pxarEntries
		}
	}

	// Build lookup maps.
	edgeNames := make(map[string]bool, len(journalEdges))
	for _, e := range journalEdges {
		edgeNames[e.Name] = true
	}

	pxarByName := make(map[string]*dirEntrySlim, len(pxarEntries))
	for i := range pxarEntries {
		pxarByName[pxarEntries[i].name] = &pxarEntries[i]
	}

	// Build merged entry list.
	var entries []commitEntry

	// Add journal edges.
	for i := range journalEdges {
		edge := &journalEdges[i]
		node, err := ow.mfs.journal.GetNode(edge.ChildID)
		if err != nil || node == nil {
			continue
		}

		pxarOffset := ^uint64(0) // sort last by default

		// If node wraps pxar content, resolve its payload offset for sorting.
		if node.RedirectTo != "" && !node.HasData {
			if pe := resolvePxarPayloadOffset(ow, node.RedirectTo); pe != 0 {
				pxarOffset = pe
			}
		}
		// If pxar has an entry with the same name, use its offset for locality.
		if pxarOffset == ^uint64(0) {
			if pe, ok := pxarByName[edge.Name]; ok {
				pxarOffset = pe.entryStart
			}
		}

		entries = append(entries, commitEntry{
			name:       edge.Name,
			node:       node,
			pxarOffset: pxarOffset,
		})
	}

	// Add non-overridden, non-whited-out pxar entries.
	for i := range pxarEntries {
		pe := &pxarEntries[i]
		if edgeNames[pe.name] || whiteoutSet[pe.name] {
			continue
		}
		offset := pe.contentOffset
		if pe.isDir {
			offset = minDescendantOffset(ow, pe.inode)
		}
		entries = append(entries, commitEntry{
			name:       pe.name,
			pxarSlim:   pe,
			pxarOffset: offset,
		})
	}

	// Sort by pxar offset for sequential payload access.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].pxarOffset < entries[j].pxarOffset
	})

	if f, err := os.OpenFile("/tmp/commit-sort.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		fmt.Fprintf(f, "COMMIT-WALK dir=%q entries=%d:\n", relPath, len(entries))
		for i := range entries {
			var kind string
			if entries[i].node != nil {
				kind = fmt.Sprintf("journal-%d", entries[i].node.Kind)
			} else {
				kind = "pxar"
			}
			fmt.Fprintf(f, "  [%d] %s name=%q offset=%d isDir=%v\n", i, kind, entries[i].name, entries[i].pxarOffset, entries[i].pxarSlim != nil && entries[i].pxarSlim.isDir)
		}
		f.Close()
	}

	// Process each merged entry.
	for i := range entries {
		if entries[i].node != nil {
			if err := ow.emitJournalEntry(&entries[i], relPath); err != nil {
				return err
			}
		} else {
			if err := ow.emitPxarEntry(&entries[i], relPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// emitJournalEntry writes a journal node to the archive.
func (ow *commitWalkState) emitJournalEntry(ce *commitEntry, parentRelPath string) error {
	node := ce.node
	childPath := joinPath(parentRelPath, ce.name)

	// Resolve xattrs.
	xattrs := ow.xattrCache[node.ID]
	meta := nodeToMetadata(node, xattrs)

	switch node.Kind {
	case NodeDir:
		// Determine pxar inode for this directory.
		var pxarChildIno uint64
		if node.RedirectTo != "" {
			// Node wraps a pxar directory — resolve its inode.
			if pnode := ow.mfs.findPxarNode(node.RedirectTo); pnode != nil {
				pxarChildIno = pnode.inode
			}
		} else if ce.pxarSlim != nil {
			// Journal node shadows a pxar entry at the same position.
			pxarChildIno = ce.pxarSlim.inode
		}

		if err := ow.writer.BeginDirectory(ce.name, &meta); err != nil {
			return fmt.Errorf("begin dir %s: %w", ce.name, err)
		}
		if err := ow.commitWalk(node.ID, pxarChildIno, childPath); err != nil {
			return err
		}
		if err := ow.writer.EndDirectory(); err != nil {
			return fmt.Errorf("end dir %s: %w", ce.name, err)
		}

	case NodeSymlink:
		entry := &pxar.Entry{
			Path:       ce.name,
			Kind:       pxar.KindSymlink,
			Metadata:   meta,
			LinkTarget: node.SymlinkTgt,
		}
		if err := ow.writer.WriteEntry(entry, nil); err != nil {
			return fmt.Errorf("write symlink %s: %w", ce.name, err)
		}

	case NodeFile:
		if node.HasData {
			return ow.emitBackedFile(node, ce.name, childPath, meta)
		}
		if node.RedirectTo != "" {
			return ow.emitRedirectedFile(node, ce.name, meta)
		}
		// Empty file (created via touch, no data).
		entry := &pxar.Entry{
			Path:     ce.name,
			Kind:     pxar.KindFile,
			Metadata: meta,
			FileSize: node.Size,
		}
		return ow.writer.WriteEntry(entry, nil)
	}

	return nil
}

// emitBackedFile uploads a new/modified file from the mutable dir.
func (ow *commitWalkState) emitBackedFile(node *GraphNode, name, childPath string, meta pxar.Metadata) error {
	abs := ow.mfs.mutablePath(childPath)
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("open backed file %s: %w", childPath, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat backed file %s: %w", childPath, err)
	}

	entry := &pxar.Entry{
		Path:     name,
		Kind:     pxar.KindFile,
		Metadata: meta,
		FileSize: uint64(fi.Size()),
	}

	hash := sha256.New()
	tee := io.TeeReader(f, hash)

	if err := ow.writer.WriteEntryReader(entry, tee, uint64(fi.Size())); err != nil {
		return fmt.Errorf("write backed file %s: %w", name, err)
	}

	var h [32]byte
	hash.Sum(h[:0])
	ow.backedHashes[childPath] = h
	ow.mutableFiles++

	if ow.prog != nil {
		ow.prog.AddFile(fi.Size())
	}
	return nil
}

// emitRedirectedFile writes a pxar chunk reference for a journal node that
// wraps original pxar content (possibly renamed or with metadata changes).
func (ow *commitWalkState) emitRedirectedFile(node *GraphNode, name string, meta pxar.Metadata) error {
	// Resolve the pxar entry at the redirect path to get payload offset.
	pxarEntry, err := resolvePxarEntry(ow.mfs, node.RedirectTo)
	if err != nil {
		return fmt.Errorf("resolve redirect %s for %s: %w", node.RedirectTo, name, err)
	}

	entry := &pxar.Entry{
		Path:     name,
		Kind:     pxar.KindFile,
		Metadata: meta,
		FileSize: node.Size,
	}
	if entry.FileSize == 0 {
		entry.FileSize = pxarEntry.FileSize
	}

	if err := ow.writer.WriteEntryRef(entry, pxarEntry.PayloadOffset); err != nil {
		return fmt.Errorf("write ref %s (redirect from %s): %w", name, node.RedirectTo, err)
	}
	return nil
}

// emitPxarEntry writes a pure pxar entry to the archive.
func (ow *commitWalkState) emitPxarEntry(ce *commitEntry, parentRelPath string) error {
	slim := ce.pxarSlim
	if slim == nil {
		return nil
	}

	// Read the full pxar entry.
	ow.mfs.pxar.readerMu.Lock()
	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
	ow.mfs.pxar.readerMu.Unlock()
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
	}

	childPath := joinPath(parentRelPath, ce.name)

	if pxarEntry.IsDir() {
		childIno := ToInode(pxarEntry)
		// Register so ReadDirRaw works on recursion.
		ow.mfs.pxar.RegisterSlimNode(slim, 0)

		meta := buildMetaFromPxarEntry(pxarEntry)
		if err := ow.writer.BeginDirectory(ce.name, &meta); err != nil {
			return fmt.Errorf("begin pxar dir %s: %w", ce.name, err)
		}
		// Pure pxar directory — no journal parent (pass 0).
		if err := ow.commitWalk(0, childIno, childPath); err != nil {
			return err
		}
		if err := ow.writer.EndDirectory(); err != nil {
			return fmt.Errorf("end pxar dir %s: %w", ce.name, err)
		}
		return nil
	}

	// Non-directory pxar entry — emit as chunk reference.
	clone := clonePxarEntry(pxarEntry, ce.name)
	if err := ow.writer.WriteEntryRef(clone, pxarEntry.PayloadOffset); err != nil {
		return fmt.Errorf("write pxar ref %s: %w", ce.name, err)
	}
	return nil
}

// --- helpers ---

// nodeToMetadata converts a journal GraphNode to pxar.Metadata.
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

// buildMetaFromPxarEntry extracts metadata from a pxar.Entry.
func buildMetaFromPxarEntry(e *pxar.Entry) pxar.Metadata {
	return pxar.Metadata{
		Stat:   e.Metadata.Stat,
		XAttrs: e.Metadata.XAttrs,
		FCaps:  e.Metadata.FCaps,
		ACL:    e.Metadata.ACL,
	}
}

// clonePxarEntry clones a pxar entry with a new name.
func clonePxarEntry(e *pxar.Entry, name string) *pxar.Entry {
	clone := *e
	clone.Path = name
	return &clone
}

// resolvePxarEntry walks the pxar archive to find the entry at relPath.
func resolvePxarEntry(mfs *MutableFS, relPath string) (*pxar.Entry, error) {
	if relPath == "/" || relPath == "" {
		mfs.pxar.readerMu.Lock()
		defer mfs.pxar.readerMu.Unlock()
		return mfs.pxar.Reader().ReadRoot()
	}

	parts := splitPath(relPath)
	curIno := RootInode

	for i, comp := range parts {
		entries, err := mfs.pxar.ReadDirRaw(curIno)
		if err != nil {
			return nil, fmt.Errorf("readdir ino %d: %w", curIno, err)
		}
		found := false
		for _, e := range entries {
			if e.name == comp {
				mfs.pxar.readerMu.Lock()
				pxarEntry, err := mfs.pxar.Reader().ReadEntryAt(int64(e.entryStart))
				mfs.pxar.readerMu.Unlock()
				if err != nil {
					return nil, fmt.Errorf("read entry at %d: %w", e.entryStart, err)
				}
				if i == len(parts)-1 {
					return pxarEntry, nil
				}
				if pxarEntry.IsDir() {
					curIno = ToInode(pxarEntry)
					found = true
					break
				}
				return nil, fmt.Errorf("%s is not a directory", comp)
			}
		}
		if !found {
			return nil, fmt.Errorf("component %q not found in ino %d", comp, curIno)
		}
	}
	return nil, fmt.Errorf("path %q not found", relPath)
}

// resolvePxarPayloadOffset resolves the payload offset for a pxar path.
// Returns 0 if not found.
func resolvePxarPayloadOffset(ow *commitWalkState, relPath string) uint64 {
	parts := splitPath(relPath)
	curIno := RootInode

	var target dirEntrySlim
	found := false

	for i, comp := range parts {
		entries, ok := ow.pxarDirCache[curIno]
		if !ok {
			var err error
			entries, err = ow.mfs.pxar.ReadDirRaw(curIno)
			if err != nil {
				return 0
			}
			ow.pxarDirCache[curIno] = entries
		}
		for _, e := range entries {
			if e.name == comp {
				if i == len(parts)-1 {
					target = e
					found = true
				} else {
					curIno = e.inode
				}
				break
			}
		}
	}

	if !found {
		return 0
	}

	if !target.isDir {
		return target.contentOffset
	}
	return minDescendantOffset(ow, target.inode)
}

// minDescendantOffset returns the minimum content offset among all regular
// file descendants of the pxar directory identified by inode.
func minDescendantOffset(ow *commitWalkState, pxarInode uint64) uint64 {
	entries, ok := ow.pxarDirCache[pxarInode]
	if !ok {
		entries, _ = ow.mfs.pxar.ReadDirRaw(pxarInode)
		ow.pxarDirCache[pxarInode] = entries
	}
	min := ^uint64(0)
	for i := range entries {
		e := &entries[i]
		if e.isDir {
			child := minDescendantOffset(ow, e.inode)
			if child < min {
				min = child
			}
		} else if e.isReg && e.contentOffset < min {
			min = e.contentOffset
		}
	}
	return min
}

// verifyBackedFileHashes checks that backed files haven't changed since upload.
func verifyBackedFileHashes(mfs *MutableFS, hashes map[string][32]byte) error {
	buf := make([]byte, 64*1024)
	for relPath, expected := range hashes {
		abs := mfs.mutablePath(relPath)
		f, err := os.Open(abs)
		if err != nil {
			return fmt.Errorf("open backed file %q for verification: %w", relPath, err)
		}
		h := sha256.New()
		_, err = io.CopyBuffer(h, f, buf)
		f.Close()
		if err != nil {
			return fmt.Errorf("hash backed file %q: %w", relPath, err)
		}
		var actual [32]byte
		h.Sum(actual[:0])
		if actual != expected {
			return fmt.Errorf("backed file %q content hash differs", relPath)
		}
	}
	return nil
}

// ensureNamespaceDir creates the namespace directory structure on the PBS
// datastore. PBS expects on-disk ns/<component>/ns/<component>/... directories
// owned by the backup user (uid/gid 34) so the PBS daemon can write to them.
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
		// PBS daemon runs as uid/gid 34 (backup user).
		_ = os.Chown(cur, 34, 34)
	}
	return nil
}

// postCommit hot-swaps the pxar reader and resets the journal + mutable dir.
func postCommit(mfs *MutableFS, backupID, backupType, namespace, archiveName string, backupTime int64) error {
	// Determine the new snapshot directory.
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

	// mmap new DIDX files.
	metaData, err := mmapFile(mpxarPath)
	if err != nil {
		return fmt.Errorf("mmap new mpxar: %w", err)
	}
	payloadData, err := mmapFile(ppxarPath)
	if err != nil {
		munmap(metaData)
		return fmt.Errorf("mmap new ppxar: %w", err)
	}

	store, err := datastore.NewChunkStore(mfs.pbsStore)
	if err != nil {
		munmap(metaData)
		munmap(payloadData)
		return fmt.Errorf("open chunk store: %w", err)
	}
	source := datastore.NewChunkStoreSource(store)

	newReader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		munmap(metaData)
		munmap(payloadData)
		return fmt.Errorf("create new reader: %w", err)
	}

	// Release previous mmap'd DIDX data.
	for _, d := range mfs.mmapData {
		munmap(d)
	}

	// Hot-swap the pxar reader.
	mfs.pxar.HotSwap(newReader)
	mfs.mmapData = nil
	if len(metaData) > 0 {
		mfs.mmapData = append(mfs.mmapData, metaData)
	}
	if len(payloadData) > 0 {
		mfs.mmapData = append(mfs.mmapData, payloadData)
	}

	// Update snapshot reference.
	mfs.origSnapshot = snapshotRef{
		BackupType:  backupType,
		BackupID:    backupID,
		BackupTime:  backupTime,
		Namespace:   namespace,
		ArchiveName: archiveName,
	}
	mfs.origPpxarDidx = ppxarPath

	// Clear the journal.
	if err := mfs.journal.Clear(); err != nil {
		return fmt.Errorf("clear journal: %w", err)
	}

	// Reset mutable dir — preserve journal directory.
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

// snapshotGroupDir returns the PBS snapshot group directory path.
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

// ParseOrigSnapshot extracts snapshot metadata from the original DIDX path.
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

// RunCommitSubcommand is the CLI entry point for `pxar-mount commit`.
func RunCommitSubcommand() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")
	pbsURL := fs.String("pbs-url", "", "PBS server URL")
	datastoreName := fs.String("datastore", "", "PBS datastore name")
	authToken := fs.String("token", "", "PBS API token")
	namespace := fs.String("ns", "", "PBS namespace")
	backupType := fs.String("backup-type", "host", "Backup type")
	backupID := fs.String("backup-id", "", "Backup ID")

	fs.Parse(os.Args[2:])

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  \u2717 error connecting to socket %s: %v\n", *socketPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s\n",
		*pbsURL, *datastoreName, *authToken, *namespace, *backupType, *backupID)
	if _, err := fmt.Fprint(conn, cmd); err != nil {
		fmt.Fprintf(os.Stderr, "  \u2717 error sending command: %v\n", err)
		os.Exit(1)
	}

	display := NewProgressDisplay(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Committing snapshot...\n")

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
