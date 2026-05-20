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

// commitMu serializes commit operations globally.
var commitMu sync.Mutex

// lastCommitTime tracks the last committed backup timestamp to ensure
// monotonically increasing timestamps. PBS rejects identical timestamps
// for the same backup group.
var lastCommitTime int64

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

// StartCommitListener listens on a Unix socket for commit requests and
// starts the commit monitor hub for progress broadcasting.
func StartCommitListener(sockPath string, mfs *MutableFS) (net.Listener, error) {
	_ = os.Remove(sockPath) // ignore error — may not exist
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sockPath, 0o660); err != nil {
		_ = l.Close()
		return nil, err
	}

	// Start the monitor hub for progress broadcasting.
	hub, err := newCommitHub(sockPath)
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

// handleCommitConn processes a single commit connection.
//
// Protocol:
//   - Client sends: COMMIT <url> <ds> <token> <ns> <type> <id>\n
//   - Client may send: DETACH\n after COMMIT to request background mode
//   - Daemon replies: JOB <id>\n  (if detached, then closes connection)
//   - Progress: PROGRESS <msg>\n  (streamed to both original conn and monitor)
//   - Terminal: OK <msg>\n  or  ERR <msg>\n
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

	// Check for DETACH flag on next line with a short deadline.
	// The client sends DETACH immediately after COMMIT if at all;
	// blocking here would deadlock the foreground (non-detached)
	// path where the client is already waiting for progress.
	detached := false
	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if scanner.Scan() {
		detached = scanner.Text() == "DETACH"
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear deadline

	if detached {
		// Start commit in background, send JOB id, close connection.
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
		// Foreground: stream progress to both the connection and the hub.
		globalCommitHub.startJob()
		defer globalCommitHub.endJob()

		primary := NewProgressReporter(conn)
		prog := &fanoutReporter{primary: primary, hub: globalCommitHub}
		if err := CommitSnapshot(mfs, req, prog); err != nil {
			prog.Error(err.Error())
			return
		}
	}
}

// --- Commit Core ---

// commitEntry represents one item in the merged directory view during commit walk.
type commitEntry struct {
	name     string
	node     *GraphNode    // journal node (nil for pure pxar)
	pxarSlim *dirEntrySlim // pxar slim entry (nil for journal-only)
	sortKey  uint64        // min descendant payload offset (dirs only)
}

// commitWalkState holds state for the recursive commit walk.
// All per-walk slices are pre-allocated once and reused across recursive
// calls via save/restore, eliminating heap allocation per directory level.
type commitWalkState struct {
	mfs          *MutableFS
	writer       transfer.ArchiveWriter
	prog         CommitProgress
	xattrCache   map[int64][]format.XAttr // journal node ID → xattrs
	backedHashes map[string]uint64        // relPath → xxh3 hash of uploaded files
	mutableFiles int                      // count of new/modified files

	// lastRefPayloadOffset tracks the highest payload offset emitted via
	// WriteEntryRef across the ENTIRE recursive walk. Before emitting any
	// payload ref, we check that the offset is strictly greater than this
	// value. If not, we fall back to re-encoding the file content from the
	// archive, which advances the encoder's payload position sequentially.
	//
	// This mirrors the Rust PBS client's try_record_reusable_offset +
	// re-encode pattern from pbs-client/src/pxar/create.rs, and prevents
	// the encoder's strict monotonic PAYLOAD_REF offset check from ever
	// firing (since we pre-validate before calling WriteEntryRef).
	lastRefPayloadOffset uint64
	hasLastRefPayload    bool

	// Reusable scratch slices — allocated once, cleared per-directory
	// via save/restore in commitWalk. Eliminates per-level heap allocation.
	refEntries     []commitEntry
	newDataEntries []commitEntry
	allEntries     []commitEntry
}

// CommitProgress is the interface used by CommitSnapshot to report progress.
// Both *ProgressReporter and the hub-based reporters implement it.
type CommitProgress interface {
	SetPhase(ProgressPhase)
	SetMsg(msg string)
	Done(msg string)
	Error(msg string)
	AddFile(bytes int64)
	SetTotals(files, bytes int64)
	State() ProgressState
}

// Ensure *ProgressReporter satisfies CommitProgress.
var _ CommitProgress = (*ProgressReporter)(nil)

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
func CommitSnapshot(mfs *MutableFS, req *CommitRequest, prog CommitProgress) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	// --- Phase 0: Freeze FUSE mutations ---
	mfs.freezeMu.Lock()
	mfs.frozen = true
	mfs.freezeMu.Unlock()

	// Sync journal while frozen — all pending metadata is durably
	// checkpointed before we start reading the journal for commit.
	// This is the write barrier equivalent: all metadata must be on
	// stable storage before we snapshot it, like ext4's journal flush
	// before checkpoint.
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
	if backupID == "" {
		// Init-mounted namespace with no previous snapshot — derive from hostname.
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
	// Ensure strictly increasing timestamps — PBS rejects identical
	// backup times for the same group ("timestamp too old").
	if now <= lastCommitTime {
		now = lastCommitTime + 1
	}
	lastCommitTime = now
	backupTime := now

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

	// --- Phase 3: Create dedup writer ---
	metaName := archiveName + ".mpxar.didx"
	payloadName := archiveName + ".ppxar.didx"

	var origPayloadIdx []byte
	if mfs.origPpxarDidx != "" {
		origPayloadIdx, _ = os.ReadFile(mfs.origPpxarDidx)
	}

	writer, err := transfer.NewRemoteDedupWriter(ctx, session, metaName, payloadName, origPayloadIdx)
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

	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		return fmt.Errorf("begin archive: %w", err)
	}

	// --- Phase 5: Walk merged tree ---
	prog.SetPhase(PhaseWalk)
	prog.SetMsg("Archiving files")

	ow := &commitWalkState{
		mfs:            mfs,
		writer:         writer,
		prog:           prog,
		xattrCache:     make(map[int64][]format.XAttr),
		backedHashes:   make(map[string]uint64),
		refEntries:     make([]commitEntry, 0, 64),
		newDataEntries: make([]commitEntry, 0, 16),
		allEntries:     make([]commitEntry, 0, 64),
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
	prog.SetMsg(fmt.Sprintf("Flushing upload (%d new/modified files)", ow.mutableFiles))

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
		if err := verifyBackedFileHashes(mfs, ow.backedHashes, prog); err != nil {
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
//
// Zero-allocation: uses save/restore on pre-allocated slices so no heap
// allocation occurs per directory level beyond what the journal/pxar
// libraries return.
func (ow *commitWalkState) commitWalk(journalParentID int64, pxarInode uint64, relPath string) error {
	// --- Save parent's slice state, reset for this level ---
	savedRef := ow.refEntries
	savedNew := ow.newDataEntries
	savedAll := ow.allEntries
	ow.refEntries = ow.refEntries[:0]
	ow.newDataEntries = ow.newDataEntries[:0]
	ow.allEntries = ow.allEntries[:0]

	// Restore on return so the parent level reuses the same backing array.
	defer func() {
		ow.refEntries = savedRef
		ow.newDataEntries = savedNew
		ow.allEntries = savedAll
	}()

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
		if len(whiteouts) > 0 {
			whiteoutSet = make(map[string]bool, len(whiteouts))
			for _, w := range whiteouts {
				whiteoutSet[w] = true
			}
		}
	}

	// Get pxar children (read on demand, no cache — avoids O(N) memory
	// blowup on large archives where N is the total entry count).
	var pxarEntries []dirEntrySlim
	if pxarInode != 0 {
		var err error
		pxarEntries, err = ow.mfs.pxar.ReadDirRaw(pxarInode)
		if err != nil {
			pxarEntries = nil
		}
	}

	// Build lookup set from journal edges — only allocate if non-empty.
	var edgeNames map[string]bool
	if len(journalEdges) > 0 {
		edgeNames = make(map[string]bool, len(journalEdges))
		for _, e := range journalEdges {
			edgeNames[e.Name] = true
		}
	}

	// --- Classify pxar entries directly into allEntries ---
	//
	// PBS archives are written depth-first with strictly ascending payload
	// offsets. Each directory's children occupy a contiguous, non-overlapping
	// range. GOODBYE hash ordering differs from payload ordering, so we
	// must sort FILE entries by contentOffset within each directory.
	//
	// We build allEntries in two passes (dirs+symlinks first, then files)
	// so the final sort sees dirs before files, matching depth-first order.
	//
	// NOTE: We intentionally do NOT recursively compute min-descendant
	// offsets. The recursive approach materializes the entire archive
	// tree into memory (O(N) where N = total entries), causing extreme
	// memory blowup on large archives. Instead, we rely on the
	// monotonicity fallback in emitPxarEntry which re-encodes any file
	// whose payload ref violates the strict ascending offset check.
	// This matches the Rust pxar client's try_record_reusable_offset +
	// re-encode pattern.

	for i := range pxarEntries {
		pe := &pxarEntries[i]
		if edgeNames != nil && edgeNames[pe.name] {
			continue
		}
		if whiteoutSet != nil && whiteoutSet[pe.name] {
			continue
		}
		if pe.isDir || pe.isSymlink {
			ow.allEntries = append(ow.allEntries, commitEntry{name: pe.name, pxarSlim: pe})
		}
	}
	for i := range pxarEntries {
		pe := &pxarEntries[i]
		if edgeNames != nil && edgeNames[pe.name] {
			continue
		}
		if whiteoutSet != nil && whiteoutSet[pe.name] {
			continue
		}
		if !pe.isDir && !pe.isSymlink {
			ow.allEntries = append(ow.allEntries, commitEntry{name: pe.name, pxarSlim: pe})
		}
	}

	// Compute sort keys and sort.
	for i := range ow.allEntries {
		e := &ow.allEntries[i]
		if e.pxarSlim.isDir || e.pxarSlim.isSymlink {
			e.sortKey = e.pxarSlim.entryStart
		} else {
			e.sortKey = e.pxarSlim.contentOffset
		}
	}
	sort.Slice(ow.allEntries, func(i, j int) bool {
		ei := &ow.allEntries[i]
		ej := &ow.allEntries[j]
		if ei.pxarSlim.isDir != ej.pxarSlim.isDir {
			return ei.pxarSlim.isDir
		}
		return ei.sortKey < ej.sortKey
	})

	if ow.mfs.verbose {
		hasDir, hasFile := false, false
		for _, e := range ow.allEntries {
			if e.pxarSlim != nil {
				if e.pxarSlim.isDir {
					hasDir = true
				} else if !e.pxarSlim.isSymlink {
					hasFile = true
				}
			}
		}
		if hasDir && hasFile {
			ow.mfs.debugf("commit-walk sort %q: %d entries", relPath, len(ow.allEntries))
			for i, e := range ow.allEntries {
				if e.pxarSlim == nil {
					continue
				}
				kind := "file"
				if e.pxarSlim.isDir {
					kind = "dir"
				} else if e.pxarSlim.isSymlink {
					kind = "sym"
				}
				ow.mfs.debugf("  [%d] %s %s sortKey=%d contentOffset=%d",
					i, kind, e.name, e.sortKey, e.pxarSlim.contentOffset)
			}
		}
	}

	// All pxar entries are refs (no new data).
	ow.refEntries = append(ow.refEntries, ow.allEntries...)

	// --- Classify journal edges by whether they emit new data ---
	//
	// Architectural decision: split entries into ref-emitters and
	// new-data-emitters. All refs (WriteEntryRef) must precede all
	// new-data writes (WriteEntryReader) because WriteEntryReader
	// advances the payload stream position past all existing ref
	// offsets, making subsequent refs fail the monotonicity check.
	for i := range journalEdges {
		edge := &journalEdges[i]
		node, err := ow.mfs.journal.GetNode(edge.ChildID)
		if err != nil || node == nil {
			continue
		}
		ce := commitEntry{name: edge.Name, node: node}
		if node.Kind == NodeFile && node.HasData {
			ow.newDataEntries = append(ow.newDataEntries, ce)
		} else {
			ow.refEntries = append(ow.refEntries, ce)
		}
	}

	// --- Emit all ref entries first, then all new-data entries ---
	if ow.mfs.verbose {
		ow.mfs.debugf("commit-walk %q: %d refEntries, %d newDataEntries (pxarInode=%d)",
			relPath, len(ow.refEntries), len(ow.newDataEntries), pxarInode)
		for i, e := range ow.refEntries {
			ow.mfs.debugf("  ref[%d] name=%q node=%v slim=%v", i, e.name, e.node != nil, e.pxarSlim != nil)
		}
		for i, e := range ow.newDataEntries {
			ow.mfs.debugf("  new[%d] name=%q node=%v slim=%v", i, e.name, e.node != nil, e.pxarSlim != nil)
		}
	}
	for i := range ow.refEntries {
		ce := &ow.refEntries[i]
		if ce.node != nil {
			if err := ow.emitJournalEntry(ce, relPath); err != nil {
				return err
			}
		} else {
			if err := ow.emitPxarEntry(ce, relPath); err != nil {
				return err
			}
		}
	}
	for i := range ow.newDataEntries {
		ce := &ow.newDataEntries[i]
		if ce.node != nil {
			if err := ow.emitJournalEntry(ce, relPath); err != nil {
				return err
			}
		} else {
			if err := ow.emitPxarEntry(ce, relPath); err != nil {
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

	ow.mfs.debugf("emit journal %q kind=%d", childPath, node.Kind)

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
			// Merge pxar-preserved fields (fcaps, acl, flags, quota)
			// into journal metadata so default ACLs and fcaps survive.
			if pxDirEntry, rerr := resolvePxarEntry(ow.mfs, node.RedirectTo); rerr == nil {
				meta = mergeMetaWithPxar(meta, pxDirEntry)
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
	defer func() { _ = f.Close() }()

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

	h := xxh3.New()
	tee := io.TeeReader(f, h)

	if err := ow.writer.WriteEntryReader(entry, tee, uint64(fi.Size())); err != nil {
		return fmt.Errorf("write backed file %s: %w", name, err)
	}

	ow.backedHashes[childPath] = h.Sum64()
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

	// Merge journal overrides (mode/uid/gid/mtime) with pxar-preserved
	// fields (fcaps, acl, flags, quota). Without this, fcaps and ACLs
	// are silently dropped on redirected entries.
	mergedMeta := mergeMetaWithPxar(meta, pxarEntry)

	entry := &pxar.Entry{
		Path:     name,
		Kind:     pxar.KindFile,
		Metadata: mergedMeta,
		FileSize: node.Size,
	}
	if entry.FileSize == 0 {
		entry.FileSize = pxarEntry.FileSize
	}

	return ow.tryRefOrReencode(entry, pxarEntry, name, node.RedirectTo)
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

	// Non-directory pxar entry.
	clone := clonePxarEntry(pxarEntry, ce.name)

	if pxarEntry.IsSymlink() {
		// Symlinks have no payload — write inline.
		if err := ow.writer.WriteEntry(clone, nil); err != nil {
			return fmt.Errorf("write pxar symlink %s: %w", ce.name, err)
		}
		return nil
	}

	// Regular file with payload — use tryRefOrReencode which checks global
	// monotonicity before attempting WriteEntryRef, falling back to re-encoding
	// from archive content when the offset would violate the strict ascending
	// offset invariant required by the pxar decoder.
	return ow.tryRefOrReencode(clone, pxarEntry, ce.name, "")
}

// tryRefOrReencode attempts to emit a file as a payload reference (dedup).
// If the payload offset would violate global monotonicity, it falls back to
// re-encoding the file content from the archive. This mirrors the Rust PBS
// client's try_record_reusable_offset + re-encode pattern.
//
// redirectPath is non-empty for journal redirect entries (for error messages).
func (ow *commitWalkState) tryRefOrReencode(entry *pxar.Entry, pxarEntry *pxar.Entry, name, redirectPath string) error {
	offset := pxarEntry.PayloadOffset

	// Pre-check: is the offset strictly greater than the last emitted ref?
	if ow.hasLastRefPayload && offset <= ow.lastRefPayloadOffset {
		ow.mfs.debugf("ref %s offset=%d <= lastRef=%d, re-encoding", name, offset, ow.lastRefPayloadOffset)
		return ow.reencodeFromArchive(pxarEntry, entry, name)
	}

	if err := ow.writer.WriteEntryRef(entry, offset); err != nil {
		// If the writer rejected the offset despite our pre-check (e.g. the
		// encoder's per-state-level check caught something we missed), fall
		// back to re-encoding.
		ow.mfs.debugf("ref %s offset=%d writer rejected: %v, re-encoding", name, offset, err)
		return ow.reencodeFromArchive(pxarEntry, entry, name)
	}

	ow.lastRefPayloadOffset = offset
	ow.hasLastRefPayload = true
	return nil
}

// reencodeFromArchive reads the original file content from the pxar archive
// and writes it as new data via WriteEntryReader, bypassing payload refs
// entirely. This advances the encoder's payload stream sequentially.
func (ow *commitWalkState) reencodeFromArchive(pxarEntry *pxar.Entry, entry *pxar.Entry, name string) error {
	ow.mfs.pxar.readerMu.Lock()
	rc, err := ow.mfs.pxar.reader.ReadFileContentReader(pxarEntry)
	ow.mfs.pxar.readerMu.Unlock()
	if err != nil {
		return fmt.Errorf("read pxar content for re-encode %s: %w", name, err)
	}
	defer func() { _ = rc.Close() }()
	if err := ow.writer.WriteEntryReader(entry, rc, pxarEntry.FileSize); err != nil {
		return fmt.Errorf("write re-encoded %s: %w", name, err)
	}
	return nil
}

// --- Helpers ---

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

// mergeMetaWithPxar merges journal node metadata with preserved pxar fields.
// Journal values take precedence for stat fields that the user may have
// modified (mode, uid, gid, mtime). Pxar values are preserved for fields
// the journal does not track: fcaps, acl, flags, and quota_project_id.
// This matches proxmox-backup-client behavior where metadata changes
// to a previously-backed-up file only affect the stat fields while
// fcaps/acl are read from the source filesystem on each backup.
func mergeMetaWithPxar(journalMeta pxar.Metadata, pxarEntry *pxar.Entry) pxar.Metadata {
	out := journalMeta
	out.Stat.Flags = pxarEntry.Metadata.Stat.Flags
	out.QuotaProjectID = pxarEntry.Metadata.QuotaProjectID
	out.FCaps = pxarEntry.Metadata.FCaps
	out.ACL = pxarEntry.Metadata.ACL
	return out
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

// xxh3Pool reuses xxh3 hashers across verify calls to avoid per-worker allocation.
var xxh3Pool = sync.Pool{
	New: func() any { return xxh3.New() },
}

// verifyBackedFileHashes checks that backed files haven't changed since upload.
// Uses a worker pool with atomic index to avoid channel/alloc overhead.
// Hashers are pooled via sync.Pool to eliminate per-file xxh3 allocation.
func verifyBackedFileHashes(mfs *MutableFS, hashes map[string]uint64, prog CommitProgress) error {
	workers := min(min(runtime.NumCPU(), 16), len(hashes))

	// Flatten map into a single contiguous slice — one allocation.
	// Use parallel flat layout: [relPath0, expected0, relPath1, expected1, ...]
	// to avoid struct padding overhead. But clarity wins here since this
	// is a one-time cost per commit.
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
				prog.SetMsg(fmt.Sprintf("Verifying backed files %d/%d (%d%%)", int(done), total, pct))
				lastProgress = time.Now()
			}
		}
	}

	// Use manual wg.Add + go instead of wg.Go to avoid closure allocation
	// per goroutine (wg.Go creates a wrapper closure internally).
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

	// Clear the journal first so concurrent reads during the swap window
	// see old-pxar + empty-journal (consistent) rather than new-pxar +
	// stale-journal (inconsistent edges).
	if err := mfs.journal.Clear(); err != nil {
		return fmt.Errorf("clear journal: %w", err)
	}
	if err := mfs.journal.Sync(); err != nil {
		return fmt.Errorf("sync journal after clear: %w", err)
	}

	// Release previous mmap'd DIDX data.
	for _, d := range mfs.mmapData {
		_ = munmap(d)
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

	// Clear stale in-memory state from the previous snapshot.
	mfs.resetAfterCommit()

	// Update snapshot reference.
	mfs.origSnapshot = snapshotRef{
		BackupType:  backupType,
		BackupID:    backupID,
		BackupTime:  backupTime,
		Namespace:   namespace,
		ArchiveName: archiveName,
	}
	mfs.origPpxarDidx = ppxarPath

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
	detach := fs.Bool("detach", false, "Run commit in background; use 'attach' to watch progress")

	fs.Parse(os.Args[2:]) //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *socketPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	// Resolve auth token: explicit flag > local token file.
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
		// Send DETACH so the daemon runs commit in background.
		if _, err := fmt.Fprintln(conn, "DETACH"); err != nil {
			fmt.Fprintf(os.Stderr, "  \u2717 error sending detach: %v\n", err)
			os.Exit(1)
		}

		// Read the JOB <id> response.
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

	// Foreground: stream progress directly from the connection.
	// On Ctrl+C, send DETACH so the daemon keeps running the commit in background.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	display := NewProgressDisplay(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Committing snapshot...\n")

	go func() {
		<-sigCh
		// Stop the spinner first.
		display.stop()
		// Send DETACH so the daemon switches to background mode.
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

// RunAttachSubcommand is the CLI entry point for `pxar-mount attach`.
// It connects to the commit monitor socket and streams live progress.
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

	// Process the first line we already read.
	for processMonitorLine(first, display) {
		if !scanner.Scan() {
			break
		}
	}
}

// processMonitorLine handles a single line from the monitor socket.
// Returns false if the connection should be closed (OK/ERR).
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
	// Catch-up lines from before we connected.
	fmt.Fprintf(os.Stderr, "  %s\n", line)
	return true
}

// RunLogsSubcommand is the CLI entry point for `pxar-mount logs`.
// It reads and displays the log file from the last commit.
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
