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
		prog := &fanoutReporter{primary: primary, hub: globalCommitHub, started: time.Now()}
		if err := CommitSnapshot(mfs, req, prog); err != nil {
			prog.Error(err.Error())
			return
		}
	}
}

// --- Commit Core ---

// commitEntry represents one item in the merged directory view during commit walk.
type commitEntry struct {
	name        string
	node        *GraphNode    // journal node (nil for pure pxar)
	pxarSlim    *dirEntrySlim // pxar slim entry (nil for journal-only)
	sortKey     uint64        // min descendant payload offset (dirs only)
	cachedEntry *pxar.Entry   // eagerly read pxar entry (avoids duplicate ReadEntryAt)
}

// deferredDir holds the minimal data needed to recurse into a subdirectory
// after the parent's pxarEntries slice has been released.  This breaks the
// memory retention chain: pxarEntries → backing array → all parent entries
// alive during all child commitWalk calls.
//
// For a root with 5M files, each deferredDir is ~56 bytes vs the ~440MB
// pxarEntries slice that would otherwise stay on the stack.
type deferredDir struct {
	name       string     // directory name (copied from pxarEntries[i].name)
	node       *GraphNode // non-nil for journal dirs
	entryStart uint64     // pxar entry offset (0 for journal-only dirs)
	pxarIno    uint64     // pxar child inode for recursion
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

	// redirectCache caches resolved pxar entries by redirect path to avoid
	// redundant resolvePxarEntry calls. Keys are journal RedirectTo paths;
	// values are the resolved pxar.Entry. The commit walk is single-threaded
	// and read-only, so a plain map without synchronization is safe.
	redirectCache map[string]*pxar.Entry

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
	pendingRefs []commitEntry // ref-files batched for contentOffset-ordered emission

	// Reusable pxar.Entry buffer — avoids heap allocation on every emit.
	entryBuf pxar.Entry
	pathBuf  strings.Builder
}

// allocEntry returns a pointer to the reusable entry buffer after zeroing it.
// All emit functions use this instead of &pxar.Entry{...} to avoid heap allocation.
func (ow *commitWalkState) allocEntry() *pxar.Entry {
	ow.entryBuf = pxar.Entry{}
	return &ow.entryBuf
}

// buildPath joins parent and name using the reusable path builder.
func (ow *commitWalkState) buildPath(parent, name string) string {
	ow.pathBuf.Reset()
	ow.pathBuf.WriteString(parent)
	if parent != "/" {
		ow.pathBuf.WriteByte('/')
	}
	ow.pathBuf.WriteString(name)
	return ow.pathBuf.String()
}

// maxPendingRefs bounds the pending ref batch to limit memory on
// directories with millions of entries. When exceeded, the batch is
// flushed immediately. Mirrors proxmox-backup-client's max_cache_size.
const maxPendingRefs = 4096

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
		mfs:           mfs,
		writer:        writer,
		prog:          prog,
		xattrCache:    make(map[int64][]format.XAttr),
		backedHashes:  make(map[string]uint64),
		redirectCache: make(map[string]*pxar.Entry),
		pendingRefs:   make([]commitEntry, 0, 64),
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
// Entry ordering matches proxmox-backup-client: directory entries are written
// in alphabetical order. Payload refs are batched and emitted in contentOffset
// order to satisfy the encoder's strict monotonic offset invariant, mirroring
// the Rust client's lookahead cache (pbs-client/src/pxar/look_ahead_cache.rs).
//
// Zero-allocation: uses save/restore on pre-allocated slices so no heap
// allocation occurs per directory level beyond what the journal/pxar
// libraries return.
func (ow *commitWalkState) commitWalk(journalParentID int64, pxarInode uint64, relPath string) error {
	// --- Save parent's pendingRefs, reset for this level ---
	savedPending := ow.pendingRefs
	ow.pendingRefs = ow.pendingRefs[:0]
	defer func() { ow.pendingRefs = savedPending }()

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

	// Get pxar children sorted alphabetically (matching proxmox-backup-client).
	var pxarEntries []dirEntrySlim
	if pxarInode != 0 {
		var err error
		pxarEntries, err = ow.mfs.pxar.ReadDirRaw(pxarInode)
		if err != nil {
			pxarEntries = nil
		}
	}

	// Build edgeNames set for O(1) pxar shadowing check.
	var edgeNames map[string]bool
	if len(journalEdges) > 0 {
		edgeNames = make(map[string]bool, len(journalEdges))
		for _, e := range journalEdges {
			edgeNames[e.Name] = true
		}
	}

	// --- Pre-filter pxar entries and sort both lists ---
	//
	// Filter pxar entries shadowed by journal edges or whiteouts.
	// Register slim nodes for inode lookup during recursion.
	filtered := 0
	for i := range pxarEntries {
		pe := &pxarEntries[i]
		if edgeNames != nil && edgeNames[pe.name] {
			continue
		}
		if whiteoutSet != nil && whiteoutSet[pe.name] {
			continue
		}
		if filtered != i {
			pxarEntries[filtered] = *pe
		}
		filtered++
	}
	pxarEntries = pxarEntries[:filtered]

	// Sort both lists alphabetically for two-pointer merge.
	sort.Slice(pxarEntries, func(i, j int) bool {
		return pxarEntries[i].name < pxarEntries[j].name
	})
	sort.Slice(journalEdges, func(i, j int) bool {
		return journalEdges[i].Name < journalEdges[j].Name
	})

	// --- Two-pointer merge: walk alphabetically, emitting incrementally ---
	//
	// Directories are collected into deferredDirs instead of recursing inline.
	// After the loop completes, pxarEntries is released (nil'd), THEN we
	// recurse into deferred directories. This breaks the memory retention
	// chain where pxarEntries stays on the stack during all child commitWalk
	// calls — the root cause of OOM on large archives with millions of files.
	//
	// deferredDirs only holds ~56 bytes per directory vs ~88 bytes per entry
	// in pxarEntries. For a root with 5M files and 10K dirs, that's ~560KB
	// vs ~440MB.
	var deferredDirs []deferredDir

	pi, ji := 0, 0
	for pi < len(pxarEntries) || ji < len(journalEdges) {
		var ce commitEntry
		if pi >= len(pxarEntries) {
			// Journal only.
			edge := &journalEdges[ji]
			node, _ := ow.mfs.journal.GetNode(edge.ChildID)
			if node == nil {
				ji++
				continue
			}
			ce = commitEntry{name: edge.Name, node: node}
			ji++
		} else if ji >= len(journalEdges) {
			// Pxar only.
			ce = commitEntry{name: pxarEntries[pi].name, pxarSlim: &pxarEntries[pi]}
			pi++
		} else if pxarEntries[pi].name < journalEdges[ji].Name {
			ce = commitEntry{name: pxarEntries[pi].name, pxarSlim: &pxarEntries[pi]}
			pi++
		} else if pxarEntries[pi].name > journalEdges[ji].Name {
			edge := &journalEdges[ji]
			node, _ := ow.mfs.journal.GetNode(edge.ChildID)
			if node == nil {
				ji++
				continue
			}
			ce = commitEntry{name: edge.Name, node: node}
			ji++
		} else {
			// Same name: journal takes priority, consume both.
			edge := &journalEdges[ji]
			node, _ := ow.mfs.journal.GetNode(edge.ChildID)
			if node != nil {
				ce = commitEntry{name: edge.Name, node: node}
			}
			pi++
			ji++
			if ce.node == nil {
				continue
			}
		}

		// Directories are collected for deferred recursion (after releasing
		// pxarEntries). Non-directories are emitted inline.
		isDir := (ce.node != nil && ce.node.Kind == NodeDir) ||
			(ce.node == nil && ce.pxarSlim != nil && ce.pxarSlim.isDir)

		if isDir {
			// Flush pending refs to maintain emission order (refs before dir).
			if err := ow.flushPendingRefs(relPath); err != nil {
				return err
			}

			// Collect minimal info — no pxarEntries reference retained.
			dd := deferredDir{name: ce.name}
			if ce.node != nil {
				dd.node = ce.node
				if ce.pxarSlim != nil {
					dd.pxarIno = ce.pxarSlim.inode
				}
			} else {
				dd.entryStart = ce.pxarSlim.entryStart
			}
			deferredDirs = append(deferredDirs, dd)
		} else {
			// Non-directory: emit inline.
			var err error
			if ce.node != nil {
				err = ow.emitAlphabeticalJournal(&ce, relPath)
			} else {
				err = ow.emitAlphabeticalPxar(&ce, relPath)
			}
			if err != nil {
				return err
			}
		}
	}

	// Flush any remaining batched refs.
	if err := ow.flushPendingRefs(relPath); err != nil {
		return err
	}

	// --- Release pxarEntries before recursing into subdirectories ---
	//
	// This is the key memory optimization: the parent's large entries slice
	// is no longer needed (merge loop is done), so we nil it to allow GC
	// before descending into any child directory's commitWalk.
	pxarEntries = nil
	_ = pxarEntries // suppress linter

	// --- Deferred directory recursion ---
	for i := range deferredDirs {
		if err := ow.processDeferredDir(&deferredDirs[i], relPath); err != nil {
			return err
		}
	}

	return nil
}

// emitAlphabeticalJournal emits a journal entry during alphabetical walk.
// Directories and new-data files flush pending refs and emit inline.
// Ref-eligible entries (redirected files, symlinks, empty files) are deferred
// to the pendingRefs batch where they are sorted by contentOffset.
func (ow *commitWalkState) emitAlphabeticalJournal(ce *commitEntry, parentRelPath string) error {
	node := ce.node

	switch node.Kind {
	case NodeDir:
		if err := ow.flushPendingRefs(parentRelPath); err != nil {
			return err
		}
		return ow.emitJournalDir(ce, parentRelPath)

	case NodeFile:
		if node.HasData {
			// New/modified data — flush pending refs, emit inline.
			if err := ow.flushPendingRefs(parentRelPath); err != nil {
				return err
			}
			childPath := ow.buildPath(parentRelPath, ce.name)
			xattrs := ow.xattrCache[node.ID]
			meta := nodeToMetadata(node, xattrs)
			// Merge pxar fcaps/acl if this file was copied up from pxar.
			if node.RedirectTo != "" {
				if pxEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
					meta = mergeMetaWithPxar(meta, pxEntry)
				}
			}
			return ow.emitBackedFile(node, ce.name, childPath, meta)
		}
		if node.RedirectTo != "" {
			// Redirected file — defer to pending refs for offset ordering.
			if err := ow.addToPendingRefs(ce, parentRelPath); err != nil {
				return err
			}
			return nil
		}
		// Empty file (touch, no data) — flush and emit inline.
		if err := ow.flushPendingRefs(parentRelPath); err != nil {
			return err
		}
		xattrs := ow.xattrCache[node.ID]
		meta := nodeToMetadata(node, xattrs)
		entry := ow.allocEntry()
		entry.Path = ce.name
		entry.Kind = pxar.KindFile
		entry.Metadata = meta
		entry.FileSize = node.Size
		return ow.writer.WriteEntry(entry, nil)

	case NodeSymlink:
		if err := ow.flushPendingRefs(parentRelPath); err != nil {
			return err
		}
		xattrs := ow.xattrCache[node.ID]
		meta := nodeToMetadata(node, xattrs)
		if node.RedirectTo != "" {
			if pxEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
				meta = mergeMetaWithPxar(meta, pxEntry)
			}
		}
		entry := ow.allocEntry()
		entry.Path = ce.name
		entry.Kind = pxar.KindSymlink
		entry.Metadata = meta
		entry.LinkTarget = node.SymlinkTgt
		return ow.writer.WriteEntry(entry, nil)
	}
	return nil
}

// emitAlphabeticalPxar emits a pxar-only entry during alphabetical walk.
// Directories flush pending refs and recurse. Symlinks flush and emit inline.
// Regular files are deferred to the pendingRefs batch for contentOffset ordering.
func (ow *commitWalkState) emitAlphabeticalPxar(ce *commitEntry, parentRelPath string) error {
	slim := ce.pxarSlim
	if slim == nil {
		return nil
	}

	if slim.isDir {
		if err := ow.flushPendingRefs(parentRelPath); err != nil {
			return err
		}
		return ow.emitPxarDir(ce, parentRelPath)
	}

	if slim.isSymlink {
		if err := ow.flushPendingRefs(parentRelPath); err != nil {
			return err
		}
		return ow.emitPxarSymlink(ce, parentRelPath)
	}

	// Regular file — eagerly read the full entry now and cache it in
	// commitEntry so emitPxarRef can reuse it without a duplicate ReadEntryAt.
	ow.mfs.pxar.readerMu.Lock()
	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
	ow.mfs.pxar.readerMu.Unlock()
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
	}
	ce.cachedEntry = pxarEntry

	// Use payloadOffset from pxarEntry for sortKey (same as contentOffset in v2).
	ce.sortKey = pxarEntry.PayloadOffset

	return ow.addToPendingRefs(ce, parentRelPath)
}

// --- Pending ref batching ---

// addToPendingRefs adds a ref-eligible entry to the pending batch,
// maintaining the batch sorted by contentOffset. Flushes the batch
// automatically when it reaches maxPendingRefs to bound memory on
// directories with millions of entries.
func (ow *commitWalkState) addToPendingRefs(ce *commitEntry, parentRelPath string) error {
	// Compute sort key: payloadOffset for files, entryStart for non-files.
	// For pxar entries, sortKey was already set by emitAlphabeticalPxar.
	if ce.pxarSlim != nil {
		if ce.pxarSlim.isReg {
			if ce.sortKey == 0 {
				ce.sortKey = ce.pxarSlim.payloadOffset
			}
		} else {
			ce.sortKey = ce.pxarSlim.entryStart
		}
	} else {
		// Journal redirect entry — use cached resolve to get sort key.
		if ce.node != nil && ce.node.RedirectTo != "" {
			if pxEntry, err := ow.resolvePxarEntryCached(ce.node.RedirectTo); err == nil {
				ce.sortKey = pxEntry.PayloadOffset
			} else {
				ce.sortKey = 0
			}
		}
	}
	ow.pendingRefs = append(ow.pendingRefs, *ce)

	// Flush if batch exceeds limit — bounds memory on large directories.
	if len(ow.pendingRefs) >= maxPendingRefs {
		return ow.flushPendingRefs(parentRelPath)
	}
	return nil
}

// flushPendingRefs emits all batched ref entries sorted by contentOffset,
// then clears the batch. This ensures payload ref monotonicity before any
// new data write or directory boundary.
func (ow *commitWalkState) flushPendingRefs(parentRelPath string) error {
	if len(ow.pendingRefs) == 0 {
		return nil
	}

	// Sort pending refs by contentOffset for ascending payload ref order.
	sort.Slice(ow.pendingRefs, func(i, j int) bool {
		return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
	})

	for i := range ow.pendingRefs {
		ce := &ow.pendingRefs[i]
		var err error
		if ce.node != nil {
			err = ow.emitJournalRef(ce, parentRelPath)
		} else {
			err = ow.emitPxarRef(ce, parentRelPath)
		}
		if err != nil {
			return err
		}
	}
	ow.pendingRefs = ow.pendingRefs[:0]
	return nil
}

// processDeferredDir handles deferred directory recursion after pxarEntries
// has been released. It performs the same work as emitJournalDir/emitPxarDir
// but uses only the minimal data captured in deferredDir.
func (ow *commitWalkState) processDeferredDir(dd *deferredDir, parentRelPath string) error {
	childPath := ow.buildPath(parentRelPath, dd.name)

	if dd.node != nil {
		// Journal directory (may have pxar shadow).
		node := dd.node
		xattrs := ow.xattrCache[node.ID]
		meta := nodeToMetadata(node, xattrs)

		var pxarChildIno uint64
		if node.RedirectTo != "" {
			if pxDirEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
				meta = mergeMetaWithPxar(meta, pxDirEntry)
				pxarChildIno = ToInode(pxDirEntry)
			}
		} else if dd.pxarIno != 0 {
			pxarChildIno = dd.pxarIno
		}

		if err := ow.writer.BeginDirectory(dd.name, &meta); err != nil {
			return fmt.Errorf("begin dir %q: %w", dd.name, err)
		}
		if err := ow.commitWalk(node.ID, pxarChildIno, childPath); err != nil {
			return err
		}
		return ow.writer.EndDirectory()
	}

	// Pxar-only directory — re-read the entry to get metadata.
	ow.mfs.pxar.readerMu.Lock()
	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(dd.entryStart))
	ow.mfs.pxar.readerMu.Unlock()
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", dd.entryStart, err)
	}

	childIno := ToInode(pxarEntry)
	meta := buildMetaFromPxarEntry(pxarEntry)

	if err := ow.writer.BeginDirectory(dd.name, &meta); err != nil {
		return fmt.Errorf("begin pxar dir %q: %w", dd.name, err)
	}
	if err := ow.commitWalk(0, childIno, childPath); err != nil {
		return err
	}
	return ow.writer.EndDirectory()
}

// emitJournalRef emits a journal ref entry (redirected file) from the pending batch.
func (ow *commitWalkState) emitJournalRef(ce *commitEntry, parentRelPath string) error {
	node := ce.node
	xattrs := ow.xattrCache[node.ID]
	meta := nodeToMetadata(node, xattrs)

	// Resolve pxar entry for payload offset and metadata merge (cached).
	pxarEntry, err := ow.resolvePxarEntryCached(node.RedirectTo)
	if err != nil {
		return fmt.Errorf("resolve redirect %q for %q: %w", node.RedirectTo, ce.name, err)
	}
	mergedMeta := mergeMetaWithPxar(meta, pxarEntry)

	entry := ow.allocEntry()
	entry.Path = ce.name
	entry.Kind = pxar.KindFile
	entry.Metadata = mergedMeta
	entry.FileSize = node.Size
	if entry.FileSize == 0 {
		entry.FileSize = pxarEntry.FileSize
	}

	return ow.tryRefOrReencode(entry, pxarEntry, ce.name, node.RedirectTo)
}

// emitPxarRef emits a pxar ref entry (regular file payload ref) from the pending batch.
// Uses ce.cachedEntry which was eagerly read in emitAlphabeticalPxar, avoiding
// a duplicate ReadEntryAt for every pxar-only file.
func (ow *commitWalkState) emitPxarRef(ce *commitEntry, parentRelPath string) error {
	slim := ce.pxarSlim
	if slim == nil {
		return nil
	}

	// Reuse the entry that was eagerly read during emitAlphabeticalPxar.
	pxarEntry := ce.cachedEntry
	if pxarEntry == nil {
		// Fallback: should not happen with the eager-read path, but handle gracefully.
		ow.mfs.pxar.readerMu.Lock()
		var err error
		pxarEntry, err = ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
		ow.mfs.pxar.readerMu.Unlock()
		if err != nil {
			return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
		}
	}

	clone := ow.clonePxarEntryBuf(pxarEntry, ce.name)
	return ow.tryRefOrReencode(clone, pxarEntry, ce.name, "")
}

// emitJournalDir emits a journal directory entry during alphabetical walk.
func (ow *commitWalkState) emitJournalDir(ce *commitEntry, parentRelPath string) error {
	node := ce.node
	childPath := ow.buildPath(parentRelPath, ce.name)
	xattrs := ow.xattrCache[node.ID]
	meta := nodeToMetadata(node, xattrs)

	var pxarChildIno uint64
	if node.RedirectTo != "" {
		if pxDirEntry, rerr := ow.resolvePxarEntryCached(node.RedirectTo); rerr == nil {
			meta = mergeMetaWithPxar(meta, pxDirEntry)
			pxarChildIno = ToInode(pxDirEntry)
		}
	} else if ce.pxarSlim != nil {
		pxarChildIno = ce.pxarSlim.inode
	}

	if err := ow.writer.BeginDirectory(ce.name, &meta); err != nil {
		return fmt.Errorf("begin dir %q: %w", ce.name, err)
	}
	if err := ow.commitWalk(node.ID, pxarChildIno, childPath); err != nil {
		return err
	}
	return ow.writer.EndDirectory()
}

// emitPxarDir emits a pure pxar directory entry during alphabetical walk.
func (ow *commitWalkState) emitPxarDir(ce *commitEntry, parentRelPath string) error {
	slim := ce.pxarSlim
	childPath := ow.buildPath(parentRelPath, ce.name)

	ow.mfs.pxar.readerMu.Lock()
	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
	ow.mfs.pxar.readerMu.Unlock()
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
	}

	childIno := ToInode(pxarEntry)
	meta := buildMetaFromPxarEntry(pxarEntry)

	if err := ow.writer.BeginDirectory(ce.name, &meta); err != nil {
		return fmt.Errorf("begin pxar dir %q: %w", ce.name, err)
	}
	if err := ow.commitWalk(0, childIno, childPath); err != nil {
		return err
	}
	return ow.writer.EndDirectory()
}

// emitPxarSymlink emits a pure pxar symlink entry during alphabetical walk.
func (ow *commitWalkState) emitPxarSymlink(ce *commitEntry, parentRelPath string) error {
	slim := ce.pxarSlim

	ow.mfs.pxar.readerMu.Lock()
	pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(slim.entryStart))
	ow.mfs.pxar.readerMu.Unlock()
	if err != nil {
		return fmt.Errorf("read pxar entry at %d: %w", slim.entryStart, err)
	}

	clone := ow.clonePxarEntryBuf(pxarEntry, ce.name)
	return ow.writer.WriteEntry(clone, nil)
}

// emitBackedFile uploads a new/modified file from the mutable dir.
func (ow *commitWalkState) emitBackedFile(node *GraphNode, name, childPath string, meta pxar.Metadata) error {
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
		ow.mfs.debugf("ref %q offset=%d <= lastRef=%d, re-encoding", name, offset, ow.lastRefPayloadOffset)
		return ow.reencodeFromArchive(pxarEntry, entry, name)
	}

	if err := ow.writer.WriteEntryRef(entry, offset); err != nil {
		// If the writer rejected the offset despite our pre-check (e.g. the
		// encoder's per-state-level check caught something we missed), fall
		// back to re-encoding.
		ow.mfs.debugf("ref %q offset=%d writer rejected: %v, re-encoding", name, offset, err)
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
		return fmt.Errorf("read pxar content for re-encode %q: %w", name, err)
	}
	defer func() { _ = rc.Close() }()
	if err := ow.writer.WriteEntryReader(entry, rc, pxarEntry.FileSize); err != nil {
		return fmt.Errorf("write re-encoded %q: %w", name, err)
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

// clonePxarEntryBuf clones a pxar entry into the reusable buffer.
func (ow *commitWalkState) clonePxarEntryBuf(e *pxar.Entry, name string) *pxar.Entry {
	ow.entryBuf = *e
	ow.entryBuf.Path = name
	return &ow.entryBuf
}

// resolvePxarEntryCached resolves a pxar entry by redirect path, caching the result
// in redirectCache. Intermediate path components use dirEntrySlim.isDir + inode from
// ReadDirRaw, avoiding per-component ReadEntryAt calls. Only the terminal component
// triggers a full ReadEntryAt, and only on cache miss.
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

// resolvePxarEntryUncached walks the pxar archive to find the entry at relPath.
// Uses ReadDirRaw + dirEntrySlim fields for intermediate components, avoiding
// per-component ReadEntryAt. Only calls ReadEntryAt for the terminal component.
func (ow *commitWalkState) resolvePxarEntryUncached(relPath string) (*pxar.Entry, error) {
	if relPath == "/" || relPath == "" {
		ow.mfs.pxar.readerMu.Lock()
		defer ow.mfs.pxar.readerMu.Unlock()
		return ow.mfs.pxar.Reader().ReadRoot()
	}

	parts := splitPath(relPath)
	curIno := RootInode

	for i, comp := range parts {
		entries, err := ow.mfs.pxar.ReadDirRaw(curIno)
		if err != nil {
			return nil, fmt.Errorf("readdir ino %d: %w", curIno, err)
		}
		found := false
		for _, e := range entries {
			if e.name == comp {
				if i == len(parts)-1 {
					// Terminal component: full ReadEntryAt.
					ow.mfs.pxar.readerMu.Lock()
					pxarEntry, err := ow.mfs.pxar.Reader().ReadEntryAt(int64(e.entryStart))
					ow.mfs.pxar.readerMu.Unlock()
					if err != nil {
						return nil, fmt.Errorf("read entry at %d: %w", e.entryStart, err)
					}
					return pxarEntry, nil
				}
				// Intermediate component: use dirEntrySlim fields (no ReadEntryAt).
				if !e.isDir {
					return nil, fmt.Errorf("%q is not a directory", comp)
				}
				curIno = e.inode
				found = true
				break
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
				prog.SetMsg(fmt.Sprintf("%s (%d/%d, %d%%)", it.relPath, int(done), total, pct))
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
