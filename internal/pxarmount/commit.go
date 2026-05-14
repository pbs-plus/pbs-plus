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
	"syscall"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

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
		return nil, fmt.Errorf("invalid COMMIT format: expected COMMIT [url] [datastore] [token] [ns] [type] [id]")
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

// commitMu serializes commit operations.
var commitMu sync.Mutex

// overlayWalk tracks state across the recursive walk.
type overlayWalk struct {
	writer       transfer.ArchiveWriter
	dedup        *transfer.RemoteDedupSplitArchiveWriter
	backedHashes map[string][32]byte       // relPath → SHA256 of uploaded backed files
	pxarDirCache map[uint64][]dirEntrySlim // pxar inode → cached dir entries
	backedDirs   map[string][]string       // parent relPath → backed child names
	progress     *ProgressReporter         // optional progress reporting
}

// StartSocketListener opens a Unix domain socket and processes commit requests.
func (fs *PassthroughFS) StartSocketListener(sockPath string) (net.Listener, chan struct{}, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", sockPath, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer l.Close()
		defer os.Remove(sockPath)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go fs.handleSocketConn(conn)
		}
	}()
	return l, done, nil
}

func (fs *PassthroughFS) handleSocketConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		req, err := ParseCommitLine(line)
		if err != nil {
			fmt.Fprintf(conn, "ERR %v\n", err)
			return
		}
		prog := NewProgressReporter(conn)
		if err := fs.commitOverlay(req, prog); err != nil {
			prog.Error(err.Error())
			return
		}
		return
	}
}

func (fs *PassthroughFS) commitOverlay(req *CommitRequest, prog *ProgressReporter) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	orig := fs.origSnapshot

	pbsURL := req.PBSURL
	if pbsURL == "" {
		pbsURL = "https://localhost:8007/api2/json"
		req.SkipTLS = true
	}
	datastoreName := req.Datastore
	if datastoreName == "" {
		datastoreName = ResolveDatastoreName(fs.pbsStore)
	}
	authToken := req.AuthToken
	if authToken == "" {
		authToken = ReadLocalToken()
	}

	backupID := req.BackupID
	if backupID == "" {
		backupID = orig.BackupID
	}
	backupType := req.BackupType
	if backupType == "" {
		backupType = orig.BackupType
	}
	namespace := req.Namespace
	if namespace == "" && orig.Namespace != "" {
		namespace = orig.Namespace
	}
	archiveName := orig.ArchiveName
	if archiveName == "" {
		archiveName = backupID
	}
	backupTime := time.Now().Unix()

	bt, err := datastore.ParseBackupType(backupType)
	if err != nil {
		return fmt.Errorf("invalid backup type %q: %w", backupType, err)
	}

	var prev *backupproxy.PreviousBackupRef
	if orig.BackupID != "" && orig.BackupTime > 0 {
		prev = &backupproxy.PreviousBackupRef{
			BackupType: bt,
			BackupID:   orig.BackupID,
			BackupTime: orig.BackupTime,
			Namespace:  orig.Namespace,
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

	prog.SetPhase(PhasePrepare)

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
	if fs.origPpxarDidx != "" {
		origPayloadIdx, _ = os.ReadFile(fs.origPpxarDidx)
	}

	writer, err := transfer.NewRemoteDedupSplitArchiveWriter(ctx, session, metaName, payloadName, origPayloadIdx)
	if err != nil {
		return fmt.Errorf("create dedup writer: %w", err)
	}

	rootPxarEntry := fs.getPxarEntry(RootInode)
	rootMeta := buildMetaFromEntry(rootPxarEntry)

	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		return fmt.Errorf("begin archive: %w", err)
	}

	prog.SetPhase(PhaseWalk)
	prog.SetMsg(fmt.Sprintf("Scanning overlay (backed: %d dirs)", len(fs.preWalkBackingDir())))

	ow := &overlayWalk{
		writer:       writer,
		dedup:        writer,
		backedHashes: make(map[string][32]byte),
		pxarDirCache: make(map[uint64][]dirEntrySlim),
		backedDirs:   fs.preWalkBackingDir(),
		progress:     prog,
	}
	if err := fs.walkOverlay(ow, RootInode, "/"); err != nil {
		return fmt.Errorf("walk overlay: %w", err)
	}

	prog.SetPhase(PhaseUpload)
	prog.SetMsg("Uploading to PBS")

	// Sync transaction log before finalizing upload.
	if fs.txnLog != nil {
		if err := fs.txnLog.Sync(); err != nil {
			return fmt.Errorf("sync transaction log: %w", err)
		}
	}

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}

	if _, err = session.Finish(ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}

	prog.SetPhase(PhaseVerify)
	prog.SetMsg(fmt.Sprintf("Verifying %d backed files", len(ow.backedHashes)))

	// Verify backed files using upload-time hashes (no full re-read).
	if len(ow.backedHashes) > 0 {
		if err := fs.verifyBackedFileHashes(ow.backedHashes); err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
	}

	prog.SetPhase(PhaseFinalize)
	prog.SetMsg("Swapping snapshot")

	if err := fs.postCommit(backupID, backupType, namespace, archiveName, backupTime); err != nil {
		return err
	}
	prog.Done(fmt.Sprintf("committed %s/%s", namespace, backupID))
	return nil
}

func (fs *PassthroughFS) postCommit(backupID, backupType, namespace, archiveName string, backupTime int64) error {
	var groupDir string
	if fs.origPpxarDidx != "" {
		origDir := filepath.Dir(fs.origPpxarDidx)
		groupDir = filepath.Dir(origDir)
	} else {
		groupDir = fs.snapshotGroupDir(backupType, backupID, namespace)
	}

	newTimeISO := time.Unix(backupTime, 0).UTC().Format("2006-01-02T15:04:05Z")
	snapDir := filepath.Join(groupDir, newTimeISO)

	mpxarPath := filepath.Join(snapDir, archiveName+".mpxar.didx")
	ppxarPath := filepath.Join(snapDir, archiveName+".ppxar.didx")

	// mmap DIDX files — avoids heap allocation.
	metaData, err := mmapFile(mpxarPath)
	if err != nil {
		return fmt.Errorf("mmap new mpxar: %w", err)
	}
	payloadData, err := mmapFile(ppxarPath)
	if err != nil {
		_ = munmap(metaData)
		return fmt.Errorf("mmap new ppxar: %w", err)
	}

	store, err := datastore.NewChunkStore(fs.pbsStore)
	if err != nil {
		_ = munmap(metaData)
		_ = munmap(payloadData)
		return fmt.Errorf("open chunk store: %w", err)
	}
	source := datastore.NewChunkStoreSource(store)

	newReader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		_ = munmap(metaData)
		_ = munmap(payloadData)
		return fmt.Errorf("create new reader: %w", err)
	}

	// Release previous mmap'd DIDX data.
	for _, d := range fs.mmapData {
		_ = munmap(d)
	}

	fs.pxar.HotSwap(newReader)
	fs.mmapData = nil
	if len(metaData) > 0 {
		fs.mmapData = append(fs.mmapData, metaData)
	}
	if len(payloadData) > 0 {
		fs.mmapData = append(fs.mmapData, payloadData)
	}
	fs.ResetState()

	fs.mu.Lock()
	fs.origSnapshot = snapshotRef{
		BackupType:  backupType,
		BackupID:    backupID,
		BackupTime:  backupTime,
		Namespace:   namespace,
		ArchiveName: archiveName,
	}
	fs.origPpxarDidx = ppxarPath
	fs.mu.Unlock()

	if err := os.RemoveAll(fs.backingDir); err != nil {
		return fmt.Errorf("cleanup backing dir: %w", err)
	}
	if err := os.MkdirAll(fs.backingDir, 0o755); err != nil {
		return fmt.Errorf("recreate backing dir: %w", err)
	}

	if fs.txnLog != nil {
		// Recreate the transactions subdirectory and clear the log.
		if err := os.MkdirAll(filepath.Join(fs.backingDir, TransactionsDir), 0o700); err != nil {
			return fmt.Errorf("recreate transactions dir: %w", err)
		}
		if err := fs.txnLog.Clear(); err != nil {
			return fmt.Errorf("clear transaction log: %w", err)
		}
	}

	return nil
}

// verifyBackedFileHashes compares each backed file's current SHA256
// against the hash computed during upload. Uses a single 64KB buffer
// across all files.
func (fs *PassthroughFS) verifyBackedFileHashes(hashes map[string][32]byte) error {
	buf := make([]byte, 64*1024)
	for relPath, expected := range hashes {
		abs := fs.absPath(relPath)
		f, err := os.Open(abs)
		if err != nil {
			return fmt.Errorf("open backed file %q for verification: %w", relPath, err)
		}
		h := sha256.New()
		_, err = io.CopyBuffer(h, f, buf)
		_ = f.Close()
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

func (fs *PassthroughFS) walkOverlay(ow *overlayWalk, pxarIno uint64, relPath string) error {
	// 1. Get pxar entries — cached to avoid redundant deserialization.
	pxarEntries, ok := ow.pxarDirCache[pxarIno]
	if !ok {
		pxarEntries, _ = fs.pxar.ReadDirRaw(pxarIno)
		ow.pxarDirCache[pxarIno] = pxarEntries
	}

	// 2. Get backed entry names from pre-walk.
	backedNames := ow.backedDirs[relPath]

	// 3. Build lookup maps.
	backedSet := make(map[string]bool, len(backedNames))
	for _, name := range backedNames {
		backedSet[name] = true
	}

	pxarByName := make(map[string]*dirEntrySlim, len(pxarEntries))
	for i := range pxarEntries {
		pxarByName[pxarEntries[i].name] = &pxarEntries[i]
	}

	// 4. Build sorted entry list.
	type posEntry struct {
		name       string
		entryStart uint64
		backed     bool
	}
	var entries []posEntry

	for _, pe := range pxarEntries {
		if backedSet[pe.name] {
			continue
		}
		childPath := joinPath(relPath, pe.name)
		if fs.mutationMode && fs.isPathDeleted(childPath) {
			continue
		}
		entries = append(entries, posEntry{
			name:       pe.name,
			entryStart: pe.entryStart,
			backed:     false,
		})
	}

	for _, name := range backedNames {
		fo := ^uint64(0)
		if pe, ok := pxarByName[name]; ok {
			fo = pe.entryStart
		}
		entries = append(entries, posEntry{
			name:       name,
			entryStart: fo,
			backed:     true,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entryStart < entries[j].entryStart
	})

	// 5. Process entries.
	for _, we := range entries {
		childRel := joinPath(relPath, we.name)

		if we.backed {
			abs := fs.absPath(childRel)
			fi, err := os.Lstat(abs)
			if err != nil {
				return fmt.Errorf("lstat %s: %w", abs, err)
			}

			if fi.IsDir() {
				meta := statToMetadata(fi, true)
				if err := ow.writer.BeginDirectory(we.name, &meta); err != nil {
					return fmt.Errorf("begin dir %s: %w", we.name, err)
				}
				// Look up pxar child inode from already-cached entries.
				var pxarChildIno uint64
				if pe, ok := pxarByName[we.name]; ok {
					pxarChildIno = pe.inode
				}
				if err := fs.walkOverlay(ow, pxarChildIno, childRel); err != nil {
					return err
				}
				if err := ow.writer.EndDirectory(); err != nil {
					return fmt.Errorf("end dir %s: %w", we.name, err)
				}
				continue
			}

			if fi.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(abs)
				if err != nil {
					return fmt.Errorf("readlink %s: %w", abs, err)
				}
				entry := &pxar.Entry{
					Path:       childRel,
					Kind:       pxar.KindSymlink,
					Metadata:   statToMetadata(fi, false),
					FileSize:   uint64(fi.Size()),
					LinkTarget: target,
				}
				if err := ow.writer.WriteEntry(entry, nil); err != nil {
					return fmt.Errorf("write symlink %s: %w", we.name, err)
				}
				continue
			}

			if !fi.Mode().IsRegular() {
				meta := statToMetadata(fi, false)
				entry := &pxar.Entry{
					Path:     childRel,
					Kind:     pxar.KindFile,
					Metadata: meta,
					FileSize: 0,
				}
				if err := ow.writer.WriteEntry(entry, nil); err != nil {
					return fmt.Errorf("write entry %s: %w", we.name, err)
				}
				continue
			}

			// Regular file: upload with concurrent SHA256 hashing.
			f, err := os.Open(abs)
			if err != nil {
				return fmt.Errorf("open %s: %w", abs, err)
			}
			hash := sha256.New()
			tee := io.TeeReader(f, hash)
			meta := statToMetadata(fi, false)
			entry := &pxar.Entry{
				Path:     childRel,
				Kind:     pxar.KindFile,
				Metadata: meta,
				FileSize: uint64(fi.Size()),
			}
			writeErr := ow.writer.WriteEntryReader(entry, tee, uint64(fi.Size()))
			if writeErr == nil {
				var h [32]byte
				hash.Sum(h[:0])
				ow.backedHashes[childRel] = h
			}
			f.Close()
			if writeErr != nil {
				return fmt.Errorf("write file %s: %w", we.name, writeErr)
			}
			if ow.progress != nil {
				ow.progress.AddFile(fi.Size())
			}
			continue
		}

		// pxar entry — use chunk reference (no data read).
		if we.entryStart == 0 {
			continue
		}

		fs.pxar.readerMu.Lock()
		pxarEntry, err := fs.pxar.reader.ReadEntryAt(int64(we.entryStart))
		fs.pxar.readerMu.Unlock()
		if err != nil {
			continue
		}

		childIno := ToInode(pxarEntry)

		if pxarEntry.IsDir() {
			fs.pxar.RegisterNode(childIno, pxarIno, pxarEntry)
			meta := buildMetaFromEntry(pxarEntry)
			if err := ow.writer.BeginDirectory(we.name, &meta); err != nil {
				return fmt.Errorf("begin dir %s: %w", we.name, err)
			}
			if err := fs.walkOverlay(ow, childIno, childRel); err != nil {
				return err
			}
			if err := ow.writer.EndDirectory(); err != nil {
				return fmt.Errorf("end dir %s: %w", we.name, err)
			}
			continue
		}

		entry := cloneEntryWithName(pxarEntry, we.name)
		if err := ow.writer.WriteEntryRef(entry, pxarEntry.PayloadOffset); err != nil {
			return fmt.Errorf("write pxar ref %s: %w", we.name, err)
		}
	}

	return nil
}

// preWalkBackingDir performs a single walk of the backing directory tree
// and returns a map from parent relPath to child names. Only directories
// containing backed files have entries, so empty directories are skipped.
func (fs *PassthroughFS) preWalkBackingDir() map[string][]string {
	tree := make(map[string][]string)
	filepath.Walk(fs.backingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(fs.backingDir, path)
		if err != nil || rel == "." {
			return nil
		}
		// Skip the internal transactions directory.
		if rel == TransactionsDir {
			return filepath.SkipDir
		}
		if strings.HasPrefix(rel, TransactionsDir+string(filepath.Separator)) {
			return nil
		}
		parentDir := filepath.Dir(rel)
		parentRel := "/"
		if parentDir != "." {
			parentRel = "/" + parentDir
		}
		tree[parentRel] = append(tree[parentRel], info.Name())
		return nil
	})
	return tree
}

func (fs *PassthroughFS) getPxarEntry(ino uint64) *pxar.Entry {
	if ino == RootInode {
		fs.pxar.readerMu.Lock()
		entry, err := fs.pxar.reader.ReadRoot()
		fs.pxar.readerMu.Unlock()
		if err == nil {
			return entry
		}
	}
	return &pxar.Entry{Kind: pxar.KindDirectory, Path: "/"}
}

func statToMetadata(fi os.FileInfo, isDir bool) pxar.Metadata {
	var mformat uint64
	if fi.Mode()&os.ModeSymlink != 0 {
		mformat = format.ModeIFLNK
	} else if isDir {
		mformat = format.ModeIFDIR
	} else {
		mformat = format.ModeIFREG
	}

	stat := fi.Sys().(*syscall.Stat_t)
	return pxar.Metadata{
		Stat: format.Stat{
			Mode: mformat | uint64(fi.Mode().Perm()),
			UID:  stat.Uid,
			GID:  stat.Gid,
			Mtime: format.StatxTimestamp{
				Secs:  stat.Mtim.Sec,
				Nanos: uint32(stat.Mtim.Nsec),
			},
		},
	}
}

func buildMetaFromEntry(e *pxar.Entry) pxar.Metadata {
	return pxar.Metadata{
		Stat:   e.Metadata.Stat,
		XAttrs: e.Metadata.XAttrs,
		FCaps:  e.Metadata.FCaps,
		ACL:    e.Metadata.ACL,
	}
}

func cloneEntryWithName(e *pxar.Entry, name string) *pxar.Entry {
	clone := *e
	clone.Path = name
	return &clone
}

// RunCommitSubcommand is the CLI entry point for `pxar-mount commit`.
func RunCommitSubcommand() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")
	pbsURL := fs.String("pbs-url", "", "PBS server URL")
	datastoreName := fs.String("datastore", "", "PBS datastore name")
	authToken := fs.String("token", "", "PBS API token")
	namespace := fs.String("ns", "", "PBS namespace")
	backupType := fs.String("backup-type", "", "Backup type")
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
		fmt.Fprintf(os.Stderr, "  ✗ error connecting to socket %s: %v\n", *socketPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s\n",
		*pbsURL, *datastoreName, *authToken, *namespace, *backupType, *backupID)
	if _, err := fmt.Fprint(conn, cmd); err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error sending command: %v\n", err)
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
		// Unknown line — ignore.
	}
	fmt.Fprintf(os.Stderr, "  ✗ error: no response from daemon\n")
	os.Exit(1)
}
