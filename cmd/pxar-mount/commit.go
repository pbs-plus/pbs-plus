package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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

// commitRequest holds parameters for a re-snapshot commit.
type commitRequest struct {
	PBSURL     string
	Datastore  string
	AuthToken  string
	Namespace  string
	BackupID   string
	BackupType string
	SkipTLS    bool
}

// parseCommitLine parses a COMMIT line from the socket protocol.
// Format: COMMIT <url> <datastore> <token> [namespace] [backup-type] [backup-id]
// All fields after token are optional and default to the original snapshot's values.
func parseCommitLine(line string) (*commitRequest, error) {
	parts := strings.SplitN(line, " ", 8)
	if len(parts) < 4 || parts[0] != "COMMIT" {
		return nil, fmt.Errorf("invalid COMMIT format: expected COMMIT <url> <datastore> <token> [namespace] [backup-type] [backup-id]")
	}
	req := &commitRequest{
		PBSURL:    parts[1],
		Datastore: parts[2],
		AuthToken: parts[3],
	}
	if len(parts) > 4 {
		req.Namespace = parts[4]
	}
	if req.Namespace == "-" {
		req.Namespace = ""
	}
	if len(parts) > 5 {
		req.BackupType = parts[5]
	}
	if len(parts) > 6 {
		req.BackupID = parts[6]
	}
	return req, nil
}

// startSocketListener opens a Unix domain socket and processes commit requests.
// Returns the listener (for graceful shutdown) and a done channel.
func (fs *passthroughFS) startSocketListener(sockPath string) (net.Listener, chan struct{}, error) {
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

func (fs *passthroughFS) handleSocketConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		req, err := parseCommitLine(line)
		if err != nil {
			fmt.Fprintf(conn, "ERR %v\n", err)
			return
		}
		if err := fs.commitOverlay(req); err != nil {
			fmt.Fprintf(conn, "ERR %v\n", err)
			return
		}
		fmt.Fprintf(conn, "OK committed %s/%s\n", req.Namespace, req.BackupID)
		return
	}
}

// commitOverlay walks the merged overlay filesystem, builds a new split pxar
// archive, and uploads it to PBS via the backup protocol.
//
// Uses the original snapshot's backup-id and archive-name. Backup-time is
// always the current time. Deduplicates against the original snapshot.
func (fs *passthroughFS) commitOverlay(req *commitRequest) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	orig := fs.origSnapshot

	// Resolve backup metadata: command-line overrides take priority,
	// otherwise default to the original snapshot's values.
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

	// Build previous backup ref for chunk dedup
	var prev *backupproxy.PreviousBackupRef
	if orig.BackupID != "" && orig.BackupTime > 0 {
		prev = &backupproxy.PreviousBackupRef{
			BackupType: bt,
			BackupID:   orig.BackupID,
			BackupTime: orig.BackupTime,
			Namespace:  orig.Namespace,
		}
	}

	store := backupproxy.NewPBSRemoteStore(backupproxy.PBSConfig{
		BaseURL:       req.PBSURL,
		Datastore:     req.Datastore,
		AuthToken:     req.AuthToken,
		Namespace:     namespace,
		SkipTLSVerify: req.SkipTLS,
	}, buzhash.Config{}, false)

	ctx := context.Background()
	session, err := store.StartSession(ctx, backupproxy.BackupConfig{
		BackupType:     bt,
		BackupID:       backupID,
		BackupTime:     backupTime,
		PreviousBackup: prev,
		CryptMode:      datastore.CryptModeNone,
	})
	if err != nil {
		return fmt.Errorf("start PBS session: %w", err)
	}

	// Use SplitSessionArchiveWriter to build and upload the archive.
	// The archive base name matches the original snapshot's archive filename.
	metaName := archiveName
	payloadName := archiveName + ".ppxar"

	writer := transfer.NewSplitSessionArchiveWriter(ctx, session, metaName, payloadName)

	// Build root metadata from pxar root entry
	rootPxarEntry := fs.getPxarEntry(rootInode)
	rootMeta := buildMetaFromEntry(rootPxarEntry)

	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		return fmt.Errorf("begin archive: %w", err)
	}

	if err := fs.walkOverlay(writer, rootInode, "/"); err != nil {
		return fmt.Errorf("walk overlay: %w", err)
	}

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}

	_, err = session.Finish(ctx)
	if err != nil {
		return fmt.Errorf("finish session: %w", err)
	}

	// Post-commit: hot-swap to the new snapshot and clean up
	if err := fs.postCommit(backupID, backupType, namespace, archiveName, backupTime); err != nil {
		return fmt.Errorf("post-commit: %w", err)
	}

	return nil
}

// postCommit switches the mounted pxar to the newly committed snapshot,
// verifies backed files are present, and cleans up the backing directory.
func (fs *passthroughFS) postCommit(backupID, backupType, namespace, archiveName string, backupTime int64) error {
	// Derive the new snapshot directory from the original DIDX path.
	// Example original: /pbs-store/ns/2001-PROJECTS/host/XM-05/2025-08-05T13:55:39Z/XM-05---F.ppxar.didx
	// The group directory is two levels up (past time + filename).
	origDir := filepath.Dir(fs.origPpxarDidx) // the time directory
	groupDir := filepath.Dir(origDir)         // the group directory

	// Format the new backup time as ISO 8601 (PBS directory naming).
	newTimeISO := time.Unix(backupTime, 0).UTC().Format("2006-01-02T15:04:05Z")
	snapDir := filepath.Join(groupDir, newTimeISO)

	mpxarPath := filepath.Join(snapDir, archiveName+".mpxar.didx")
	ppxarPath := filepath.Join(snapDir, archiveName+".ppxar.didx")

	// Read the new DIDX data
	metaData, err := os.ReadFile(mpxarPath)
	if err != nil {
		return fmt.Errorf("read new mpxar: %w", err)
	}
	payloadData, err := os.ReadFile(ppxarPath)
	if err != nil {
		return fmt.Errorf("read new ppxar: %w", err)
	}

	// Create a new chunk store source from the same datastore
	store, err := datastore.NewChunkStore(fs.pbsStore)
	if err != nil {
		return fmt.Errorf("open chunk store: %w", err)
	}
	source := datastore.NewChunkStoreSource(store)

	// Create new archive reader from the committed snapshot
	newReader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		return fmt.Errorf("create new reader: %w", err)
	}

	// Verify that every backed file exists in the new snapshot
	if err := fs.verifyBackedFiles(newReader); err != nil {
		_ = newReader.Close()
		return fmt.Errorf("verification failed: %w", err)
	}

	// Hot-swap the pxar reader to the new snapshot
	fs.pxar.hotSwap(newReader)

	// Update origSnapshot to the new state
	fs.origSnapshot = snapshotRef{
		BackupType:  backupType,
		BackupID:    backupID,
		BackupTime:  backupTime,
		Namespace:   namespace,
		ArchiveName: archiveName,
	}
	fs.origPpxarDidx = ppxarPath

	// Clean up the backing directory (all files are now in the new snapshot)
	if err := os.RemoveAll(fs.backingDir); err != nil {
		return fmt.Errorf("cleanup backing dir: %w", err)
	}
	if err := os.MkdirAll(fs.backingDir, 0o755); err != nil {
		return fmt.Errorf("recreate backing dir: %w", err)
	}

	return nil
}

// verifyBackedFiles checks that all files in the backing directory can be
// found in the new snapshot (confirming the commit was complete).
func (fs *passthroughFS) verifyBackedFiles(reader *transfer.SplitArchiveReader) error {
	return filepath.Walk(fs.backingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(fs.backingDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		// Try to look up this path in the new snapshot
		entry, lookupErr := reader.Lookup("/" + rel)
		if lookupErr != nil {
			return fmt.Errorf("backed file %q not found in new snapshot: %w", rel, lookupErr)
		}

		if info.IsDir() {
			if !entry.IsDir() {
				return fmt.Errorf("backed dir %q is not a directory in new snapshot", rel)
			}
			return nil
		}

		if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			return nil // special files can't be verified easily
		}

		if info.Mode()&os.ModeSymlink != 0 {
			if !entry.IsSymlink() {
				return fmt.Errorf("backed symlink %q is not a symlink in new snapshot", rel)
			}
			return nil
		}

		// Regular file: compare size and content
		if entry.FileSize != uint64(info.Size()) {
			return fmt.Errorf("backed file %q size mismatch: backing=%d snapshot=%d", rel, info.Size(), entry.FileSize)
		}

		if info.Size() > 0 {
			rc, err := reader.ReadFileContentReader(entry)
			if err != nil {
				return fmt.Errorf("read backed file %q from new snapshot: %w", rel, err)
			}

			f, err := os.Open(path)
			if err != nil {
				_ = rc.Close()
				return err
			}

			equal, err := readersEqual(rc, f)
			_ = rc.Close()
			_ = f.Close()
			if err != nil {
				return fmt.Errorf("compare backed file %q: %w", rel, err)
			}
			if !equal {
				return fmt.Errorf("backed file %q content differs from new snapshot", rel)
			}
		}

		return nil
	})
}

// readersEqual returns true if two readers produce identical bytes.
func readersEqual(a, b io.Reader) (bool, error) {
	bufA := make([]byte, 64*1024)
	bufB := make([]byte, 64*1024)
	for {
		nA, errA := io.ReadFull(a, bufA)
		nB, errB := io.ReadFull(b, bufB)

		if nA != nB {
			return false, nil
		}
		if !bytes.Equal(bufA[:nA], bufB[:nB]) {
			return false, nil
		}

		if errA == io.ErrUnexpectedEOF || errA == io.EOF {
			if errB == io.ErrUnexpectedEOF || errB == io.EOF {
				return true, nil
			}
			return false, nil
		}
		if errA != nil {
			return false, errA
		}
		if errB != nil {
			return false, errB
		}
	}
}

// walkOverlay recursively walks the overlay and writes entries to the ArchiveWriter.
func (fs *passthroughFS) walkOverlay(w transfer.ArchiveWriter, pxarIno uint64, relPath string) error {
	pxarEntries, _ := fs.pxar.readDirRaw(pxarIno)
	backedEntries := fs.readBackedDir(relPath)

	backedName := make(map[string]bool, len(backedEntries))
	for _, be := range backedEntries {
		backedName[be.name] = true
	}

	// Process entries: pxar entries first, but skip those shadowed by backed
	var allEntries []walkEntry
	allEntries = append(allEntries, backedEntries...)
	for _, pe := range pxarEntries {
		if backedName[pe.name] {
			continue
		}
		allEntries = append(allEntries, walkEntry{
			name:   pe.name,
			pxar:   &pe.meta,
			backed: false,
		})
	}

	for _, we := range allEntries {
		childRel := relPath
		if childRel != "/" {
			childRel += "/"
		}
		childRel += we.name

		if we.backed {
			abs := fs.absPath(childRel)
			fi, err := os.Lstat(abs)
			if err != nil {
				return fmt.Errorf("lstat %s: %w", abs, err)
			}

			if fi.IsDir() {
				meta := statToMetadata(fi, true)
				if err := w.BeginDirectory(we.name, &meta); err != nil {
					return fmt.Errorf("begin dir %s: %w", we.name, err)
				}
				pxarChildIno := fs.pxarInoForPath(pxarIno, we.name)
				if err := fs.walkOverlay(w, pxarChildIno, childRel); err != nil {
					return err
				}
				if err := w.EndDirectory(); err != nil {
					return fmt.Errorf("end dir %s: %w", we.name, err)
				}
				continue
			}

			entry, content, err := fs.readBackedEntry(childRel, fi)
			if err != nil {
				return fmt.Errorf("read backed entry %s: %w", childRel, err)
			}
			if err := w.WriteEntry(entry, content); err != nil {
				return fmt.Errorf("write entry %s: %w", we.name, err)
			}
			continue
		}

		// Pxar-only entry
		pxarEntry := we.pxar
		if pxarEntry == nil {
			continue
		}

		childIno := toInode(pxarEntry)

		if pxarEntry.IsDir() {
			meta := buildMetaFromEntry(pxarEntry)
			if err := w.BeginDirectory(we.name, &meta); err != nil {
				return fmt.Errorf("begin dir %s: %w", we.name, err)
			}
			if err := fs.walkOverlay(w, childIno, childRel); err != nil {
				return err
			}
			if err := w.EndDirectory(); err != nil {
				return fmt.Errorf("end dir %s: %w", we.name, err)
			}
			continue
		}

		// Pxar file
		content, err := fs.readPxarContent(pxarEntry)
		if err != nil {
			return fmt.Errorf("read pxar file %s: %w", childRel, err)
		}
		entry := cloneEntryWithName(pxarEntry, we.name)
		if err := w.WriteEntry(entry, content); err != nil {
			return fmt.Errorf("write pxar entry %s: %w", we.name, err)
		}
	}

	return nil
}

// walkEntry represents a single entry in the merged overlay walk.
type walkEntry struct {
	name   string
	pxar   *pxar.Entry // non-nil if from pxar
	backed bool
}

// readBackedDir reads directory entries from the backing filesystem.
func (fs *passthroughFS) readBackedDir(relPath string) []walkEntry {
	abs := fs.absPath(relPath)
	des, err := os.ReadDir(abs)
	if err != nil || len(des) == 0 {
		return nil
	}
	entries := make([]walkEntry, 0, len(des))
	for _, de := range des {
		entries = append(entries, walkEntry{
			name:   de.Name(),
			backed: true,
		})
	}
	return entries
}

// pxarInoForPath finds the pxar inode for a child within a pxar directory.
func (fs *passthroughFS) pxarInoForPath(parentIno uint64, name string) uint64 {
	entries, err := fs.pxar.readDirRaw(parentIno)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.name == name {
			return e.inode
		}
	}
	return 0
}

// readBackedEntry reads a file/symlink from the backing dir and returns
// a pxar.Entry plus content bytes.
func (fs *passthroughFS) readBackedEntry(relPath string, fi os.FileInfo) (*pxar.Entry, []byte, error) {
	abs := fs.absPath(relPath)

	if fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(abs)
		if err != nil {
			return nil, nil, err
		}
		entry := &pxar.Entry{
			Path:       relPath,
			Kind:       pxar.KindSymlink,
			Metadata:   statToMetadata(fi, false),
			FileSize:   uint64(fi.Size()),
			LinkTarget: target,
		}
		return entry, nil, nil
	}

	if fi.IsDir() {
		entry := &pxar.Entry{
			Path:     relPath,
			Kind:     pxar.KindDirectory,
			Metadata: statToMetadata(fi, true),
			FileSize: uint64(fi.Size()),
		}
		return entry, nil, nil
	}

	// Regular file
	content, err := os.ReadFile(abs)
	if err != nil {
		return nil, nil, err
	}
	entry := &pxar.Entry{
		Path:     relPath,
		Kind:     pxar.KindFile,
		Metadata: statToMetadata(fi, false),
		FileSize: uint64(len(content)),
	}
	return entry, content, nil
}

// readPxarContent reads the content of a pxar archive entry.
func (fs *passthroughFS) readPxarContent(entry *pxar.Entry) ([]byte, error) {
	if entry.IsRegularFile() {
		fs.pxar.readerMu.Lock()
		pxarEntry, err := fs.pxar.readEntryForNode(&node{
			inode:      toInode(entry),
			entryStart: entry.FileOffset,
		})
		if err != nil {
			fs.pxar.readerMu.Unlock()
			return nil, err
		}
		rc, err := fs.pxar.reader.ReadFileContentReader(pxarEntry)
		if err != nil {
			fs.pxar.readerMu.Unlock()
			return nil, err
		}
		content, err := io.ReadAll(rc)
		_ = rc.Close()
		fs.pxar.readerMu.Unlock()
		return content, err
	}
	if entry.IsSymlink() {
		return nil, nil
	}
	return nil, nil
}

// --- helpers ---

func (fs *passthroughFS) getPxarEntry(ino uint64) *pxar.Entry {
	if ino == rootInode {
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

// commitMu serializes commit operations (one at a time).
var commitMu sync.Mutex

// runCommitSubcommand is the CLI entry point for `pxar-mount commit`.
// It connects to a running pxar-mount daemon socket and sends a COMMIT command.
func runCommitSubcommand() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	socketPath := fs.String("socket", "", "Path to pxar-mount Unix socket (required)")
	pbsURL := fs.String("pbs-url", "", "PBS server URL (required)")
	datastoreName := fs.String("datastore", "", "PBS datastore name (required)")
	authToken := fs.String("token", "", "PBS API token (required)")
	namespace := fs.String("ns", "-", "PBS namespace (use '-' for none; defaults to original snapshot's)")
	backupType := fs.String("backup-type", "", "Backup type (defaults to original snapshot's)")
	backupID := fs.String("backup-id", "", "Backup ID (defaults to original snapshot's; generates a new snapshot in the same group)")

	fs.Parse(os.Args[2:])

	if *socketPath == "" || *pbsURL == "" || *datastoreName == "" || *authToken == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> --pbs-url <url> --datastore <name> --token <token> [--ns <ns>] [--backup-type host] [--backup-id <id>]\n")
		os.Exit(1)
	}

	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to socket %s: %v\n", *socketPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s\n",
		*pbsURL, *datastoreName, *authToken, *namespace, *backupType, *backupID)
	if _, err := fmt.Fprint(conn, cmd); err != nil {
		fmt.Fprintf(os.Stderr, "Error sending command: %v\n", err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		response := scanner.Text()
		fmt.Println(response)
		if strings.HasPrefix(response, "ERR") {
			os.Exit(1)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Error: no response from daemon\n")
		os.Exit(1)
	}
}
