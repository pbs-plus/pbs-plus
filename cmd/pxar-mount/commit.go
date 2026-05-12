package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"

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
	BackupTime int64
	SkipTLS    bool
}

// parseCommitLine parses a COMMIT line from the socket protocol.
// Format: COMMIT <url> <datastore> <token> <namespace> <backup-type> <backup-id> [backup-time]
func parseCommitLine(line string) (*commitRequest, error) {
	parts := strings.SplitN(line, " ", 8)
	if len(parts) < 7 || parts[0] != "COMMIT" {
		return nil, fmt.Errorf("invalid COMMIT format: expected COMMIT <url> <datastore> <token> <namespace> <backup-type> <backup-id>")
	}
	req := &commitRequest{
		PBSURL:     parts[1],
		Datastore:  parts[2],
		AuthToken:  parts[3],
		Namespace:  parts[4],
		BackupType: parts[5],
		BackupID:   parts[6],
	}
	if req.Namespace == "-" {
		req.Namespace = ""
	}
	if len(parts) > 7 {
		_, _ = fmt.Sscanf(parts[7], "%d", &req.BackupTime)
	}
	if req.BackupType == "" {
		req.BackupType = "host"
	}
	return req, nil
}

// startSocketListener opens a Unix domain socket and processes commit requests.
// Returns a done channel that closes when the listener exits.
func (fs *passthroughFS) startSocketListener(sockPath string) (chan struct{}, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", sockPath, err)
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
	return done, nil
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
func (fs *passthroughFS) commitOverlay(req *commitRequest) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	store := backupproxy.NewPBSRemoteStore(backupproxy.PBSConfig{
		BaseURL:       req.PBSURL,
		Datastore:     req.Datastore,
		AuthToken:     req.AuthToken,
		Namespace:     req.Namespace,
		SkipTLSVerify: req.SkipTLS,
	}, buzhash.Config{}, false)

	bt, err := datastore.ParseBackupType(req.BackupType)
	if err != nil {
		return fmt.Errorf("invalid backup type %q: %w", req.BackupType, err)
	}

	ctx := context.Background()
	session, err := store.StartSession(ctx, backupproxy.BackupConfig{
		BackupType:     bt,
		BackupID:       req.BackupID,
		BackupTime:     req.BackupTime,
		PreviousBackup: nil, // TODO: support previous backup ref for dedup
		CryptMode:      datastore.CryptModeNone,
	})
	if err != nil {
		return fmt.Errorf("start PBS session: %w", err)
	}

	// Use SplitSessionArchiveWriter to build and upload the archive
	metaName := req.BackupID // will get .mpxar.didx suffix
	payloadName := req.BackupID + ".ppxar"

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

	return nil
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
	if err != nil {
		return nil
	}
	var entries []walkEntry
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
	namespace := fs.String("ns", "-", "PBS namespace (use '-' for none)")
	backupType := fs.String("backup-type", "host", "Backup type (host, vm, ct)")
	backupID := fs.String("backup-id", "", "Backup ID (required)")
	backupTime := fs.Int64("backup-time", 0, "Backup timestamp (Unix seconds)")

	fs.Parse(os.Args[2:])

	if *socketPath == "" || *pbsURL == "" || *datastoreName == "" || *authToken == "" || *backupID == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> --pbs-url <url> --datastore <name> --token <token> [--ns <ns>] [--backup-type host] --backup-id <id>\n")
		os.Exit(1)
	}

	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to socket %s: %v\n", *socketPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s %d\n",
		*pbsURL, *datastoreName, *authToken, *namespace, *backupType, *backupID, *backupTime)
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
