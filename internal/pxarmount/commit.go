package pxarmount

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
	writer transfer.ArchiveWriter
	dedup  *transfer.RemoteDedupSplitArchiveWriter
}

type walkEntry struct {
	entryStart uint64
	name       string
	backed     bool
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
		if err := fs.commitOverlay(req); err != nil {
			fmt.Fprintf(conn, "ERR %v\n", err)
			return
		}
		fmt.Fprintf(conn, "OK committed %s/%s\n", req.Namespace, req.BackupID)
		return
	}
}

func (fs *PassthroughFS) commitOverlay(req *CommitRequest) error {
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

	ow := &overlayWalk{writer: writer, dedup: writer}
	if err := fs.walkOverlay(ow, RootInode, "/"); err != nil {
		return fmt.Errorf("walk overlay: %w", err)
	}

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}

	if _, err = session.Finish(ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}

	return fs.postCommit(backupID, backupType, namespace, archiveName, backupTime)
}

func (fs *PassthroughFS) postCommit(backupID, backupType, namespace, archiveName string, backupTime int64) error {
	origDir := filepath.Dir(fs.origPpxarDidx)
	groupDir := filepath.Dir(origDir)

	newTimeISO := time.Unix(backupTime, 0).UTC().Format("2006-01-02T15:04:05Z")
	snapDir := filepath.Join(groupDir, newTimeISO)

	mpxarPath := filepath.Join(snapDir, archiveName+".mpxar.didx")
	ppxarPath := filepath.Join(snapDir, archiveName+".ppxar.didx")

	metaData, err := os.ReadFile(mpxarPath)
	if err != nil {
		return fmt.Errorf("read new mpxar: %w", err)
	}
	payloadData, err := os.ReadFile(ppxarPath)
	if err != nil {
		return fmt.Errorf("read new ppxar: %w", err)
	}

	store, err := datastore.NewChunkStore(fs.pbsStore)
	if err != nil {
		return fmt.Errorf("open chunk store: %w", err)
	}
	source := datastore.NewChunkStoreSource(store)

	newReader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		return fmt.Errorf("create new reader: %w", err)
	}

	if err := fs.verifyBackedFiles(newReader); err != nil {
		_ = newReader.Close()
		return fmt.Errorf("verification failed: %w", err)
	}

	fs.pxar.HotSwap(newReader)
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
		if err := fs.txnLog.Clear(); err != nil {
			return fmt.Errorf("clear transaction log: %w", err)
		}
	}

	return nil
}

func (fs *PassthroughFS) verifyBackedFiles(reader *transfer.SplitArchiveReader) error {
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
			return nil
		}

		if info.Mode()&os.ModeSymlink != 0 {
			if !entry.IsSymlink() {
				return fmt.Errorf("backed symlink %q is not a symlink in new snapshot", rel)
			}
			return nil
		}

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

func (fs *PassthroughFS) walkOverlay(ow *overlayWalk, pxarIno uint64, relPath string) error {
	pxarEntries, _ := fs.pxar.ReadDirRaw(pxarIno)
	backedEntries := fs.readBackedDir(relPath)

	fmt.Fprintf(os.Stderr, "walkOverlay ino=%d rel=%q pxar=%d backed=%d\n", pxarIno, relPath, len(pxarEntries), len(backedEntries))

	backedName := make(map[string]bool, len(backedEntries))
	for _, be := range backedEntries {
		backedName[be.name] = true
	}

	pxarByName := make(map[string]*dirEntrySlim, len(pxarEntries))
	for i := range pxarEntries {
		pxarByName[pxarEntries[i].name] = &pxarEntries[i]
	}

	type posEntry struct {
		walkEntry
		fileOffset uint64
	}
	var entries []posEntry

	for _, pe := range pxarEntries {
		if backedName[pe.name] {
			continue
		}
		childPath := joinPath(relPath, pe.name)
		if fs.mutationMode && fs.isPathDeleted(childPath) {
			continue
		}
		entries = append(entries, posEntry{
			walkEntry:  walkEntry{name: pe.name, entryStart: pe.entryStart, backed: false},
			fileOffset: pe.entryStart,
		})
	}

	for _, be := range backedEntries {
		fo := ^uint64(0)
		if pe, ok := pxarByName[be.name]; ok {
			fo = pe.entryStart
		}
		entries = append(entries, posEntry{
			walkEntry:  be,
			fileOffset: fo,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].fileOffset < entries[j].fileOffset
	})

	for _, we := range entries {
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
				if err := ow.writer.BeginDirectory(we.name, &meta); err != nil {
					return fmt.Errorf("begin dir %s: %w", we.name, err)
				}
				pxarChildIno := fs.pxarInoForPath(pxarIno, we.name)
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

			f, err := os.Open(abs)
			if err != nil {
				return fmt.Errorf("open %s: %w", abs, err)
			}
			meta := statToMetadata(fi, false)
			entry := &pxar.Entry{
				Path:     childRel,
				Kind:     pxar.KindFile,
				Metadata: meta,
				FileSize: uint64(fi.Size()),
			}
			err = ow.writer.WriteEntryReader(entry, f, uint64(fi.Size()))
			f.Close()
			if err != nil {
				return fmt.Errorf("write file %s: %w", we.name, err)
			}
			continue
		}

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

func (fs *PassthroughFS) readBackedDir(relPath string) []walkEntry {
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

func (fs *PassthroughFS) pxarInoForPath(parentIno uint64, name string) uint64 {
	entries, err := fs.pxar.ReadDirRaw(parentIno)
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
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit --socket <path> [options]\n")
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
