package pxarmount

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
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

// commitMu serializes commit operations.
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

// StartCommitListener opens a Unix domain socket and processes commit requests.
func StartCommitListener(sockPath string, mfs *MutableFS) (net.Listener, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", sockPath, err)
	}
	go func() {
		defer l.Close()
		defer os.Remove(sockPath)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleCommitConn(conn, mfs)
		}
	}()
	return l, nil
}

func handleCommitConn(conn net.Conn, mfs *MutableFS) {
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
		if err := commitOverlay(mfs, req, prog); err != nil {
			prog.Error(err.Error())
			return
		}
		return
	}
}

// commitOverlay folds the journal back into the immutable pxar and uploads.
func commitOverlay(mfs *MutableFS, req *CommitRequest, prog *ProgressReporter) error {
	commitMu.Lock()
	defer commitMu.Unlock()

	orig := mfs.origSnapshot

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

	if err := EnsureNamespaceDir(mfs.pbsStore, namespace); err != nil {
		return fmt.Errorf("ensure namespace dir: %w", err)
	}

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
	if mfs.origPpxarDidx != "" {
		origPayloadIdx, _ = os.ReadFile(mfs.origPpxarDidx)
	}

	writer, err := transfer.NewRemoteDedupSplitArchiveWriter(ctx, session, metaName, payloadName, origPayloadIdx)
	if err != nil {
		return fmt.Errorf("create dedup writer: %w", err)
	}

	// Build root metadata.
	rootMeta := buildRootMeta(mfs)

	if err := writer.Begin(&rootMeta, transfer.WriterOptions{Format: format.FormatVersion2}); err != nil {
		return fmt.Errorf("begin archive: %w", err)
	}

	prog.SetPhase(PhaseWalk)
	prog.SetMsg("Walking journal entries")

	// Load journal and xattrs.
	allEntries, _ := mfs.journal.AllEntries()
	allXAttrs, _ := mfs.journal.AllXAttrs()

	journalIdx := make(map[string]*JournalEntry, len(allEntries))
	whiteouts := make(map[string]bool)
	for i := range allEntries {
		journalIdx[allEntries[i].Path] = &allEntries[i]
		if allEntries[i].State == StateWhiteout {
			whiteouts[allEntries[i].Path] = true
		}
	}

	backedHashes := make(map[string][32]byte)

	// Walk the pxar tree recursively and apply journal mutations.
	if mfs.pxar.reader != nil {
		if err := walkPxarCommit(mfs, writer, RootInode, "/", journalIdx, whiteouts, allXAttrs, backedHashes, prog); err != nil {
			return fmt.Errorf("walk pxar: %w", err)
		}
	}

	// Emit new-only entries (state=new, not in pxar).
	var newPaths []string
	for path, je := range journalIdx {
		if je.State == StateNew && !whiteouts[path] {
			newPaths = append(newPaths, path)
		}
	}
	sort.Strings(newPaths)

	for _, path := range newPaths {
		je := journalIdx[path]
		if err := writeNewEntry(writer, mfs, path, je, allXAttrs[path], backedHashes, prog); err != nil {
			return fmt.Errorf("write new entry %q: %w", path, err)
		}
	}

	prog.SetPhase(PhaseUpload)
	prog.SetMsg("Uploading to PBS")

	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}

	if _, err = session.Finish(ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}

	prog.SetPhase(PhaseVerify)
	prog.SetMsg(fmt.Sprintf("Verifying %d backed files", len(backedHashes)))

	if len(backedHashes) > 0 {
		if err := verifyBackedFileHashes(mfs.mutableDir, backedHashes); err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
	}

	prog.SetPhase(PhaseFinalize)
	prog.SetMsg("Swapping snapshot")

	if err := postCommit(mfs, backupID, backupType, namespace, archiveName, backupTime); err != nil {
		return err
	}

	prog.Done(fmt.Sprintf("committed %s/%s", namespace, backupID))
	return nil
}

// walkPxarCommit recursively walks the pxar tree, checking the journal at each node.
func walkPxarCommit(mfs *MutableFS, writer *transfer.RemoteDedupSplitArchiveWriter,
	ino uint64, relPath string,
	journalIdx map[string]*JournalEntry, whiteouts map[string]bool,
	allXAttrs map[string]map[string][]byte,
	backedHashes map[string][32]byte, prog *ProgressReporter,
) error {
	entries, err := mfs.pxar.ReadDirRaw(ino)
	if err != nil {
		return nil // non-directory, or empty
	}

	// Get journal children for this dir (for whiteout/opaque checking).
	journalChildren, _ := mfs.journal.ListDir(relPath)
	journalChildSet := make(map[string]*JournalEntry)
	for i := range journalChildren {
		journalChildSet[journalChildren[i].Path] = &journalChildren[i]
	}

	// Get backed children from the mutable directory.
	backedNames := make(map[string]bool)
	absDir := filepath.Join(mfs.mutableDir, relPath)
	if des, readErr := os.ReadDir(absDir); readErr == nil {
		for _, de := range des {
			if de.Name() == JournalDir {
				continue
			}
			backedNames[de.Name()] = true
		}
	}

	// Build merged entry list: pxar minus whiteouts + backed.
	type walkEntry struct {
		name       string
		entryStart uint64
		backed     bool
	}
	var toprocess []walkEntry
	nameSet := make(map[string]bool)

	for _, e := range entries {
		childPath := joinPath(relPath, e.name)
		if whiteouts[childPath] {
			continue
		}
		if backedNames[e.name] {
			continue
		}
		if je, ok := journalChildSet[childPath]; ok && je.State == StateOpaque {
			// Opaque dir: backed dir children emitted separately, skip pxar children.
			continue
		}
		toprocess = append(toprocess, walkEntry{
			name:       e.name,
			entryStart: e.entryStart,
		})
		nameSet[e.name] = true
	}

	for name := range backedNames {
		if nameSet[name] {
			continue
		}
		childPath := joinPath(relPath, name)
		if whiteouts[childPath] {
			continue
		}
		toprocess = append(toprocess, walkEntry{
			name:   name,
			backed: true,
		})
	}

	sort.Slice(toprocess, func(i, j int) bool {
		return toprocess[i].entryStart < toprocess[j].entryStart
	})

	for _, we := range toprocess {
		childRel := joinPath(relPath, we.name)

		if we.backed {
			if err := writeBackedEntry(writer, mfs, childRel, journalIdx[childRel], allXAttrs[childRel], backedHashes, prog); err != nil {
				return err
			}
			continue
		}

		// pxar entry — check journal overlay.
		pxarEntry, perr := mfs.pxar.reader.ReadEntryAt(int64(we.entryStart))
		if perr != nil {
			continue
		}

		je := journalIdx[childRel]
		childIno := ToInode(pxarEntry)

		if pxarEntry.IsDir() {
			meta := buildMetaFromPxarEntry(pxarEntry)
			if je != nil {
				applyJournalToMeta(je, &meta)
			}

			if err := writer.BeginDirectory(we.name, &meta); err != nil {
				return fmt.Errorf("begin dir %s: %w", we.name, err)
			}

			// Check if journal says this dir is opaque - skip children.
			if je != nil && je.State == StateOpaque {
				// empty dir with journal metadata
			} else {
				if err := walkPxarCommit(mfs, writer, childIno, childRel, journalIdx, whiteouts, allXAttrs, backedHashes, prog); err != nil {
					return err
				}
			}

			if err := writer.EndDirectory(); err != nil {
				return fmt.Errorf("end dir %s: %w", we.name, err)
			}
			continue
		}

		// Non-directory: emit as ref or with data.
		entry := clonePxarEntry(pxarEntry, we.name)
		if je != nil {
			applyJournalToEntry(je, entry)
		}

		if je != nil && je.HasData {
			// Backed file data.
			abs := mfs.mutablePath(childRel)
			f, ferr := os.Open(abs)
			if ferr != nil {
				return fmt.Errorf("open backed %s: %w", childRel, ferr)
			}
			hash := sha256.New()
			tee := io.TeeReader(f, hash)
			wErr := writer.WriteEntryReader(entry, tee, je.Size)
			f.Close()
			if wErr != nil {
				return fmt.Errorf("write backed %s: %w", childRel, wErr)
			}
			var h [32]byte
			hash.Sum(h[:0])
			backedHashes[childRel] = h
			if prog != nil {
				prog.AddFile(int64(je.Size))
			}
		} else {
			// Immutable ref.
			if err := writer.WriteEntryRef(entry, pxarEntry.PayloadOffset); err != nil {
				return fmt.Errorf("write ref %s: %w", we.name, err)
			}
		}
	}

	return nil
}

// writeBackedEntry writes a fully mutable entry (from the backing directory).
func writeBackedEntry(writer *transfer.RemoteDedupSplitArchiveWriter,
	mfs *MutableFS, relPath string, je *JournalEntry,
	xattrs map[string][]byte, backedHashes map[string][32]byte, prog *ProgressReporter,
) error {
	abs := mfs.mutablePath(relPath)
	fi, err := os.Lstat(abs)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", abs, err)
	}

	if fi.Mode().IsDir() {
		meta := statToMetadata(fi, true)
		if je != nil {
			applyJournalToMeta(je, &meta)
		}
		name := filepath.Base(relPath)
		if err := writer.BeginDirectory(name, &meta); err != nil {
			return fmt.Errorf("begin backed dir %s: %w", relPath, err)
		}
		// Recurse into backed dir children.
		subEntries, _ := mfs.journal.ListDir(relPath)
		subIdx := make(map[string]*JournalEntry)
		for i := range subEntries {
			subIdx[subEntries[i].Path] = &subEntries[i]
		}
		// Get backed children.
		backedChildren := make(map[string]bool)
		if des, readErr := os.ReadDir(abs); readErr == nil {
			for _, de := range des {
				if de.Name() == JournalDir {
					continue
				}
				backedChildren[de.Name()] = true
			}
		}
		var childNames []string
		for name := range backedChildren {
			childNames = append(childNames, name)
		}
		sort.Strings(childNames)
		for _, childName := range childNames {
			childRel := joinPath(relPath, childName)
			if err := writeBackedEntry(writer, mfs, childRel, subIdx[childRel], nil, backedHashes, prog); err != nil {
				return err
			}
		}
		return writer.EndDirectory()
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		target, rerr := os.Readlink(abs)
		if rerr != nil {
			return fmt.Errorf("readlink %s: %w", abs, rerr)
		}
		entry := &pxar.Entry{
			Path:       filepath.Base(relPath),
			Kind:       pxar.KindSymlink,
			Metadata:   statToMetadata(fi, false),
			FileSize:   uint64(fi.Size()),
			LinkTarget: target,
		}
		return writer.WriteEntry(entry, nil)
	}

	if !fi.Mode().IsRegular() {
		meta := statToMetadata(fi, false)
		entry := &pxar.Entry{
			Path:     filepath.Base(relPath),
			Kind:     pxar.KindFile,
			Metadata: meta,
			FileSize: 0,
		}
		return writer.WriteEntry(entry, nil)
	}

	// Regular file.
	f, ferr := os.Open(abs)
	if ferr != nil {
		return fmt.Errorf("open %s: %w", abs, ferr)
	}
	defer f.Close()

	hash := sha256.New()
	tee := io.TeeReader(f, hash)
	meta := statToMetadata(fi, false)
	entry := &pxar.Entry{
		Path:     filepath.Base(relPath),
		Kind:     pxar.KindFile,
		Metadata: meta,
		FileSize: uint64(fi.Size()),
	}

	wErr := writer.WriteEntryReader(entry, tee, uint64(fi.Size()))
	if wErr != nil {
		return fmt.Errorf("write backed file %s: %w", relPath, wErr)
	}

	var h [32]byte
	hash.Sum(h[:0])
	backedHashes[relPath] = h
	if prog != nil {
		prog.AddFile(fi.Size())
	}
	return nil
}

// writeNewEntry writes a journal-only entry (no pxar counterpart).
func writeNewEntry(writer *transfer.RemoteDedupSplitArchiveWriter,
	mfs *MutableFS, relPath string, je *JournalEntry,
	xattrs map[string][]byte, backedHashes map[string][32]byte, prog *ProgressReporter,
) error {
	name := filepath.Base(relPath)

	if je.HasData {
		abs := mfs.mutablePath(relPath)
		fi, err := os.Lstat(abs)
		if err != nil {
			return fmt.Errorf("lstat %s: %w", abs, err)
		}

		if fi.IsDir() {
			meta := statToMetadata(fi, true)
			if je != nil {
				applyJournalToMeta(je, &meta)
			}
			return writer.BeginDirectory(name, &meta)
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			target, _ := os.Readlink(abs)
			entry := &pxar.Entry{
				Path:       name,
				Kind:       pxar.KindSymlink,
				Metadata:   statToMetadata(fi, false),
				FileSize:   uint64(fi.Size()),
				LinkTarget: target,
			}
			return writer.WriteEntry(entry, nil)
		}

		f, ferr := os.Open(abs)
		if ferr != nil {
			return fmt.Errorf("open new %s: %w", abs, ferr)
		}
		defer f.Close()

		hash := sha256.New()
		tee := io.TeeReader(f, hash)
		meta := statToMetadata(fi, false)
		if je != nil {
			applyJournalToMeta(je, &meta)
		}
		entry := &pxar.Entry{
			Path:     name,
			Kind:     pxar.KindFile,
			Metadata: meta,
			FileSize: uint64(fi.Size()),
		}

		wErr := writer.WriteEntryReader(entry, tee, uint64(fi.Size()))
		if wErr != nil {
			return fmt.Errorf("write new file %s: %w", relPath, wErr)
		}

		var h [32]byte
		hash.Sum(h[:0])
		backedHashes[relPath] = h
		if prog != nil {
			prog.AddFile(fi.Size())
		}
		return nil
	}

	// Metadata-only new entry (directory, symlink without data, etc.).
	entry := &pxar.Entry{
		Path: name,
		Kind: pxar.KindDirectory,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode: uint64(je.Mode),
				UID:  je.UID,
				GID:  je.GID,
				Mtime: format.StatxTimestamp{
					Secs:  je.MtimeNs / 1_000_000_000,
					Nanos: uint32(je.MtimeNs % 1_000_000_000),
				},
			},
		},
	}
	if je.Mode&syscall.S_IFDIR != 0 {
		entry.Kind = pxar.KindDirectory
	} else if je.Mode&syscall.S_IFLNK != 0 {
		entry.Kind = pxar.KindSymlink
		entry.LinkTarget = je.Target
	}
	return writer.WriteEntry(entry, nil)
}

// buildRootMeta builds the root pxar.Metadata, applying journal overrides.
func buildRootMeta(mfs *MutableFS) pxar.Metadata {
	meta := pxar.Metadata{
		Stat: format.Stat{
			Mode:  uint64(syscall.S_IFDIR | 0o755),
			UID:   0,
			GID:   0,
			Mtime: format.StatxTimestamp{Secs: time.Now().Unix()},
		},
	}
	if mfs.pxar.reader != nil {
		rootEntry, err := mfs.pxar.reader.ReadRoot()
		if err == nil {
			meta = rootEntry.Metadata
		}
	}
	if je, _ := mfs.journal.GetEntry("/"); je != nil && je.State != StateWhiteout {
		meta.Stat.Mode = uint64(je.Mode)
		meta.Stat.UID = je.UID
		meta.Stat.GID = je.GID
		meta.Stat.Mtime = format.StatxTimestamp{
			Secs:  je.MtimeNs / 1_000_000_000,
			Nanos: uint32(je.MtimeNs % 1_000_000_000),
		}
	}
	return meta
}

// statToMetadata converts an os.FileInfo to pxar.Metadata.
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

// buildMetaFromPxarEntry extracts Metadata from a pxar entry.
func buildMetaFromPxarEntry(e *pxar.Entry) pxar.Metadata {
	return pxar.Metadata{
		Stat:   e.Metadata.Stat,
		XAttrs: e.Metadata.XAttrs,
		FCaps:  e.Metadata.FCaps,
		ACL:    e.Metadata.ACL,
	}
}

// clonePxarEntry shallow-copies a pxar entry with a new path name.
func clonePxarEntry(e *pxar.Entry, name string) *pxar.Entry {
	clone := *e
	clone.Path = name
	return &clone
}

// applyJournalToMeta applies journal metadata overrides to a pxar.Metadata.
func applyJournalToMeta(je *JournalEntry, meta *pxar.Metadata) {
	meta.Stat.Mode = (meta.Stat.Mode & format.ModeIFMT) | uint64(je.Mode&0o7777)
	meta.Stat.UID = je.UID
	meta.Stat.GID = je.GID
	meta.Stat.Mtime = format.StatxTimestamp{
		Secs:  je.MtimeNs / 1_000_000_000,
		Nanos: uint32(je.MtimeNs % 1_000_000_000),
	}
}

// applyJournalToEntry applies journal overrides to a pxar.Entry.
func applyJournalToEntry(je *JournalEntry, entry *pxar.Entry) {
	entry.Metadata.Stat.Mode = (entry.Metadata.Stat.Mode & format.ModeIFMT) | uint64(je.Mode&0o7777)
	entry.Metadata.Stat.UID = je.UID
	entry.Metadata.Stat.GID = je.GID
	entry.Metadata.Stat.Mtime = format.StatxTimestamp{
		Secs:  je.MtimeNs / 1_000_000_000,
		Nanos: uint32(je.MtimeNs % 1_000_000_000),
	}
}

// postCommit swaps the new archive into place and resets mutable state.
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

	newReader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		_ = munmap(metaData)
		_ = munmap(payloadData)
		return fmt.Errorf("create new reader: %w", err)
	}

	// Release previous mmap data.
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

	// Reset journal.
	if err := mfs.journal.Clear(); err != nil {
		return fmt.Errorf("clear journal: %w", err)
	}

	// Remove mutable data files (keep journal db and its dir).
	entries, _ := os.ReadDir(mfs.mutableDir)
	for _, e := range entries {
		if e.Name() == JournalDir {
			continue
		}
		_ = os.RemoveAll(filepath.Join(mfs.mutableDir, e.Name()))
	}

	// Update snapshot ref.
	mfs.origSnapshot = snapshotRef{
		BackupType:  backupType,
		BackupID:    backupID,
		BackupTime:  backupTime,
		Namespace:   namespace,
		ArchiveName: archiveName,
	}
	mfs.origPpxarDidx = ppxarPath

	// Re-map root inode.
	mfs.mapInode(RootInode, "/")

	return nil
}

func hashMutableFile(absPath string) [32]byte {
	f, err := os.Open(absPath)
	if err != nil {
		return [32]byte{}
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 64*1024)
	_, _ = io.CopyBuffer(h, f, buf)
	var sum [32]byte
	h.Sum(sum[:0])
	return sum
}

func verifyBackedFileHashes(mutableDir string, hashes map[string][32]byte) error {
	buf := make([]byte, 64*1024)
	for relPath, expected := range hashes {
		abs := filepath.Join(mutableDir, relPath)
		f, err := os.Open(abs)
		if err != nil {
			return fmt.Errorf("open backed file %q: %w", relPath, err)
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

func snapshotGroupDir(pbsStore, backupType, backupID, namespace string) string {
	parts := []string{pbsStore}
	if namespace != "" {
		for comp := range strings.SplitSeq(namespace, "/") {
			if comp != "" {
				parts = append(parts, "ns", comp)
			}
		}
	}
	parts = append(parts, backupType, backupID)
	return filepath.Join(parts...)
}

// EnsureNamespaceDir creates namespace directory structure on the PBS datastore.
func EnsureNamespaceDir(pbsStore, namespace string) error {
	if namespace == "" {
		return nil
	}
	path := pbsStore
	for comp := range strings.SplitSeq(namespace, "/") {
		if comp == "" {
			continue
		}
		path = filepath.Join(path, "ns", comp)
	}
	fi, err := os.Stat(path)
	if err == nil && fi.IsDir() {
		return nil
	}
	return os.MkdirAll(path, 0o755)
}

// ParseOrigSnapshot extracts snapshot identity from a DIDX file path.
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
	if len(os.Args) < 4 || os.Args[2] == "-h" || os.Args[2] == "--help" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount commit <socket-path> [pbs-url] [datastore] [token] [ns] [type] [id]\n")
		os.Exit(1)
	}

	sockPath := os.Args[2]
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to socket %s: %v\n", sockPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	var parts []string
	parts = append(parts, "COMMIT")
	for i := 3; i < len(os.Args); i++ {
		parts = append(parts, os.Args[i])
	}
	command := strings.Join(parts, " ")
	_, _ = fmt.Fprintln(conn, command)

	display := NewProgressDisplay(os.Stderr)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "PROGRESS ") {
			display.Update(line)
		} else if strings.HasPrefix(line, "OK ") {
			display.Done(line)
			return
		} else if strings.HasPrefix(line, "ERR ") {
			display.Error(line)
			os.Exit(1)
		} else {
			fmt.Fprintln(os.Stderr, line)
		}
	}
}
