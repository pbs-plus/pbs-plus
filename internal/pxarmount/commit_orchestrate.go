package pxarmount

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

var commitMu sync.Mutex

var lastCommitTime int64

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
		pbsURL = token.DefaultAPIURL
		req.SkipTLS = true
	}
	datastoreName := req.Datastore
	if datastoreName == "" {
		datastoreName = ResolveDatastoreName(mfs.pbsStore)
	}
	authToken := req.AuthToken
	if authToken == "" {
		authToken = token.ReadLocal()
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
			Mode:  modeFormat | uint64(n.Mode&0o7777),
			UID:   n.UID,
			GID:   n.GID,
			Mtime: format.NewStatxTimestampFromTime(time.Unix(0, n.MtimeNs)),
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
