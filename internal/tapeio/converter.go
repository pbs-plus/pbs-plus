package tapeio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
)

var errSnapshotDone = errors.New("selected snapshot processed")

type Config struct {
	PBSURL        string
	Datastore     string
	Namespace     string
	AuthToken     string
	SkipTLS       bool
	BackupID      string
	ArchiveName   string
	LocalDir      string
	Sources       []string
	TapeDevice    string
	ChangerDevice string
	// changer (almost always 0 for single-drive libraries).
	DriveIndex          int
	Verbose             bool
	Compress            bool
	Spanning            bool
	SnapshotSel         int
	IgnoreNewerPrevious bool
	SpoolDir            string
	SpoolCapBytes       int64
	NamespaceResolver   func(host, device string) string
	OnSnapshot          func(backupID, namespace string)
	TaskLog             func(string)
	Feeder              *Feeder
	MigrationTag        string
	SnapshotPBA         int64
	OnSetMapRead        func(entry mtf.SetMapEntry)
	Progress            func(Progress)
	SnapshotResolver    func(entries []mtf.SetMapEntry) int
}

type Stats struct {
	Host      string
	BackupID  string
	Snapshots int
	Files     int
	Dirs      int
	Bytes     int64
	StartTime time.Time
}

type Progress struct {
	Files      int64
	Dirs       int64
	Bytes      int64
	PhysInst   float64
	PhysAvg    float64
	TapeInst   float64
	TapeAvg    float64
	IngestInst float64
	IngestAvg  float64
	FilesInst  float64
	FilesAvg   float64
}

type Snapshot struct {
	Index       int
	SourceFile  string
	Name        string
	BackupTime  time.Time
	Owner       string
	MachineName string
	VolumeName  string
	Truncated   bool
}

type backupMeta struct {
	HostName   string
	BackupTime time.Time
	SetName    string
	Owner      string
}

// in stream order, visiting only structural blocks (no file data).
func ListSnapshots(ctx context.Context, cfg Config) ([]Snapshot, error) {
	_ = ctx
	var snapshots []Snapshot

	if cfg.TapeDevice != "" {
		var logf func(string)
		if cfg.TaskLog != nil {
			logf = cfg.TaskLog
		}
		rc, err := OpenTapeReaderWithLog(cfg.TapeDevice, logf)
		if err != nil {
			return nil, err
		}
		sm, smErr := mtf.ReadSetMap(rc)
		if smErr != nil {
			log.Error(smErr, "")
		}
		if sm != nil && len(sm.Entries) > 0 {
			for _, e := range sm.Entries {
				snap := Snapshot{
					Index:      len(snapshots),
					Name:       e.Name,
					BackupTime: e.WriteTime,
					Owner:      e.Owner,
				}
				for _, v := range e.Volumes {
					if snap.MachineName == "" {
						snap.MachineName = v.MachineName
					}
					if snap.VolumeName != "" {
						snap.VolumeName += "; "
					}
					snap.VolumeName += v.Name
				}
				snapshots = append(snapshots, snap)
			}
			if err := rc.Close(); err != nil {
				log.Error(err, "")
			}
			return snapshots, nil
		}
		if err := rc.Rewind(); err != nil {
			log.Error(err, "")
		}
		r := mtf.NewReader(rc)
		if cfg.Spanning {
			setupTapeContinuation(r, cfg.TapeDevice)
		}
		if err := scanSnapshots(r, cfg.TapeDevice, &snapshots); err != nil {
			return snapshots, err
		}
	} else {
		files, err := collectBKFFiles(cfg.Sources)
		if err != nil {
			return nil, err
		}
		if len(files) == 0 {
			return nil, fmt.Errorf("no .bkf files found")
		}
		if cfg.Spanning && len(files) > 1 {
			r, err := mtf.Open(files[0])
			if err != nil {
				return snapshots, err
			}
			setupFileContinuation(r, files)
			err = scanSnapshots(r, files[0], &snapshots)
			if err := r.Close(); err != nil {
				log.Error(err, "")
			}
			if err != nil {
				return snapshots, err
			}
		} else {
			for _, f := range files {
				r, err := mtf.Open(f)
				if err != nil {
					return snapshots, err
				}
				err = scanSnapshots(r, f, &snapshots)
				if err := r.Close(); err != nil {
					log.Error(err, "")
				}
				if err != nil {
					return snapshots, err
				}
			}
		}
	}

	return snapshots, nil
}

func scanSnapshots(r *mtf.Reader, source string, out *[]Snapshot) error {
	var cur *Snapshot
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if cur != nil {
				*out = append(*out, *cur)
			}
			return err
		}
		switch b.Kind {
		case mtf.KindSet:
			if cur != nil {
				*out = append(*out, *cur)
			}
			cur = &Snapshot{
				Index:      len(*out),
				SourceFile: filepath.Base(source),
			}
			if b.Set != nil {
				cur.Name = b.Set.Name
				cur.BackupTime = b.Set.CreateTime
				cur.Owner = b.Set.Owner
			}
		case mtf.KindEntry:
			if cur != nil && b.Header.Type == mtf.EntryVolume {
				cur.MachineName = b.Header.MachineName
				if cur.VolumeName != "" {
					cur.VolumeName += "; "
				}
				cur.VolumeName += b.Header.Name
			}
		}
	}
	if cur != nil {
		if r.TruncatedByEOTM() {
			cur.Truncated = true
		}
		*out = append(*out, *cur)
	}
	if r.TruncatedByEOTM() && len(*out) > 0 {
		(*out)[len(*out)-1].Truncated = true
	}
	return nil
}

// Run performs the full conversion: reads BKF sources, builds pxar archive(s),
func Run(ctx context.Context, cfg Config) (*Stats, error) {
	chunkCfg, err := buzhash.NewConfig(4 << 20)
	if err != nil {
		return nil, fmt.Errorf("chunk config: %w", err)
	}

	c := &converter{
		cfg:         cfg,
		ctx:         ctx,
		chunkCfg:    chunkCfg,
		stats:       Stats{StartTime: time.Now()},
		prog:        newProgress(),
		snapshotIdx: -1,
	}
	c.stats.StartTime = c.prog.startTime

	stopReport := c.prog.reportWith(ctx, os.Stderr, 2*time.Second, c.cfg.Progress)
	defer stopReport()

	syncStats := func() {
		files, dirs, bytes := c.prog.snapshot()
		c.stats.Files = files
		c.stats.Dirs = dirs
		c.stats.Bytes = bytes
	}

	if cfg.TapeDevice != "" {
		c.logf("Starting tape migration: device=%s changer=%s", cfg.TapeDevice, cfg.ChangerDevice)
		lock, lerr := LockTapeDevice(cfg.TapeDevice)
		if lerr != nil {
			syncStats()
			return &c.stats, lerr
		}
		c.logf("[tape-lock] acquired PBS drive lock for %s (%s)", cfg.TapeDevice, lock.Path())
		defer func() { _ = lock.Close() }()
		if err := c.runTape(); err != nil {
			syncStats()
			return &c.stats, err
		}
	} else {
		c.logf("Starting file migration: sources=%v", cfg.Sources)
		if err := c.runFiles(); err != nil {
			syncStats()
			return &c.stats, err
		}
	}

	syncStats()
	return &c.stats, nil
}

type converter struct {
	cfg      Config
	ctx      context.Context
	chunkCfg buzhash.Config
	stats    Stats
	prog     *progress

	session    backupproxy.BackupSession
	writer     *transfer.RemoteDedupWriter
	meta       backupMeta
	tapeLabel  string
	rootPrefix string
	dirStack   []string
	currentNS  string

	snapshotIdx int
}

func (c *converter) logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if c.cfg.Verbose {
		fmt.Fprintln(os.Stderr, msg)
	}
	if c.cfg.TaskLog != nil {
		c.cfg.TaskLog(msg)
	}
}

// ensureSession lazily creates the PBS/local session and pxar writer on the
func (c *converter) ensureSession() error {
	if c.session != nil {
		return nil
	}

	backupID := c.cfg.BackupID
	if backupID == "" {
		backupID = c.meta.HostName
	}
	if backupID == "" {
		h, hostErr := os.Hostname()
		if hostErr != nil {
			log.Error(hostErr, "")
		}
		backupID = h
	}
	if c.tapeLabel != "" {
		backupID = backupID + "-" + sanitizePath(c.tapeLabel)
	}
	if c.cfg.MigrationTag != "" {
		backupID = backupID + "-" + sanitizePath(c.cfg.MigrationTag)
	}

	archiveName := c.cfg.ArchiveName
	if archiveName == "" {
		archiveName = backupID
	}

	backupTime := c.meta.BackupTime
	if backupTime.IsZero() {
		backupTime = time.Now()
	}

	c.stats.BackupID = backupID
	c.stats.Host = c.meta.HostName
	c.stats.Snapshots++

	c.currentNS = c.cfg.Namespace
	if c.cfg.NamespaceResolver != nil {
		if ns := c.cfg.NamespaceResolver(c.meta.HostName, c.rootPrefix); ns != "" {
			c.currentNS = ns
		}
	}
	if c.cfg.OnSnapshot != nil {
		c.cfg.OnSnapshot(backupID, c.currentNS)
	}
	c.logf("Starting PBS session: backup=%s ns=%s time=%s", backupID, c.currentNS, backupTime.Format("2006-01-02 15:04"))

	s, err := c.createSession(backupID, backupTime)
	if err != nil {
		return err
	}
	c.session = s

	rootMeta := pxar.DirMetadata(0o755).
		Owner(0, 0).
		Mtime(format.NewStatxTimestampFromTime(backupTime)).
		Build()

	w, err := transfer.NewRemoteDedupWriter(c.ctx, s,
		archiveName+".mpxar.didx", archiveName+".ppxar.didx")
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	c.writer = w

	return c.writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2})
}

func (c *converter) createSession(backupID string, backupTime time.Time) (backupproxy.BackupSession, error) {
	if c.cfg.LocalDir != "" {
		storeDir := filepath.Join(c.cfg.LocalDir, sanitizePath(backupID))
		store, err := backupproxy.NewLocalStore(storeDir, c.chunkCfg, true)
		if err != nil {
			return nil, fmt.Errorf("local store: %w", err)
		}
		return store.StartSession(c.ctx, backupproxy.BackupConfig{
			BackupType:          datastore.BackupHost,
			BackupID:            backupID,
			BackupTime:          backupTime.Unix(),
			Namespace:           c.currentNS,
			CryptMode:           datastore.CryptModeNone,
			ChunkConfig:         c.chunkCfg,
			Compress:            c.cfg.Compress,
			Debug:               true,
			IgnoreNewerPrevious: true,
		})
	}

	authToken := c.cfg.AuthToken
	if authToken == "" {
		authToken = token.ReadLocal()
	}
	store := backupproxy.NewPBSStore(backupproxy.PBSConfig{
		BaseURL:       c.cfg.PBSURL,
		Datastore:     c.cfg.Datastore,
		AuthToken:     authToken,
		Namespace:     c.currentNS,
		SkipTLSVerify: c.cfg.SkipTLS,
	}, c.chunkCfg, c.cfg.Compress)

	return store.StartSession(c.ctx, backupproxy.BackupConfig{
		BackupType:          datastore.BackupHost,
		BackupID:            backupID,
		BackupTime:          backupTime.Unix(),
		Namespace:           c.currentNS,
		CryptMode:           datastore.CryptModeNone,
		ChunkConfig:         c.chunkCfg,
		Compress:            c.cfg.Compress,
		Debug:               true,
		IgnoreNewerPrevious: true,
	})
}

// finishSnapshot closes open directories and finalizes the current
func (c *converter) finishSnapshot() error {
	if c.writer == nil {
		return nil
	}
	for len(c.dirStack) > 0 {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	if err := c.writer.Finish(); err != nil {
		c.writer = nil
		c.session = nil
		return fmt.Errorf("finish writer: %w", err)
	}
	if _, err := c.session.Finish(c.ctx); err != nil {
		c.writer = nil
		c.session = nil
		return fmt.Errorf("finish session: %w", err)
	}
	files, dirs, bytes := c.prog.snapshot()
	c.logf("Snapshot complete: %d files, %d dirs, %d bytes", files, dirs, bytes)
	c.writer = nil
	c.session = nil
	return nil
}

func (c *converter) snapshotSelected() bool {
	return c.cfg.SnapshotSel < 0 || c.snapshotIdx == c.cfg.SnapshotSel
}

func (c *converter) locateToSnapshot(rc *TapeReader, r *mtf.Reader) error {
	if c.cfg.SnapshotPBA > 0 {
		c.logf("Reading TAPE descriptor block (BOT + 1)")
		if _, err := r.Next(); err != nil {
			return fmt.Errorf("read TAPE descriptor: %w", err)
		}
		c.logf("Locating to snapshot at PBA %d (from inventory)", c.cfg.SnapshotPBA)
		if err := r.SeekToBlock(c.cfg.SnapshotPBA); err != nil {
			return fmt.Errorf("seek to snapshot: %w", err)
		}
		c.logf("Located to snapshot, ready to read entries")
		return nil
	}
	if c.cfg.SnapshotSel < 0 && c.cfg.SnapshotResolver == nil {
		return nil
	}
	c.logf("Reading TAPE descriptor block (BOT + 1)")
	if _, err := r.Next(); err != nil {
		return fmt.Errorf("read TAPE descriptor: %w", err)
	}
	c.logf("Reading SetMap (EOM + read back)")
	blk, err := r.Next()
	if err != nil {
		return fmt.Errorf("read TAPE descriptor: %w", err)
	}
	c.logf("TAPE descriptor: name=%s sequence=%d family=0x%08X", blk.Tape.Name, blk.Tape.Sequence, blk.Tape.MFMID)
	c.logf("Reading SetMap (EOM + read back)")
	sm, sErr := mtf.ReadSetMap(rc)
	if sErr != nil {
		return fmt.Errorf("read set map for snapshot locate: %w", sErr)
	}
	if sm == nil || len(sm.Entries) == 0 {
		c.logf("SetMap empty, reading sequentially")
		return nil
	}
	sel := c.cfg.SnapshotSel
	if c.cfg.SnapshotResolver != nil {
		sel = c.cfg.SnapshotResolver(sm.Entries)
		c.cfg.SnapshotSel = sel
	}
	if sel < 0 || sel >= len(sm.Entries) {
		c.logf("Snapshot selection %d out of range (%d entries), reading sequentially", sel, len(sm.Entries))
		return nil
	}
	pba := int64(sm.Entries[sel].SSETPBA)
	if len(sm.Entries) > 0 && sm.Entries[0].SSETPBA > 0 {
		if pos, pErr := rc.TellBlock(); pErr == nil {
			offset := int64(sm.Entries[0].SSETPBA) - pos
			pba -= offset
			c.logf("Calibrated PBA %d (raw=%d, offset=%d, firstPos=%d)", pba, sm.Entries[sel].SSETPBA, offset, pos)
		}
	}
	c.logf("Locating to snapshot %d (%q) at PBA %d", sel, sm.Entries[sel].Name, pba)
	if c.cfg.OnSetMapRead != nil {
		c.cfg.OnSetMapRead(sm.Entries[sel])
	}
	if err := r.SeekToBlock(pba); err != nil {
		return fmt.Errorf("seek to snapshot %d: %w", sel, err)
	}
	c.snapshotIdx = sel - 1
	c.logf("Located to snapshot %d, ready to read entries", sel)
	return nil
}

func (c *converter) runTape() error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}
	if c.cfg.ChangerDevice != "" {
		return c.runChanger()
	}
	var logf func(string)
	if c.cfg.TaskLog != nil {
		logf = c.cfg.TaskLog
	}
	rc, err := OpenTapeReaderWithLog(c.cfg.TapeDevice, logf)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	r := mtf.NewReader(rc)
	if c.cfg.Spanning {
		setupTapeContinuation(r, c.cfg.TapeDevice)
	}
	if err := c.locateToSnapshot(rc, r); err != nil {
		return err
	}
	return c.processReader(r)
}

func (c *converter) runChanger() error {
	f := c.cfg.Feeder
	if f == nil {
		var err error
		f, err = NewFeeder(c.cfg.ChangerDevice, c.cfg.TapeDevice, c.cfg.DriveIndex, WithLog(func(msg string) { c.logf("%s", msg) }), WithContext(c.ctx))
		if err != nil {
			return err
		}
		defer f.Close()
	}

	err := f.ForEachTape(func(rc *TapeReader, barcode string) error {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}
		r := mtf.NewReader(rc)
		r.SetContinuation(f.AsContinuation())
		if err := c.locateToSnapshot(rc, r); err != nil {
			return err
		}
		if err := c.processReader(r); err != nil {
			return err
		}
		if c.cfg.SnapshotSel >= 0 {
			return errSnapshotDone
		}
		return nil
	})
	if errors.Is(err, errSnapshotDone) {
		return nil
	}
	return err
}

func (c *converter) runFiles() error {
	files, err := collectBKFFiles(c.cfg.Sources)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no .bkf files found")
	}

	if c.cfg.Spanning && len(files) > 1 {
		r, err := mtf.Open(files[0])
		if err != nil {
			return fmt.Errorf("open %s: %w", files[0], err)
		}
		setupFileContinuation(r, files)
		perr := c.processReader(r)
		if err := r.Close(); err != nil {
			log.Error(err, "")
		}
		return perr
	}

	for _, f := range files {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}
		r, err := mtf.Open(f)
		if err != nil {
			return fmt.Errorf("open %s: %w", f, err)
		}
		perr := c.processReader(r)
		if err := r.Close(); err != nil {
			log.Error(err, "")
		}
		if perr != nil {
			return fmt.Errorf("process %s: %w", f, perr)
		}
	}
	return nil
}

type tapeOp struct {
	kind    opKind
	name    string
	depth   int
	relPath string
	meta    pxar.Metadata
	linkTgt string
	rootPfx string
}

type opKind int

const (
	opVolume opKind = iota
	opDir
	opSymlink
	opSet
	opSetEnd
	opEnd
)

func (c *converter) processReader(r *mtf.Reader) error {
	return c.walk(r)
}

// walk drives a single-threaded MTF→pxar conversion. Files are streamed
// directly from the tape reader to the pxar writer (tape→PBS) with zero
// intermediate buffering — the mtf.Reader is positioned at the file's STAN
// data and handed to WriteEntryReader, which pulls bytes at ingest rate. This
// replaces the former pump/drain/spool pipeline that buffered whole files in
// RAM (io.ReadAll) and OOM'd on multi-GB Backup Exec archives. PBS ingest
// speed now gates tape read, which is correct backpressure. Directories,
// volumes and symlinks are tiny and are handled inline via the same consume*
// helpers the drain used.
func (c *converter) walk(r *mtf.Reader) error {
	c.prog.markProcessing()
	defer c.prog.markProcessingDone()
	defer func() { c.prog.tapePhysBytes.Store(r.Position()) }()

	var lastPos int64 = r.Position()
	finish := func(err error) error {
		if ferr := c.finishSnapshot(); ferr != nil && err == nil {
			return ferr
		}
		return err
	}

	for {
		select {
		case <-c.ctx.Done():
			return finish(c.ctx.Err())
		default:
		}

		block, err := r.Next()
		if pos := r.Position(); pos > lastPos {
			c.prog.tapePhysBytes.Add(pos - lastPos)
			lastPos = pos
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			if c.cfg.SnapshotSel >= 0 && c.snapshotIdx == c.cfg.SnapshotSel {
				c.logf("ignoring read error after selected snapshot entries: %v", err)
				return finish(nil)
			}
			return finish(fmt.Errorf("read block: %w", err))
		}

		switch block.Kind {
		case mtf.KindMedia:
			if block.Tape != nil {
				if c.meta.BackupTime.IsZero() {
					c.meta.BackupTime = block.Tape.CreateTime
				}
				if block.Tape.Name != "" {
					c.tapeLabel = block.Tape.Name
				}
				c.logf("Tape: %s, created %s", block.Tape.Name, block.Tape.CreateTime.Format("2006-01-02 15:04"))
			}
		case mtf.KindSet:
			if err := c.finishSnapshot(); err != nil {
				return err
			}
			c.snapshotIdx++
			c.meta = backupMeta{}
			c.rootPrefix = ""
			if block.Set != nil {
				c.meta.SetName = block.Set.Name
				c.meta.Owner = block.Set.Owner
				c.meta.BackupTime = block.Set.CreateTime
			}
			c.logf("SSET #%d: %q (%s)", c.snapshotIdx, c.meta.SetName, c.meta.BackupTime.Format("2006-01-02 15:04"))
		case mtf.KindSetEnd:
			if err := c.finishSnapshot(); err != nil {
				return err
			}
			if c.cfg.SnapshotSel >= 0 {
				return nil
			}
		case mtf.KindEntry:
			if !c.snapshotSelected() {
				continue
			}
			h := block.Header
			if h.Type == mtf.EntryVolume {
				c.meta.HostName = h.MachineName
				c.rootPrefix = h.Name
				if err := c.ensureSession(); err != nil {
					return finish(err)
				}
				continue
			}
			if err := c.walkEntry(r, h); err != nil {
				return finish(err)
			}
		}
	}
	if r.TruncatedByEOTM() {
		c.logf("WARNING: data set spans further media - use spanning and provide all tapes/files")
	}
	return finish(nil)
}

// walkEntry handles a directory, symlink, or file entry inline. Files are
// streamed from r (the mtf.Reader, positioned at the STAN data) straight into
// the pxar writer with no buffering. After WriteEntryReader returns, r has
// advanced past the file (its Read calls finishEntry on EOF), so the next
// walk iteration yields the following entry.
func (c *converter) walkEntry(r *mtf.Reader, h *mtf.Header) error {
	relPath := strings.TrimPrefix(strings.TrimPrefix(h.Name, c.rootPrefix), "/")
	name, depth := lastNameSegment(relPath)
	op := tapeOp{
		rootPfx: c.rootPrefix,
		relPath: relPath,
		name:    sanitizeName(name),
		depth:   depth,
	}
	switch h.Type {
	case mtf.EntryDirectory:
		c.prog.dirs.Add(1)
		op.kind = opDir
		op.meta = mtfToPxarMeta(h, format.ModeIFDIR, c.meta.BackupTime)
		c.logf("  d %s", relPath)
		if err := c.ensureSession(); err != nil {
			return err
		}
		return c.consumeDir(op)
	case mtf.EntryFile:
		c.prog.files.Add(1)
		if h.IsSymlink {
			op.kind = opSymlink
			op.meta = mtfToPxarMeta(h, format.ModeIFLNK, c.meta.BackupTime)
			op.linkTgt = h.LinkTarget
			c.logf("  l %s -> %s", relPath, h.LinkTarget)
			if err := c.ensureSession(); err != nil {
				return err
			}
			return c.consumeSymlink(op)
		}
		if err := c.ensureSession(); err != nil {
			return err
		}
		if h.IsHardLink {
			c.logf("  f %s (hardlink -> %s, %d bytes)", relPath, h.LinkTarget, h.Size)
		} else {
			c.logf("  f %s (%d bytes)", relPath, h.Size)
		}
		return c.streamFile(r, h, op)
	}
	return nil
}

// streamFile writes one file's content from the mtf.Reader directly to the
// pxar writer. r is positioned at the file's standard data stream; the pxar
// writer pulls bytes at ingest rate. The mtf.Reader serves data via Read and
// auto-advances past the entry when exhausted, so no manual positioning is
// needed afterwards.
func (c *converter) streamFile(r *mtf.Reader, h *mtf.Header, op tapeOp) error {
	for len(c.dirStack) > op.depth {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	entry := &pxar.Entry{Metadata: op.meta, Kind: pxar.KindFile, FileSize: uint64(h.Size)}
	entry.SetFileName(op.name)
	err := c.writer.WriteEntryReader(entry, r, uint64(h.Size))
	if err == nil {
		c.prog.bytes.Add(h.Size)
		c.prog.tapeBytes.Add(h.Size)
	}
	return err
}

func (c *converter) consumeDir(op tapeOp) error {
	if op.relPath == "" {
		return nil
	}
	for len(c.dirStack) > op.depth {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	meta := op.meta
	if err := c.writer.BeginDirectory(op.name, &meta); err != nil {
		return fmt.Errorf("begin dir %q: %w", op.name, err)
	}
	c.dirStack = append(c.dirStack, op.name)
	return nil
}

func (c *converter) consumeSymlink(op tapeOp) error {
	if op.relPath == "" {
		return nil
	}
	for len(c.dirStack) > op.depth {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	entry := &pxar.Entry{Metadata: op.meta, Kind: pxar.KindSymlink, LinkTarget: op.linkTgt}
	entry.SetFileName(op.name)
	return c.writer.WriteEntry(entry, nil)
}

func setupTapeContinuation(r *mtf.Reader, dev string) {
	r.SetContinuation(func(ct mtf.Continuation) (mtf.Tape, error) {
		fmt.Fprintf(os.Stderr, "\n== Insert tape %d (media %s) and press Enter ==\n",
			ct.Sequence+1, ct.Media.Name)
		var buf string
		if _, err := fmt.Scanln(&buf); err != nil {
			log.Error(err, "")
		}
		return OpenTapeReader(dev)
	})
}

func setupFileContinuation(r *mtf.Reader, files []string) {
	r.SetContinuation(func(ct mtf.Continuation) (mtf.Tape, error) {
		idx := ct.Sequence
		if idx >= len(files) {
			return nil, fmt.Errorf("sequence %d exceeds %d files", idx, len(files))
		}
		next, err := os.Open(files[idx])
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", files[idx], err)
		}
		return mtf.NewFileTape(next), nil
	})
}

func mtfToPxarMeta(h *mtf.Header, fileType uint64, fallbackTime time.Time) pxar.Metadata {
	var m pxar.Metadata
	m.Stat.Mode = fileType | (uint64(h.UnixMode()) &^ format.ModeIFMT)

	modTime := h.ModTime
	if modTime.IsZero() {
		if !h.CreateTime.IsZero() {
			modTime = h.CreateTime
		} else if !h.AccessTime.IsZero() {
			modTime = h.AccessTime
		} else if !h.BirthTime.IsZero() {
			modTime = h.BirthTime
		} else if !fallbackTime.IsZero() {
			modTime = fallbackTime
		}
	}
	m.Stat.Mtime = format.NewStatxTimestampFromTime(modTime)

	if !h.ModTime.IsZero() {
		m.XAttrs = append(m.XAttrs, format.NewXAttr(
			[]byte("user.lastwritetime"), []byte(strconv.FormatInt(h.ModTime.Unix(), 10))))
	}
	if !h.CreateTime.IsZero() {
		m.XAttrs = append(m.XAttrs, format.NewXAttr(
			[]byte("user.creationtime"), []byte(strconv.FormatInt(h.CreateTime.Unix(), 10))))
	}
	if !h.AccessTime.IsZero() {
		m.XAttrs = append(m.XAttrs, format.NewXAttr(
			[]byte("user.lastaccesstime"), []byte(strconv.FormatInt(h.AccessTime.Unix(), 10))))
	}

	if len(h.SecurityDescriptor) > 0 {
		if ownerSID := h.OwnerSID(); ownerSID != nil {
			m.Stat.UID, m.Stat.GID = mapSID(mtf.FormatSID(ownerSID))
		}
	}

	if len(h.ExtendedAttributes) > 0 {
		if xattrs := parseNTEA(h.ExtendedAttributes); len(xattrs) > 0 {
			m.XAttrs = append(m.XAttrs, xattrs...)
		}
	}

	return m
}

func mapSID(sid string) (uid, gid uint32) {
	switch sid {
	case "S-1-5-18", "S-1-5-19", "S-1-5-20", "S-1-5-32-544":
		return 0, 0
	case "S-1-5-32-545":
		return 1000, 1000
	default:
		parts := strings.Split(sid, "-")
		if len(parts) >= 3 {
			var n uint32
			if _, err := fmt.Sscanf(parts[len(parts)-1], "%d", &n); err != nil {
				log.Error(err, "")
			}
			if n > 0 {
				return n + 1000, n + 1000
			}
		}
		return 0, 0
	}
}

func collectBKFFiles(paths []string) ([]string, error) {
	var files []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", p, err)
		}
		if info.IsDir() {
			entries, err := os.ReadDir(p)
			if err != nil {
				return nil, fmt.Errorf("readdir %s: %w", p, err)
			}
			for _, e := range entries {
				if strings.HasSuffix(strings.ToLower(e.Name()), ".bkf") {
					files = append(files, filepath.Join(p, e.Name()))
				}
			}
		} else if strings.HasSuffix(strings.ToLower(p), ".bkf") {
			files = append(files, p)
		}
	}
	return files, nil
}

func lastNameSegment(relPath string) (name string, depth int) {
	lastSep := -1
	for i := 0; i < len(relPath); i++ {
		if relPath[i] == '/' {
			depth++
			lastSep = i
		}
	}
	return relPath[lastSep+1:], depth
}

func sanitizeName(name string) string {
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, "\\", "/")
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" || name == "." || name == ".." {
		name = "_"
	}
	return name
}

// sanitizePath replaces path-unsafe characters so a backup ID can be used
func sanitizePath(s string) string {
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, ":", "_")
	if s == "" {
		s = "_"
	}
	return s
}

func parseNTEA(data []byte) []format.XAttr {
	if len(data) < 4 {
		return nil
	}
	count := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
	if count == 0 || count > 256 {
		return nil
	}
	off := 4
	var xattrs []format.XAttr
	for i := 0; i < count && off+4 <= len(data); i++ {
		nameLen := int(uint16(data[off])|uint16(data[off+1])<<8) * 2
		valueLen := int(uint16(data[off+2]) | uint16(data[off+3])<<8)
		off += 4
		if off+nameLen > len(data) {
			break
		}
		nameBytes := data[off : off+nameLen]
		off += nameLen
		if off+valueLen > len(data) {
			break
		}
		valueBytes := data[off : off+valueLen]
		off += valueLen

		name := decodeUTF16LE(nameBytes)
		if name != "" {
			xattrs = append(xattrs, format.NewXAttr([]byte("user.ntea."+name), valueBytes))
		}
	}
	return xattrs
}

func decodeUTF16LE(data []byte) string {
	if len(data)%2 != 0 {
		return ""
	}
	runes := make([]rune, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		r := rune(uint16(data[i]) | uint16(data[i+1])<<8)
		if r == 0 {
			break
		}
		runes = append(runes, r)
	}
	return string(runes)
}
