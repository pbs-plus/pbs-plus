package tapeio

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	// NamespaceResolver overrides Namespace per snapshot. It receives the
	NamespaceResolver func(host, device string) string
	OnSnapshot        func(backupID, namespace string)
	TaskLog           func(string)
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
		rc, err := OpenTapeReader(cfg.TapeDevice)
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

	stopReport := c.prog.report(ctx, os.Stderr, 2*time.Second)
	defer stopReport()

	syncStats := func() {
		files, dirs, bytes := c.prog.snapshot()
		c.stats.Files = files
		c.stats.Dirs = dirs
		c.stats.Bytes = bytes
	}

	if cfg.TapeDevice != "" {
		c.logf("Starting tape migration: device=%s changer=%s", cfg.TapeDevice, cfg.ChangerDevice)
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
	rootPrefix string
	dirStack   []string
	currentNS  string

	snapshotIdx int // current SSET index (-1 before first)
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

func (c *converter) runTape() error {
	if c.cfg.ChangerDevice != "" {
		return c.runChanger()
	}
	rc, err := OpenTapeReader(c.cfg.TapeDevice)
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

	if c.cfg.SnapshotSel >= 0 {
		if _, err := r.Next(); err != nil {
			return fmt.Errorf("read TAPE descriptor: %w", err)
		}
		pba, ok, sErr := locateSnapshotPBA(rc, c.cfg.SnapshotSel)
		if sErr != nil {
			return sErr
		}
		if ok {
			c.logf("Locating to snapshot %d at PBA %d", c.cfg.SnapshotSel, pba)
			if err := r.SeekToBlock(pba); err != nil {
				return fmt.Errorf("seek to snapshot %d: %w", c.cfg.SnapshotSel, err)
			}
			c.snapshotIdx = c.cfg.SnapshotSel - 1
		}
	}

	return c.processReader(r)
}

func (c *converter) runChanger() error {
	f, err := NewFeeder(c.cfg.ChangerDevice, c.cfg.TapeDevice, c.cfg.DriveIndex)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		rc, _, _, err := f.LoadNext()
		if err != nil {
			return nil
		}
		r := mtf.NewReader(rc)
		r.SetContinuation(f.AsContinuation())
		if err := c.processReader(r); err != nil {
			if err := rc.Close(); err != nil {
				log.Error(err, "")
			}
			if err := f.UnloadCurrent(); err != nil {
				log.Error(err, "")
			}
			return err
		}
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
		if err := f.UnloadCurrent(); err != nil {
			log.Error(err, "")
		}
		f.MarkProcessed()
	}
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

// entries to pxar. Each SSET boundary starts a fresh session.
type tapeOp struct {
	kind    opKind
	header  *mtf.Header
	rootPfx string
	dataOff int64
	dataLen int64
	ssetIdx int
}

type opKind int

const (
	opVolume opKind = iota
	opDir
	opSymlink
	opFile
	opSet
	opSetEnd
	opEnd
)

func (c *converter) runPipeline(r *mtf.Reader) error {
	sp, err := newSpool(spoolCapBytes)
	if err != nil {
		return fmt.Errorf("create spool: %w", err)
	}
	defer func() {
		if err := sp.close(); err != nil {
			log.Error(err, "")
		}
	}()

	ops := make(chan tapeOp, opChanCap)
	pumpErr := make(chan error, 1)

	go func() {
		pumpErr <- c.pump(r, sp, ops)
	}()

	encErr := c.drain(ops, sp)

	if perr := <-pumpErr; perr != nil && encErr == nil {
		return perr
	}
	return encErr
}

func (c *converter) processReader(r *mtf.Reader) error {
	return c.runPipeline(r)
}

const (
	spoolCapBytes = 512 << 20
	opChanCap     = 256
)

func (c *converter) pump(r *mtf.Reader, sp *spool, ops chan<- tapeOp) error {
	finish := func(err error) error {
		ops <- tapeOp{kind: opEnd}
		return err
	}
	for {
		block, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return finish(fmt.Errorf("read block: %w", err))
		}

		switch block.Kind {
		case mtf.KindMedia:
			if block.Tape != nil {
				if c.meta.BackupTime.IsZero() {
					c.meta.BackupTime = block.Tape.CreateTime
				}
				c.logf("Tape: %s, created %s", block.Tape.Name, block.Tape.CreateTime.Format("2006-01-02 15:04"))
			}
		case mtf.KindSet:
			c.snapshotIdx++
			c.meta = backupMeta{}
			c.rootPrefix = ""
			if block.Set != nil {
				c.meta.SetName = block.Set.Name
				c.meta.Owner = block.Set.Owner
				c.meta.BackupTime = block.Set.CreateTime
			}
			c.logf("SSET #%d: %q (%s)", c.snapshotIdx, c.meta.SetName, c.meta.BackupTime.Format("2006-01-02 15:04"))
			ops <- tapeOp{kind: opSet, ssetIdx: c.snapshotIdx}
		case mtf.KindSetEnd:
			ops <- tapeOp{kind: opSetEnd}
		case mtf.KindEntry:
			if !c.snapshotSelected() {
				continue
			}
			h := block.Header
			if h.Type == mtf.EntryVolume {
				c.meta.HostName = h.MachineName
				c.rootPrefix = h.Name
				ops <- tapeOp{kind: opVolume, header: h, rootPfx: c.rootPrefix}
				continue
			}
			if err := c.pumpEntry(r, sp, ops, h); err != nil {
				return finish(err)
			}
		}
	}
	if r.TruncatedByEOTM() {
		c.logf("WARNING: data set spans further media - use spanning and provide all tapes/files")
	}
	return finish(nil)
}

func (c *converter) pumpEntry(r *mtf.Reader, sp *spool, ops chan<- tapeOp, h *mtf.Header) error {
	c.logf("Volume: machine=%s device=%s", h.MachineName, h.Name)
	op := tapeOp{kind: opDir, header: h, rootPfx: c.rootPrefix}
	switch h.Type {
	case mtf.EntryDirectory:
		c.prog.dirs.Add(1)
		op.kind = opDir
	case mtf.EntryFile:
		c.prog.files.Add(1)
		if h.IsSymlink {
			op.kind = opSymlink
		} else {
			op.kind = opFile
			off, err := sp.write(r, h.Size)
			if err != nil {
				return fmt.Errorf("spool file %q: %w", h.Name, err)
			}
			op.dataOff = off
			op.dataLen = h.Size
			c.prog.bytes.Add(h.Size)
		}
	}
	ops <- op
	return nil
}

func (c *converter) drain(ops <-chan tapeOp, sp *spool) error {
	for op := range ops {
		switch op.kind {
		case opSet:
			if err := c.finishSnapshot(); err != nil {
				return err
			}
			if c.cfg.SnapshotSel >= 0 && op.ssetIdx > c.cfg.SnapshotSel {
				return nil
			}
		case opSetEnd:
			if err := c.finishSnapshot(); err != nil {
				return err
			}
			if c.cfg.SnapshotSel >= 0 && c.snapshotIdx == c.cfg.SnapshotSel {
				return nil
			}
		case opVolume:
			c.meta.HostName = op.header.MachineName
			c.rootPrefix = op.rootPfx
			if err := c.ensureSession(); err != nil {
				return err
			}
		case opDir:
			if err := c.ensureSession(); err != nil {
				return err
			}
			if err := c.consumeDir(op.header); err != nil {
				return err
			}
		case opSymlink:
			if err := c.ensureSession(); err != nil {
				return err
			}
			if err := c.consumeSymlink(op.header); err != nil {
				return err
			}
		case opFile:
			if err := c.ensureSession(); err != nil {
				return err
			}
			if err := c.consumeFile(sp, op); err != nil {
				return err
			}
		case opEnd:
			return c.finishSnapshot()
		}
	}
	return c.finishSnapshot()
}

func (c *converter) consumeDir(h *mtf.Header) error {
	relPath := strings.TrimPrefix(h.Name, c.rootPrefix)
	relPath = strings.TrimPrefix(relPath, "/")
	if relPath == "" {
		return nil
	}
	components := strings.Split(relPath, "/")
	parent := components[:len(components)-1]
	name := sanitizeName(components[len(components)-1])
	for len(c.dirStack) > len(parent) {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	c.logf("  d %s", relPath)
	meta := mtfToPxarMeta(h, format.ModeIFDIR)
	if err := c.writer.BeginDirectory(name, &meta); err != nil {
		return fmt.Errorf("begin dir %q: %w", name, err)
	}
	c.dirStack = append(c.dirStack, name)
	return nil
}

func (c *converter) consumeSymlink(h *mtf.Header) error {
	relPath := strings.TrimPrefix(h.Name, c.rootPrefix)
	relPath = strings.TrimPrefix(relPath, "/")
	if relPath == "" {
		return nil
	}
	components := strings.Split(relPath, "/")
	name := sanitizeName(components[len(components)-1])
	for len(c.dirStack) > len(components)-1 {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	c.logf("  l %s -> %s", relPath, h.LinkTarget)
	meta := mtfToPxarMeta(h, format.ModeIFLNK)
	entry := &pxar.Entry{Metadata: meta, Kind: pxar.KindSymlink, LinkTarget: h.LinkTarget}
	entry.SetFileName(name)
	return c.writer.WriteEntry(entry, nil)
}

func (c *converter) consumeFile(sp *spool, op tapeOp) error {
	h := op.header
	relPath := strings.TrimPrefix(h.Name, op.rootPfx)
	relPath = strings.TrimPrefix(relPath, "/")
	if relPath == "" {
		return nil
	}
	components := strings.Split(relPath, "/")
	name := sanitizeName(components[len(components)-1])
	for len(c.dirStack) > len(components)-1 {
		if err := c.writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	meta := mtfToPxarMeta(h, format.ModeIFREG)
	entry := &pxar.Entry{Metadata: meta, Kind: pxar.KindFile, FileSize: uint64(h.Size)}
	entry.SetFileName(name)
	if h.IsHardLink && h.LinkTarget != "" {
		c.logf("  f %s (hardlink -> %s, %d bytes)", relPath, h.LinkTarget, h.Size)
	} else {
		c.logf("  f %s (%d bytes)", relPath, h.Size)
	}
	data := sp.reader(op.dataOff, op.dataLen)
	err := c.writer.WriteEntryReader(entry, data, uint64(h.Size))
	sp.consume(op.dataLen)
	return err
}

func locateSnapshotPBA(rc *TapeReader, sel int) (pba int64, ok bool, err error) {
	sm, sErr := mtf.ReadSetMap(rc)
	if sErr != nil {
		return 0, false, fmt.Errorf("read set map for snapshot locate: %w", sErr)
	}
	if sm == nil || sel < 0 || sel >= len(sm.Entries) {
		return 0, false, nil
	}
	return int64(sm.Entries[sel].SSETPBA) - 1, true, nil
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

func mtfToPxarMeta(h *mtf.Header, fileType uint64) pxar.Metadata {
	var m pxar.Metadata
	m.Stat.Mode = fileType | (uint64(h.UnixMode()) &^ format.ModeIFMT)
	m.Stat.Mtime = format.NewStatxTimestampFromTime(h.ModTime)

	if len(h.SecurityDescriptor) > 0 {
		if ownerSID := h.OwnerSID(); ownerSID != nil {
			m.Stat.UID, m.Stat.GID = mapSID(mtf.FormatSID(ownerSID))
		}
	}

	if len(h.ExtendedAttributes) > 0 {
		if xattrs := parseNTEA(h.ExtendedAttributes); len(xattrs) > 0 {
			m.XAttrs = xattrs
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
