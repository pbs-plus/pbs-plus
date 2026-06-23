package bkf2pxar

import (
	"bufio"
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
	_ "github.com/pbs-plus/go-mtf/besetmap" // register Backup Exec SMP2 Set Map parser

	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
)

type Config struct {
	PBSURL      string
	Datastore   string
	Namespace   string
	AuthToken   string
	SkipTLS     bool
	BackupID    string
	ArchiveName string
	LocalDir    string
	Sources     []string
	TapeDevice  string
	// ChangerDevice enables auto-changer mode: the SCSI Medium Changer device
	// (e.g. /dev/sg1) used to load/unload tapes robotically. Requires TapeDevice
	// to name the tape drive (e.g. /dev/nst0) and DriveIndex to select it.
	ChangerDevice string
	// DriveIndex is the 0-based index of the target tape drive within the
	// changer (almost always 0 for single-drive libraries).
	DriveIndex int
	Verbose    bool
	Compress   bool
	Spanning   bool
	// SnapshotSel selects a single snapshot (SSET) to migrate.
	// Negative = migrate all snapshots (default). Each snapshot becomes
	// its own PBS backup point.
	SnapshotSel int
	// IgnoreNewerPrevious skips previous-backup chunk registration when the
	// backup time is older than existing PBS snapshots. Essential for tape
	// migration where original backup times are in the past.
	IgnoreNewerPrevious bool
	// NamespaceResolver overrides Namespace per snapshot. It receives the
	// snapshot's source machine name and volume device path (e.g.
	// \\HOST.sgl.lan\D:) and returns the PBS namespace to use for that
	// backup point. When nil or when it returns "", Namespace is used.
	NamespaceResolver func(host, device string) string
	// OnSnapshot is invoked when a snapshot's session has started, with the
	// resolved backup-id and namespace, so callers can report progress.
	OnSnapshot func(backupID, namespace string)
	// TaskLog, when set, receives verbose progress messages (one per line)
	// intended for the task log viewer.
	TaskLog func(string)
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

// --- List mode ---

// ListSnapshots scans input sources and returns all backup sets (SSETs)
// in stream order, visiting only structural blocks (no file data).
func ListSnapshots(ctx context.Context, cfg Config) ([]Snapshot, error) {
	_ = ctx
	var snapshots []Snapshot

	if cfg.TapeDevice != "" {
		rc, err := openTapeReader(cfg.TapeDevice)
		if err != nil {
			return nil, err
		}
		if sm, _ := mtf.ReadSetMap(rc); sm != nil && len(sm.Entries) > 0 {
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
			_ = rc.Close()
			return snapshots, nil
		}
		_ = rc.Rewind()
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
			_ = r.Close()
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
				_ = r.Close()
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

// --- Conversion mode ---

// Run performs the full conversion: reads BKF sources, builds pxar archive(s),
// and uploads to PBS (or writes to a local store).
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
// first real entry, once metadata has been collected.
func (c *converter) ensureSession() error {
	if c.session != nil {
		return nil
	}

	backupID := c.cfg.BackupID
	if backupID == "" {
		backupID = c.meta.HostName
	}
	if backupID == "" {
		h, _ := os.Hostname()
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
	}, c.chunkCfg, true)

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
// snapshot's archive + session.
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
	rc, err := openTapeReader(c.cfg.TapeDevice)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	r := mtf.NewReader(rc)
	if c.cfg.Spanning {
		setupTapeContinuation(r, c.cfg.TapeDevice)
	}

	return c.processReader(r)
}

// runChanger migrates all data tapes in the magazine using the robotic
// changer. EOTM continuations are resolved by scanning slots for the matching
// media-family sequence; when a media set is exhausted, the next independent
// backup set starts automatically.
func (c *converter) runChanger() error {
	f, err := newFeeder(c.cfg.ChangerDevice, c.cfg.TapeDevice, c.cfg.DriveIndex)
	if err != nil {
		return err
	}
	defer f.close()

	for {
		rc, err := f.loadFirst()
		if err != nil {
			return nil
		}
		r := mtf.NewReader(rc)
		r.SetContinuation(f.asContinuation())
		if err := c.processReader(r); err != nil {
			_ = rc.Close()
			_ = f.unloadCurrent()
			return err
		}
		_ = rc.Close()
		_ = f.unloadCurrent()
		f.markProcessed()
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
		_ = r.Close()
		return perr
	}

	for _, f := range files {
		r, err := mtf.Open(f)
		if err != nil {
			return fmt.Errorf("open %s: %w", f, err)
		}
		perr := c.processReader(r)
		_ = r.Close()
		if perr != nil {
			return fmt.Errorf("process %s: %w", f, perr)
		}
	}
	return nil
}

// processReader iterates one MTF reader, collecting metadata and converting
// entries to pxar. Each SSET boundary starts a fresh session.
func (c *converter) processReader(r *mtf.Reader) error {
	for {
		block, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read block: %w", err)
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
			if err := c.finishSnapshot(); err != nil {
				return err
			}
			c.snapshotIdx++
			c.meta = backupMeta{}
			c.rootPrefix = ""
			c.dirStack = nil
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

		case mtf.KindEntry:
			if !c.snapshotSelected() {
				continue
			}
			h := block.Header
			if h.Type == mtf.EntryVolume {
				c.meta.HostName = h.MachineName
				c.rootPrefix = h.Name
				// Start the PBS session before file data is read, overlapping the
				// H2 connect + index create + download-previous with tape positioning.
				if err := c.ensureSession(); err != nil {
					return err
				}
				continue
			}
			if err := c.ensureSession(); err != nil {
				return err
			}
			c.logf("Volume: machine=%s device=%s", h.MachineName, h.Name)
			if err := c.processEntry(r, h); err != nil {
				return err
			}
		}
	}

	if r.TruncatedByEOTM() {
		c.logf("WARNING: data set spans further media - use spanning and provide all tapes/files")
	}

	return c.finishSnapshot()
}

func (c *converter) processEntry(r io.Reader, h *mtf.Header) error {
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

	switch h.Type {
	case mtf.EntryDirectory:
		c.prog.dirs.Add(1)
		meta := mtfToPxarMeta(h, format.ModeIFDIR)
		c.logf("  d %s", relPath)
		if err := c.writer.BeginDirectory(name, &meta); err != nil {
			return fmt.Errorf("begin dir %q: %w", name, err)
		}
		c.dirStack = append(c.dirStack, name)

	case mtf.EntryFile:
		c.prog.files.Add(1)
		if h.IsSymlink {
			c.logf("  l %s -> %s", relPath, h.LinkTarget)
			if err := c.writeSymlink(h, name, relPath); err != nil {
				return err
			}
		} else {
			if err := c.writeFile(r, h, name, relPath); err != nil {
				return err
			}
			c.prog.bytes.Add(h.Size)
		}
	}

	return nil
}

func (c *converter) writeSymlink(h *mtf.Header, name, relPath string) error {
	meta := mtfToPxarMeta(h, format.ModeIFLNK)
	entry := &pxar.Entry{
		Metadata:   meta,
		Kind:       pxar.KindSymlink,
		LinkTarget: h.LinkTarget,
	}
	entry.SetFileName(name)
	return c.writer.WriteEntry(entry, nil)
}

func (c *converter) writeFile(r io.Reader, h *mtf.Header, name, relPath string) error {
	meta := mtfToPxarMeta(h, format.ModeIFREG)
	entry := &pxar.Entry{
		Metadata: meta,
		Kind:     pxar.KindFile,
		FileSize: uint64(h.Size),
	}
	entry.SetFileName(name)
	if h.IsHardLink && h.LinkTarget != "" {
		c.logf("  f %s (hardlink -> %s, %d bytes)", relPath, h.LinkTarget, h.Size)
	} else {
		c.logf("  f %s (%d bytes)", relPath, h.Size)
	}

	// Decouple tape reads from the encoder: a goroutine pumps the tape into
	// an io.Pipe (32 MiB buffer) so the drive streams in large bursts instead
	// of stop/start cycles that limit throughput.
	pr, pw := io.Pipe()
	go func() {
		defer func() { _ = pw.Close() }()
		bw := bufio.NewWriterSize(pw, 32<<20)
		_, _ = io.Copy(bw, r)
		_ = bw.Flush()
	}()
	return c.writer.WriteEntryReader(entry, pr, uint64(h.Size))
}

// --- Source opening helpers ---

func setupTapeContinuation(r *mtf.Reader, dev string) {
	r.SetContinuation(func(ct mtf.Continuation) (mtf.Tape, error) {
		fmt.Fprintf(os.Stderr, "\n== Insert tape %d (media %s) and press Enter ==\n",
			ct.Sequence+1, ct.Media.Name)
		var buf string
		fmt.Scanln(&buf) //nolint:errcheck
		rc, err := openTapeReader(dev)
		if err != nil {
			return nil, err
		}
		return rc, nil
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

// --- Metadata conversion helpers ---

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
			fmt.Sscanf(parts[len(parts)-1], "%d", &n) //nolint:errcheck
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
// as a directory name.
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
