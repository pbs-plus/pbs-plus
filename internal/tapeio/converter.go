package tapeio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/transfer"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"
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

const (
	opVolume opKind = iota
	opDir
	opSymlink
	opSet
	opSetEnd
	opEnd
)
