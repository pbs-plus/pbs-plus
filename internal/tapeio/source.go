package tapeio

import (
	"errors"
	"fmt"

	pxar "github.com/pbs-plus/pxar"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

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
