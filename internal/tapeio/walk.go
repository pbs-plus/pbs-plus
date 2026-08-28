package tapeio

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"
)

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
		op.meta = mtfToPxarMeta(h, format.ModeIFREG, c.meta.BackupTime)
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
