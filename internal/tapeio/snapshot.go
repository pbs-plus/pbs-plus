package tapeio

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

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

func (c *converter) finishSnapshot() error {
	if c.writer == nil {
		return nil
	}
	writer, session := c.writer, c.session
	c.writer, c.session = nil, nil
	defer writer.Close()
	defer session.Close()

	for len(c.dirStack) > 0 {
		if err := writer.EndDirectory(); err != nil {
			return err
		}
		c.dirStack = c.dirStack[:len(c.dirStack)-1]
	}
	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}
	if _, err := session.Finish(c.ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}
	files, dirs, bytes := c.prog.snapshot()
	c.logf("Snapshot complete: %d files, %d dirs, %d bytes", files, dirs, bytes)
	return nil
}

func (c *converter) snapshotSelected() bool {
	return c.cfg.SnapshotSel < 0 || c.snapshotIdx == c.cfg.SnapshotSel
}
