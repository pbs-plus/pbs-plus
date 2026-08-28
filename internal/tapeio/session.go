package tapeio

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"

	mtf "github.com/pbs-plus/go-mtf"
	_ "github.com/pbs-plus/go-mtf/besetmap"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
)

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
		MtimeTime(backupTime).
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
