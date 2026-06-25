package mtf

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	mtflib "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const pbsInventoryPath = conf.DbBasePath + "/tape/inventory.json"

type pbsMediaEntry struct {
	ID struct {
		Label struct {
			LabelText string `json:"label_text"`
		} `json:"label"`
	} `json:"id"`
}

func ListTapeLabels() (map[string]bool, error) {
	data, err := os.ReadFile(pbsInventoryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var entries []pbsMediaEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse PBS inventory: %w", err)
	}
	labels := make(map[string]bool, len(entries))
	for _, e := range entries {
		if e.ID.Label.LabelText != "" {
			labels[e.ID.Label.LabelText] = true
		}
	}
	return labels, nil
}

type Options struct {
	ChangerDevice string
	TapeDevice    string
	DriveIndex    int
	BKFPath       string
	Label         string
}

type Result struct {
	RunID      int64
	Cartridges int
	Families   int
	Duration   time.Duration
}

type TaskLogger interface {
	LogString(data string)
}

type Scanner struct {
	db      *store.Database
	taskLog TaskLogger
}

func NewScanner(db *store.Database) *Scanner {
	return &Scanner{db: db}
}

func (s *Scanner) ScanWithLog(ctx context.Context, opts Options, log TaskLogger) (*Result, error) {
	s.taskLog = log
	return s.Scan(ctx, opts)
}

func (s *Scanner) Scan(ctx context.Context, opts Options) (*Result, error) {
	if opts.ChangerDevice != "" && opts.TapeDevice == "" {
		return nil, errors.New("changer scan requires a tape drive device")
	}
	if opts.ChangerDevice == "" && opts.TapeDevice == "" && opts.BKFPath == "" {
		return nil, errors.New("no source: provide changer+drive, drive, or bkf path")
	}

	src := "drive:" + opts.TapeDevice
	if opts.ChangerDevice != "" {
		src = "changer:" + opts.ChangerDevice + " drive:" + opts.TapeDevice
	}
	if opts.BKFPath != "" {
		src = "bkf:" + opts.BKFPath
	}
	syslog.L.Info().WithMessage("mtf inventory scan starting").WithField("source", src).Write()

	runID, err := s.db.Queries().CreateInventoryRun(ctx, mtfquery.CreateInventoryRunParams{
		Changer:   nullStr(opts.ChangerDevice),
		StartedAt: nullInt(time.Now().Unix()),
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: create inventory run").Write()
		return nil, fmt.Errorf("create inventory run: %w", err)
	}

	start := time.Now()
	res := &Result{RunID: runID}
	var scanErr error

	switch {
	case opts.BKFPath != "":
		scanErr = s.scanBKFFile(ctx, opts.BKFPath, opts.Label, res)
	case opts.ChangerDevice != "":
		scanErr = s.scanChanger(ctx, opts, res)
	default:
		scanErr = s.scanDrive(ctx, opts.TapeDevice, "", res)
	}

	status := "completed"
	msg := ""
	if scanErr != nil {
		status = "failed"
		msg = scanErr.Error()
		syslog.L.Error(scanErr).WithMessage("mtf inventory scan failed").WithField("source", src).Write()
	} else {
		syslog.L.Info().WithMessage("mtf inventory scan completed").
			WithField("source", src).
			WithField("cartridges", res.Cartridges).
			WithField("families", res.Families).
			WithField("duration", res.Duration.String()).
			Write()
	}
	res.Duration = time.Since(start)
	if err := s.db.Queries().CompleteInventoryRun(ctx, mtfquery.CompleteInventoryRunParams{
		CompletedAt: nullInt(time.Now().Unix()),
		Status:      nullStr(status),
		Cartridges:  nullInt(int64(res.Cartridges)),
		Message:     nullStr(msg),
		ID:          runID,
	}); err != nil {
		syslog.L.Error(err).Write()
	}
	return res, scanErr
}

// scanChanger uses the same feeder pattern as bkf2pxar: load each tape,
func (s *Scanner) scanChanger(ctx context.Context, opts Options, res *Result) error {
	syslog.L.Info().WithMessage("mtf: opening changer").WithField("device", opts.ChangerDevice).Write()
	chg, err := changer.Open(opts.ChangerDevice)
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: open changer").WithField("device", opts.ChangerDevice).Write()
		return fmt.Errorf("open changer %s: %w", opts.ChangerDevice, err)
	}
	defer func() {
		if err := chg.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	st, err := chg.Status()
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: changer status").Write()
		return fmt.Errorf("changer status: %w", err)
	}

	pbsLabels, err := ListTapeLabels()
	if err != nil {
		syslog.L.Error(err).Write()
	}
	processed := make(map[string]bool)

	for i, slot := range st.Slots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !slot.Full || slot.ImportExport {
			continue
		}
		barcode := slot.VolumeTag
		if barcode == "" {
			barcode = fmt.Sprintf("SLOT%03d", i+1)
		}
		if bkf2pxar.IsCleaningTape(barcode) {
			continue
		}
		if processed[barcode] {
			continue
		}
		if pbsLabels != nil && pbsLabels[barcode] {
			syslog.L.Info().WithMessage("mtf: skipping PBS tape").WithField("barcode", barcode).Write()
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Slot %d: skipping PBS tape %s", i+1, barcode))
			}
			continue
		}

		syslog.L.Info().WithMessage("mtf: scanning cartridge").WithField("barcode", barcode).Write()
		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("Slot %d: loading %s...", i+1, barcode))
		}

		if err := chg.Load(st, i+1, opts.DriveIndex); err != nil {
			syslog.L.Error(err).WithMessage("mtf: load failed").WithField("barcode", barcode).Write()
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Slot %d: load failed  -  %v", i+1, err))
			}
			continue
		}
		loaded := true

		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("Slot %d: rewinding %s...", i+1, barcode))
		}
		rc, err := bkf2pxar.OpenTapeReader(opts.TapeDevice)
		if err != nil {
			syslog.L.Error(err).WithMessage("mtf: open tape").WithField("barcode", barcode).Write()
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Slot %d: open failed  -  %v", i+1, err))
			}
			if err := chg.Unload(st, opts.DriveIndex, i+1); err != nil {
				syslog.L.Error(err).Write()
			}
			continue
		}

		// Fast path: read Set Map (same as bkf2pxar -list).
		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("Slot %d: reading catalog...", i+1))
		}
		sm, err := mtflib.ReadSetMap(rc)
		if err != nil {
			syslog.L.Error(err).Write()
		}
		if sm != nil && len(sm.Entries) > 0 {
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Slot %d: found %d data sets via catalog", i+1, len(sm.Entries)))
			}
			if err := s.indexSetMap(ctx, rc, barcode, sm, res); err != nil {
				syslog.L.Error(err).WithMessage("mtf: index set map failed").WithField("barcode", barcode).Write()
				if s.taskLog != nil {
					s.taskLog.LogString(fmt.Sprintf("Slot %d: index failed  -  %v", i+1, err))
				}
			}
		} else {
			// Fallback: Census forward walk (only for tapes without Set Map).
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Slot %d: no catalog, doing full census...", i+1))
			}
			if err := rc.Rewind(); err != nil {
				syslog.L.Error(err).Write()
			}
			r := mtflib.NewReader(rc)
			if err := s.indexReader(ctx, r, barcode, false, "", res, func() error {
				return nil
			}); err != nil {
				syslog.L.Error(err).WithMessage("mtf: census failed").WithField("barcode", barcode).Write()
				if s.taskLog != nil {
					s.taskLog.LogString(fmt.Sprintf("Slot %d: census failed  -  %v", i+1, err))
				}
			}
		}
		if err := rc.Close(); err != nil {
			syslog.L.Error(err).Write()
		}

		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("Slot %d: unloading %s...", i+1, barcode))
		}
		if err := chg.Unload(st, opts.DriveIndex, i+1); err != nil {
			syslog.L.Error(err).WithMessage("mtf: unload failed").WithField("barcode", barcode).Write()
		} else {
			loaded = false
		}
		if loaded {
			if err := chg.Unload(st, opts.DriveIndex, i+1); err != nil {
				syslog.L.Error(err).Write()
			}
		}

		processed[barcode] = true
		syslog.L.Info().WithMessage("mtf: cartridge scanned").WithField("barcode", barcode).Write()
	}

	for dIdx, drive := range st.Drives {
		if !drive.Full {
			continue
		}
		barcode := drive.VolumeTag
		if barcode == "" {
			barcode = fmt.Sprintf("DRIVE%02d", dIdx)
		}
		if bkf2pxar.IsCleaningTape(barcode) || processed[barcode] {
			continue
		}
		if pbsLabels != nil && pbsLabels[barcode] {
			syslog.L.Info().WithMessage("mtf: skipping PBS tape in drive").WithField("barcode", barcode).Write()
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Drive %d: skipping PBS tape %s", dIdx, barcode))
			}
			continue
		}
		msg := fmt.Sprintf("Drive %d: scanning %s...", dIdx, barcode)
		syslog.L.Info().WithMessage("mtf: scanning tape in drive").WithField("barcode", barcode).Write()
		if s.taskLog != nil {
			s.taskLog.LogString(msg)
		}
		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("Drive %d: rewinding...", dIdx))
		}
		rc, err := bkf2pxar.OpenTapeReader(opts.TapeDevice)
		if err != nil {
			syslog.L.Error(err).WithMessage("mtf: open tape in drive").WithField("barcode", barcode).Write()
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Drive %d: open failed  -  %v", dIdx, err))
			}
			continue
		}
		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("Drive %d: reading catalog...", dIdx))
		}
		sm, err := mtflib.ReadSetMap(rc)
		if err != nil {
			syslog.L.Error(err).Write()
		}
		if sm != nil && len(sm.Entries) > 0 {
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Drive %d: found %d data sets via catalog", dIdx, len(sm.Entries)))
			}
			if err := s.indexSetMap(ctx, rc, barcode, sm, res); err != nil {
				syslog.L.Error(err).WithMessage("mtf: index drive tape failed").WithField("barcode", barcode).Write()
				if s.taskLog != nil {
					s.taskLog.LogString(fmt.Sprintf("Drive %d: index failed  -  %v", dIdx, err))
				}
			}
		} else {
			if s.taskLog != nil {
				s.taskLog.LogString(fmt.Sprintf("Drive %d: no catalog, doing full census...", dIdx))
			}
			if err := rc.Rewind(); err != nil {
				syslog.L.Error(err).Write()
			}
			r := mtflib.NewReader(rc)
			if err := s.indexReader(ctx, r, barcode, false, "", res, func() error { return nil }); err != nil {
				syslog.L.Error(err).WithMessage("mtf: census drive tape failed").WithField("barcode", barcode).Write()
				if s.taskLog != nil {
					s.taskLog.LogString(fmt.Sprintf("Drive %d: census failed  -  %v", dIdx, err))
				}
			}
		}
		if err := rc.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		processed[barcode] = true
	}

	return nil
}

func (s *Scanner) indexSetMap(ctx context.Context, rc *mtflib.DriveTape, barcode string, sm *mtflib.SetMap, res *Result) error {
	if s.taskLog != nil {
		s.taskLog.LogString("  Reading TAPE header...")
	}
	// Rewind to BOT, read just the first TAPE block (no full Census).
	if err := rc.Rewind(); err != nil {
		syslog.L.Error(err).Write()
	}
	r := mtflib.NewReader(rc)
	blk, err := r.Next()
	if err != nil {
		return fmt.Errorf("read TAPE block: %w", err)
	}
	if blk.Kind != mtflib.KindMedia || blk.Tape == nil {
		return fmt.Errorf("first block is not a TAPE block")
	}

	fam := r.Family()
	famID := int64(fam.ID)
	if famID == 0 {
		famID = syntheticFamilyID(barcode)
	}
	famName := fam.TapeName
	if famName == "" {
		famName = barcode
	}

	if s.taskLog != nil {
		s.taskLog.LogString(fmt.Sprintf("  Media family: %s (ID %d, %d tapes)", famName, famID, fam.TotalTapes))
	}

	if _, err := s.db.Queries().UpsertMediaFamily(ctx, mtfquery.UpsertMediaFamilyParams{
		ID:          famID,
		Name:        nullStr(famName),
		TotalTapes:  nullInt(int64(maxi(fam.TotalTapes, int(fam.TapeSequence)))),
		HasCatalog:  nullInt(1),
		LastScanned: nullInt(time.Now().Unix()),
	}); err != nil {
		return fmt.Errorf("upsert family: %w", err)
	}

	if err := s.db.Queries().UpsertCartridge(ctx, mtfquery.UpsertCartridgeParams{
		Barcode:       barcode,
		Label:         nullStr(blk.Tape.Name),
		MediaFamilyID: famID,
		Sequence:      nullInt(int64(blk.Tape.Sequence)),
		Role:          nullStr("data"),
		CatalogType:   nullInt(int64(blk.Tape.CatalogType)),
		HasCatalog:    nullInt(1),
		Status:        nullStr("online"),
		LastScanned:   nullInt(time.Now().Unix()),
	}); err != nil {
		return fmt.Errorf("upsert cartridge: %w", err)
	}
	res.Cartridges++

	if sm != nil {
		res.Families++
		if s.taskLog != nil {
			s.taskLog.LogString(fmt.Sprintf("  Indexing %d data set entries...", len(sm.Entries)))
		}
		if err := s.indexSetMapEntries(ctx, famID, sm, int(blk.Tape.Sequence)); err != nil {
			return fmt.Errorf("index set map: %w", err)
		}
	}
	return nil
}

func (s *Scanner) scanDrive(ctx context.Context, dev, barcode string, res *Result) error {
	isPBS, err := bkf2pxar.IsPBSTape(dev)
	if err != nil {
		syslog.L.Error(err).WithMessage("mtf: check tape type").WithField("device", dev).Write()
		return fmt.Errorf("check tape type: %w", err)
	}
	if isPBS {
		syslog.L.Info().WithMessage("mtf: skipping PBS-formatted tape").WithField("device", dev).Write()
		if s.taskLog != nil {
			s.taskLog.LogString("Skipping PBS-formatted tape")
		}
		return nil
	}

	syslog.L.Info().WithMessage("mtf: reading tape").WithField("device", dev).Write()
	if s.taskLog != nil {
		s.taskLog.LogString(fmt.Sprintf("Reading tape from %s...", dev))
	}
	rc, err := bkf2pxar.OpenTapeReader(dev)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()
	if barcode == "" {
		barcode = "TAPE0001"
	}

	sm, err := mtflib.ReadSetMap(rc)

	if err != nil {
		syslog.L.Error(err).Write()
	}

	if sm != nil && len(sm.Entries) > 0 {
		return s.indexSetMap(ctx, rc, barcode, sm, res)
	}
	// Fallback.
	if err := rc.Rewind(); err != nil {
		syslog.L.Error(err).Write()
	}
	r := mtflib.NewReader(rc)
	return s.indexReader(ctx, r, barcode, false, "", res, func() error { return nil })
}

func (s *Scanner) scanBKFFile(ctx context.Context, path, label string, res *Result) error {
	syslog.L.Info().WithMessage("mtf: scanning BKF").WithField("path", path).Write()
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	var files []string
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if strings.HasSuffix(strings.ToLower(e.Name()), ".bkf") {
				files = append(files, path+"/"+e.Name())
			}
		}
	} else {
		files = []string{path}
	}
	if len(files) == 0 {
		return errors.New("no .bkf files found")
	}

	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := mtflib.Open(f)
		if err != nil {
			return fmt.Errorf("open %s: %w", f, err)
		}
		bc := bkfBarcode(f)
		if err := s.indexReader(ctx, r, bc, true, f, res, func() error { return nil }); err != nil {
			if err := r.Close(); err != nil {
				syslog.L.Error(err).Write()
			}
			return err
		}
		if err := r.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}
	return nil
}

func (s *Scanner) indexReader(ctx context.Context, r *mtflib.Reader, barcode string, isBKF bool, srcPath string, res *Result, onDone func() error) error {
	cen, err := r.Census()
	if onDoneErr := onDone(); onDoneErr != nil && err == nil {
		err = onDoneErr
	}
	if err != nil && cen.Tape == nil {
		syslog.L.Error(err).WithMessage("mtf: census failed").WithField("barcode", barcode).Write()
		return fmt.Errorf("census: %w", err)
	}

	fam := r.Family()
	famID := int64(fam.ID)
	if famID == 0 {
		famID = syntheticFamilyID(barcode)
	}

	famName := fam.TapeName
	if famName == "" {
		famName = barcode
	}

	if _, err := s.db.Queries().UpsertMediaFamily(ctx, mtfquery.UpsertMediaFamilyParams{
		ID:          famID,
		Name:        nullStr(famName),
		TotalTapes:  nullInt(int64(maxi(fam.TotalTapes, int(fam.TapeSequence)))),
		HasCatalog:  mtfqueryHasCatalog(fam),
		LastScanned: nullInt(time.Now().Unix()),
	}); err != nil {
		return fmt.Errorf("upsert family: %w", err)
	}

	if err := s.db.Queries().UpsertCartridge(ctx, cartridgeParams(barcode, famID, cen, isBKF, srcPath)); err != nil {
		return fmt.Errorf("upsert cartridge: %w", err)
	}
	res.Cartridges++

	if fam.SetMap != nil {
		res.Families++
		if err := s.indexSetMapEntries(ctx, famID, fam.SetMap, int(cen.MediaSequence)); err != nil {
			return fmt.Errorf("index set map: %w", err)
		}
	}
	return nil
}

func (s *Scanner) indexSetMapEntries(ctx context.Context, famID int64, sm *mtflib.SetMap, tapeSeq int) error {
	for _, e := range sm.Entries {
		var machine string
		for _, v := range e.Volumes {
			if v.MachineName != "" {
				machine = v.MachineName
				break
			}
		}
		dsID, err := s.db.Queries().UpsertDataSet(ctx, mtfquery.UpsertDataSetParams{
			MediaFamilyID:  famID,
			SetNumber:      nullInt(int64(e.SetNumber)),
			Name:           nullStr(e.Name),
			Description:    nullStr(e.Description),
			Owner:          nullStr(e.Owner),
			MachineName:    nullStr(machine),
			WriteTime:      nullInt(e.WriteTime.Unix()),
			NumDirectories: nullInt(int64(e.NumDirectories)),
			NumFiles:       nullInt(int64(e.NumFiles)),
			NumCorrupt:     nullInt(int64(e.NumCorrupt)),
			Size:           nullInt(0),
			FirstMediaSeq:  nullInt(int64(e.MediaSeq)),
			SourceMediaSeq: nullInt(int64(tapeSeq)),
		})
		if err != nil {
			return fmt.Errorf("upsert data set %d: %w", e.SetNumber, err)
		}
		if tapeSeq >= int(e.MediaSeq) {
			if _, err := s.db.Queries().DeleteVolumesByDataSet(ctx, dsID); err != nil {
				syslog.L.Error(err).Write()
			}
			for _, v := range e.Volumes {
				if err := s.db.Queries().CreateDataSetVolume(ctx, mtfquery.CreateDataSetVolumeParams{
					DataSetID:   dsID,
					Device:      nullStr(v.Name),
					VolumeLabel: nullStr(v.VolumeLabel),
					MachineName: nullStr(v.MachineName),
				}); err != nil {
					return fmt.Errorf("create volume: %w", err)
				}
			}
		}
	}
	return nil
}

func cartridgeParams(barcode string, famID int64, cen mtflib.Census, isBKF bool, srcPath string) mtfquery.UpsertCartridgeParams {
	role := "unknown"
	switch cen.Role {
	case mtflib.RoleData:
		role = "data"
	case mtflib.RoleCatalog:
		role = "catalog"
	}
	label := ""
	if cen.Tape != nil {
		label = cen.Tape.Name
	}
	return mtfquery.UpsertCartridgeParams{
		Barcode:         barcode,
		Label:           nullStr(label),
		MediaFamilyID:   famID,
		Sequence:        nullInt(int64(cen.MediaSequence)),
		Role:            nullStr(role),
		CatalogType:     nullInt(int64(cen.CatalogType)),
		IsBkfFile:       mtfqueryBool(isBKF),
		SourcePath:      nullStr(srcPath),
		Volumes:         nullInt(int64(cen.Volumes)),
		Directories:     nullInt(int64(cen.Directories)),
		Files:           nullInt(int64(cen.Files)),
		EmptyFiles:      nullInt(int64(cen.EmptyFiles)),
		FileBytes:       nullInt(cen.FileBytes),
		SparseFiles:     nullInt(int64(cen.SparseFiles)),
		CompressedFiles: nullInt(int64(cen.CompressedFiles)),
		EncryptedFiles:  nullInt(int64(cen.EncryptedFiles)),
		HasCatalog:      mtfqueryBool(cen.HasCatalog),
		CatalogBytes:    nullInt(cen.CatalogBytes),
		SetsClosed:      nullInt(int64(cen.SetsClosed)),
		Status:          nullStr("online"),
		LastScanned:     nullInt(time.Now().Unix()),
	}
}

func mtfqueryHasCatalog(f mtflib.MediaFamily) sql.NullInt64 {
	if f.SetMap != nil || f.TotalTapes > 0 {
		return nullInt(1)
	}
	return nullInt(0)
}

func bkfBarcode(path string) string {
	base := path
	if idx := strings.LastIndexByte(base, '/'); idx >= 0 {
		base = base[idx+1:]
	}
	base = strings.TrimSuffix(base, ".bkf")
	base = strings.TrimSuffix(base, ".BKF")
	if base == "" {
		base = "bkf"
	}
	return "BKF:" + base
}

func syntheticFamilyID(barcode string) int64 {
	var h int64 = 5381
	for _, c := range []byte(barcode) {
		h = h*33 + int64(c)
	}
	if h < 0 {
		h = -h
	}
	if h == 0 {
		h = 1
	}
	return h
}

func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
}
