package mtfinv

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/server/mtfstore"
	"github.com/pbs-plus/pbs-plus/internal/server/mtfstore/mtfquery"
)

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

type Scanner struct {
	db *mtfstore.Database
}

func NewScanner(db *mtfstore.Database) *Scanner {
	return &Scanner{db: db}
}

func (s *Scanner) Scan(ctx context.Context, opts Options) (*Result, error) {
	if opts.ChangerDevice != "" && opts.TapeDevice == "" {
		return nil, errors.New("changer scan requires a tape drive device")
	}
	if opts.ChangerDevice == "" && opts.TapeDevice == "" && opts.BKFPath == "" {
		return nil, errors.New("no source: provide changer+drive, drive, or bkf path")
	}

	runID, err := s.db.Queries().CreateInventoryRun(ctx, mtfquery.CreateInventoryRunParams{
		Changer:   nullStr(opts.ChangerDevice),
		StartedAt: nullInt(time.Now().Unix()),
	})
	if err != nil {
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
	}
	res.Duration = time.Since(start)
	_ = s.db.Queries().CompleteInventoryRun(ctx, mtfquery.CompleteInventoryRunParams{
		CompletedAt: nullInt(time.Now().Unix()),
		Status:      nullStr(status),
		Cartridges:  nullInt(int64(res.Cartridges)),
		Message:     nullStr(msg),
		ID:          runID,
	})
	return res, scanErr
}

func (s *Scanner) scanChanger(ctx context.Context, opts Options, res *Result) error {
	chg, err := changer.Open(opts.ChangerDevice)
	if err != nil {
		return fmt.Errorf("open changer %s: %w", opts.ChangerDevice, err)
	}
	defer func() { _ = chg.Close() }()

	if err := chg.Inventory(); err != nil {
		return fmt.Errorf("changer inventory: %w", err)
	}

	st, err := chg.Status()
	if err != nil {
		return fmt.Errorf("changer status: %w", err)
	}

	for i, slot := range st.Slots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !slot.Full || slot.ImportExport {
			continue
		}
		bc := slot.VolumeTag
		if bc != "" && bkf2pxar.IsCleaningTape(bc) {
			continue
		}
		barcode := bc
		if barcode == "" {
			barcode = fmt.Sprintf("SLOT%03d", i+1)
		}
		if err := s.scanLoadedTape(ctx, chg, st, i+1, opts, barcode, res); err != nil {
			return fmt.Errorf("slot %d (%s): %w", i+1, barcode, err)
		}
	}
	return nil
}

func (s *Scanner) scanLoadedTape(ctx context.Context, chg *changer.Changer, st *changer.Status, slot int, opts Options, barcode string, res *Result) error {
	if err := chg.Load(st, slot, opts.DriveIndex); err != nil {
		return fmt.Errorf("load: %w", err)
	}
	loaded := true
	defer func() {
		if loaded {
			_ = chg.Unload(st, opts.DriveIndex, slot)
		}
	}()

	rc, err := bkf2pxar.OpenTapeReader(opts.TapeDevice)
	if err != nil {
		return fmt.Errorf("open tape: %w", err)
	}
	r := mtf.NewReader(rc)
	return s.indexReader(ctx, r, barcode, false, "", res, func() error {
		_ = rc.Close()
		if err := chg.Unload(st, opts.DriveIndex, slot); err == nil {
			loaded = false
		}
		return nil
	})
}

func (s *Scanner) scanDrive(ctx context.Context, dev, barcode string, res *Result) error {
	rc, err := bkf2pxar.OpenTapeReader(dev)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if barcode == "" {
		barcode = "TAPE0001"
	}
	r := mtf.NewReader(rc)
	return s.indexReader(ctx, r, barcode, false, "", res, func() error { return nil })
}

func (s *Scanner) scanBKFFile(ctx context.Context, path, label string, res *Result) error {
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
		r, err := mtf.Open(f)
		if err != nil {
			return fmt.Errorf("open %s: %w", f, err)
		}
		bc := bkfBarcode(f)
		if err := s.indexReader(ctx, r, bc, true, f, res, func() error { return nil }); err != nil {
			_ = r.Close()
			return err
		}
		_ = r.Close()
	}
	return nil
}

// indexReader runs a header-only Census over one cartridge and persists its
// summary plus its Media Family / data-set index. onDone is invoked after the
// census regardless of outcome (e.g. to close/unload the cartridge).
func (s *Scanner) indexReader(ctx context.Context, r *mtf.Reader, barcode string, isBKF bool, srcPath string, res *Result, onDone func() error) error {
	cen, err := r.Census()
	if onDoneErr := onDone(); onDoneErr != nil && err == nil {
		err = onDoneErr
	}
	if err != nil && cen.Tape == nil {
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
		if err := s.indexSetMap(ctx, famID, fam.SetMap); err != nil {
			return fmt.Errorf("index set map: %w", err)
		}
	}
	return nil
}

// indexSetMap persists the Set Map's data-set entries (one per SSET). Because
// the Set Map is cumulative and rewritten on each cartridge, later scans
// supersede earlier ones for the same (family, set_number). We replace that
// family's data sets on each scan to reflect the most complete catalog seen.
func (s *Scanner) indexSetMap(ctx context.Context, famID int64, sm *mtf.SetMap) error {
	if _, err := s.db.Queries().DeleteDataSetsByFamily(ctx, famID); err != nil {
		return err
	}
	for _, e := range sm.Entries {
		var machine string
		for _, v := range e.Volumes {
			if v.MachineName != "" {
				machine = v.MachineName
				break
			}
		}
		dsID, err := s.db.Queries().CreateDataSet(ctx, mtfquery.CreateDataSetParams{
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
			Size:           nullInt(int64(e.Size)),
			FirstMediaSeq:  nullInt(int64(e.MediaSeq)),
		})
		if err != nil {
			return err
		}
		for _, v := range e.Volumes {
			if err := s.db.Queries().CreateDataSetVolume(ctx, mtfquery.CreateDataSetVolumeParams{
				DataSetID:   dsID,
				Device:      nullStr(v.Name),
				VolumeLabel: nullStr(v.VolumeLabel),
				MachineName: nullStr(v.MachineName),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func cartridgeParams(barcode string, famID int64, cen mtf.Census, isBKF bool, srcPath string) mtfquery.UpsertCartridgeParams {
	role := "unknown"
	switch cen.Role {
	case mtf.RoleData:
		role = "data"
	case mtf.RoleCatalog:
		role = "catalog"
	}
	label := ""
	if cen.Tape != nil {
		label = cen.Tape.Name
	}
	p := mtfquery.UpsertCartridgeParams{
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
	return p
}

func mtfqueryHasCatalog(f mtf.MediaFamily) sql.NullInt64 {
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
