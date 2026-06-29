package mtf

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	mtflib "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
	"github.com/pbs-plus/pbs-plus/internal/tapeio"

	"github.com/pbs-plus/pbs-plus/internal/log"
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
		return nil, err
	}
	labels := make(map[string]bool)
	for _, e := range entries {
		if e.ID.Label.LabelText != "" {
			labels[e.ID.Label.LabelText] = true
		}
	}
	return labels, nil
}

type Options struct {
	TapeDevice    string
	ChangerDevice string
	DriveIndex    int
	BKFPath       string
	Label         string
}

type Result struct {
	Cartridges int
	Families   int
	DataSets   int
	Duration   time.Duration
}

type Scanner struct {
	db     *store.Database
	logger *log.Logger
}

func NewScanner(db *store.Database) *Scanner {
	return &Scanner{db: db}
}

func (s *Scanner) ScanWithLog(ctx context.Context, opts Options, logger *log.Logger) (*Result, error) {
	s.logger = logger
	return s.Scan(ctx, opts)
}

func (s *Scanner) Scan(ctx context.Context, opts Options) (*Result, error) {
	if opts.ChangerDevice != "" && opts.TapeDevice == "" {
		return nil, ErrChangerRequiresDrive
	}
	if opts.ChangerDevice == "" && opts.TapeDevice == "" && opts.BKFPath == "" {
		return nil, ErrNoSource
	}

	res := &Result{}

	var src string
	switch {
	case opts.ChangerDevice != "":
		src = "changer:" + opts.ChangerDevice + " drive:" + opts.TapeDevice
	case opts.TapeDevice != "":
		src = "tape:" + opts.TapeDevice
	default:
		src = "bkf:" + opts.BKFPath
	}

	s.logger.Info("mtf: starting scan", "source", src)

	runStart := time.Now()

	var scanErr error
	switch {
	case opts.ChangerDevice != "":
		scanErr = s.scanChanger(ctx, opts, res)
	case opts.TapeDevice != "":
		scanErr = s.scanDrive(ctx, opts.TapeDevice, "", res)
	default:
		scanErr = s.scanBKFFile(ctx, opts.BKFPath, "", res)
	}
	res.Duration = time.Since(runStart)

	if scanErr != nil {
		s.logger.Error(scanErr, "mtf: scan failed")
		return res, scanErr
	}

	s.logger.Info("mtf: scan complete",
		"cartridges", res.Cartridges,
		"families", res.Families,
		"datasets", res.DataSets,
		"elapsed", res.Duration.Round(time.Second))

	return res, nil
}

func (s *Scanner) scanChanger(ctx context.Context, opts Options, res *Result) error {
	s.logger.Info("mtf: opening changer", "device", opts.ChangerDevice)
	pbsLabels, err := ListTapeLabels()
	if err != nil {
		s.logger.Error(err, "")
	}

	f, err := tapeio.NewFeeder(opts.ChangerDevice, opts.TapeDevice, opts.DriveIndex,
		tapeio.WithSkip(func(barcode string) bool {
			return pbsLabels != nil && pbsLabels[barcode]
		}),
	)
	if err != nil {
		s.logger.Error(err, "mtf: open changer", "device", opts.ChangerDevice)
		return fmt.Errorf("open changer %s: %w", opts.ChangerDevice, err)
	}
	defer f.Close()

	return f.ForEachTape(func(rc *tapeio.TapeReader, barcode string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return s.processTape(ctx, rc, barcode, res)
	})
}

func (s *Scanner) processTape(ctx context.Context, rc *tapeio.TapeReader, barcode string, res *Result) error {
	s.logger.Info("mtf: scanning cartridge", "barcode", barcode)
	if s.logger != nil {
		s.logger.LogString(fmt.Sprintf("%s: reading catalog...", barcode))
	}
	sm, smErr := mtflib.ReadSetMap(rc)
	if smErr != nil {
		s.logger.Error(smErr, "")
	}
	if sm != nil && len(sm.Entries) > 0 {
		if s.logger != nil {
			s.logger.LogString(fmt.Sprintf("%s: found %d data sets via catalog", barcode, len(sm.Entries)))
		}
		if iErr := s.indexSetMap(ctx, rc, barcode, sm, res); iErr != nil {
			s.logger.Error(iErr, "mtf: index failed", "barcode", barcode)
			if s.logger != nil {
				s.logger.LogString(fmt.Sprintf("%s: index failed  -  %v", barcode, iErr))
			}
			return fmt.Errorf("%s: %w", barcode, iErr)
		}
		return nil
	}
	if s.logger != nil {
		s.logger.LogString(fmt.Sprintf("%s: no catalog, doing full census...", barcode))
	}
	if rErr := rc.Rewind(); rErr != nil {
		s.logger.Error(rErr, "")
	}
	r := mtflib.NewReader(rc)
	if cErr := s.indexReader(ctx, r, barcode, false, "", res, func() error { return nil }); cErr != nil {
		s.logger.Error(cErr, "mtf: census failed", "barcode", barcode)
		if s.logger != nil {
			s.logger.LogString(fmt.Sprintf("%s: census failed  -  %v", barcode, cErr))
		}
		return fmt.Errorf("%s: %w", barcode, cErr)
	}
	return nil
}

func (s *Scanner) indexSetMap(ctx context.Context, rc *tapeio.TapeReader, barcode string, sm *mtflib.SetMap, res *Result) error {
	if s.logger != nil {
		s.logger.LogString("  Reading TAPE header...")
	}
	if err := rc.Rewind(); err != nil {
		s.logger.Error(err, "")
	}
	r := mtflib.NewReader(rc)
	blk, err := r.Next()
	if err != nil {
		return fmt.Errorf("read TAPE block: %w", err)
	}
	if blk.Kind != mtflib.KindMedia || blk.Tape == nil {
		return fmt.Errorf("first block is not a TAPE block")
	}

	var pbaOffset int64
	if sm != nil && len(sm.Entries) > 0 {
		pbaOffset = s.probeFirstSSET(rc, r, sm)
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

	if s.logger != nil {
		s.logger.LogString(fmt.Sprintf("  Media family: %s (ID %d, %d tapes)", famName, famID, fam.TotalTapes))
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
		PbaOffset:     pbaOffset,
	}); err != nil {
		return fmt.Errorf("upsert cartridge: %w", err)
	}
	res.Cartridges++

	if sm != nil {
		res.Families++
		if s.logger != nil {
			s.logger.LogString(fmt.Sprintf("  Indexing %d data set entries...", len(sm.Entries)))
		}
		if err := s.indexSetMapEntries(ctx, famID, sm, int(blk.Tape.Sequence)); err != nil {
			return fmt.Errorf("index set map: %w", err)
		}
	}
	return nil
}

func (s *Scanner) scanDrive(ctx context.Context, dev, barcode string, res *Result) error {
	isPBS, err := tapeio.IsPBSTape(dev)
	if err != nil {
		s.logger.Error(err, "mtf: check tape type", "device", dev)
		return fmt.Errorf("check tape type: %w", err)
	}
	if isPBS {
		s.logger.Info("mtf: skipping PBS-formatted tape", "device", dev)
		if s.logger != nil {
			s.logger.LogString("skipping PBS-formatted tape")
		}
		return nil
	}
	s.logger.Info("mtf: reading tape", "device", dev)
	if s.logger != nil {
		s.logger.LogString(fmt.Sprintf("Reading tape from %s...", dev))
	}
	rc, err := tapeio.OpenTapeReader(dev)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			s.logger.Error(err, "")
		}
	}()
	if barcode == "" {
		barcode = "TAPE0001"
	}
	return s.processTape(ctx, rc, barcode, res)
}

func (s *Scanner) scanBKFFile(ctx context.Context, path, label string, res *Result) error {
	s.logger.Info("mtf: scanning BKF", "path", path)
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
		return ErrNoBKFFiles
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
				s.logger.Error(err, "")
			}
			return err
		}
		if err := r.Close(); err != nil {
			s.logger.Error(err, "")
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
		s.logger.Error(err, "mtf: census failed", "barcode", barcode)
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
		ssetPba := int64(e.SSETPBA)
		onStartTape := int(e.MediaSeq) == tapeSeq
		if !onStartTape {
			ssetPba = 0
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
			SsetPba:        ssetPba,
			FirstMediaSeq:  nullInt(int64(e.MediaSeq)),
			SourceMediaSeq: nullInt(int64(tapeSeq)),
		})
		if err != nil {
			return fmt.Errorf("upsert data set %d: %w", e.SetNumber, err)
		}
		if e.SSETPBA > 0 {
			if err := s.db.Queries().CreateDataSetTape(ctx, mtfquery.CreateDataSetTapeParams{
				DataSetID: dsID,
				MediaSeq:  int64(e.MediaSeq),
				SsetPba:   int64(e.SSETPBA),
			}); err != nil {
				s.logger.Error(err, "failed to create data set tape", "set_number", e.SetNumber, "media_seq", e.MediaSeq)
			}
		}
		if tapeSeq >= int(e.MediaSeq) {
			if _, err := s.db.Queries().DeleteVolumesByDataSet(ctx, dsID); err != nil {
				s.logger.Error(err, "")
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
		PbaOffset:       0,
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

func (s *Scanner) probeFirstSSET(rc *tapeio.TapeReader, r *mtflib.Reader, sm *mtflib.SetMap) int64 {
	if len(sm.Entries) == 0 || sm.Entries[0].SSETPBA == 0 {
		return 0
	}
	rawPBA := int64(sm.Entries[0].SSETPBA)
	candidates := []int64{rawPBA - 1, rawPBA - 2, rawPBA, rawPBA - 3, rawPBA + 1}
	for _, cand := range candidates {
		if cand < 0 {
			continue
		}
		if err := rc.Rewind(); err != nil {
			s.logger.Error(err, "probe: rewind")
			continue
		}
		if _, err := r.Next(); err != nil {
			s.logger.Error(err, "probe: read TAPE")
			continue
		}
		if err := r.SeekToBlock(cand); err != nil {
			continue
		}
		blk, err := r.Next()
		if err != nil {
			continue
		}
		if blk.Kind == mtflib.KindSet {
			offset := rawPBA - cand
			if s.logger != nil {
				s.logger.LogString(fmt.Sprintf("  PBA offset calibration: stored=%d actual=%d offset=%d", rawPBA, cand, offset))
			}
			return offset
		}
	}
	if s.logger != nil {
		s.logger.LogString("  PBA offset calibration failed, using offset=0")
	}
	return 0
}
