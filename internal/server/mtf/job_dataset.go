package mtf

import (
	"context"
	"fmt"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tape"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/tapeio"
)

func (j *mtfJob) configForDataSet(ctx context.Context, ds mtfdb.DataSet, cfg tapeio.Config, tapeCfg *tape.Config) (tapeio.Config, error) {
	carts, err := j.store.MtfStore.ListCartridgesByFamily(ctx, ds.MediaFamilyID)
	if err != nil {
		return cfg, err
	}

	dsTapes, err := j.store.MtfStore.Queries().ListDataSetTapes(ctx, ds.ID)
	if err != nil {
		j.logger.Error(err, "failed to list data set tapes", "data_set_id", ds.ID)
	}

	type tapeInfo struct {
		ssetPba   int64
		barcode   string
		pbaOffset int64
	}
	seqToCart := make(map[int]mtfdb.Cartridge, len(carts))
	for _, c := range carts {
		seqToCart[c.Sequence] = c
	}

	tapePBAs := make(map[int]int64, len(dsTapes))
	for _, dt := range dsTapes {
		tapePBAs[int(dt.MediaSeq)] = dt.SsetPba
	}

	var bestTape *tapeInfo
	var bestSeq int

	startCart, haveStartCart := seqToCart[ds.FirstMediaSeq]

	if pba, ok := tapePBAs[ds.FirstMediaSeq]; ok && pba > 0 && haveStartCart {
		bestTape = &tapeInfo{pba, startCart.Barcode, startCart.PbaOffset}
		bestSeq = ds.FirstMediaSeq
	}
	if bestTape == nil {
		for seq, pba := range tapePBAs {
			if pba > 0 {
				if c, ok := seqToCart[seq]; ok {
					bestTape = &tapeInfo{pba, c.Barcode, c.PbaOffset}
					bestSeq = seq
					break
				}
			}
		}
	}
	if bestTape == nil && ds.SSETPBA > 0 && haveStartCart {
		bestTape = &tapeInfo{ds.SSETPBA, startCart.Barcode, startCart.PbaOffset}
		bestSeq = ds.FirstMediaSeq
	}

	allBKF := true
	var sources []string
	for _, c := range carts {
		if !c.IsBkfFile {
			allBKF = false
			break
		}
		sources = append(sources, c.SourcePath)
	}
	if allBKF && len(sources) > 0 {
		cfg.Sources = sources
	} else {
		dev, chg, idx, err := j.resolveDrivePaths(tapeCfg)
		if err != nil {
			return cfg, err
		}
		cfg.TapeDevice = dev
		if cfg.ChangerDevice == "" {
			cfg.ChangerDevice = chg
		}
		cfg.DriveIndex = idx

		if cfg.ChangerDevice != "" && len(carts) > 0 {
			seqToBarcode := make(map[int]string, len(carts))
			for _, c := range carts {
				seqToBarcode[c.Sequence] = c.Barcode
			}
			feeder, err := tapeio.NewFeeder(cfg.ChangerDevice, cfg.TapeDevice, cfg.DriveIndex, tapeio.WithLog(func(msg string) {
				j.task.LogString(msg)
			}), tapeio.WithContext(ctx), tapeio.WithKeepLoaded(j.job.KeepLoaded), tapeio.WithSequenceResolver(func(seq int) string {
				return seqToBarcode[seq]
			}))
			if err != nil {
				return cfg, fmt.Errorf("open changer: %w", err)
			}
			cfg.Feeder = feeder
			j.feeder = feeder

			var wantBarcode string
			if bestTape != nil {
				wantBarcode = bestTape.barcode
			} else {
				wantBarcode = seqToBarcode[ds.FirstMediaSeq]
			}
			if wantBarcode == "" {
				return cfg, fmt.Errorf("cartridge for media sequence %d is not in the library; please insert tape %d of this media set and re-inventory", ds.FirstMediaSeq, ds.FirstMediaSeq)
			}
			if err := feeder.LoadBarcodeWait(wantBarcode); err != nil {
				feeder.Close()
				return cfg, fmt.Errorf("load cartridge %s: %w", wantBarcode, err)
			}
		}
	}

	wantSet := ds.SetNumber
	wantMachine := ds.MachineName
	wantTime := ds.WriteTime

	if bestTape != nil && bestTape.ssetPba > 0 {
		offset := bestTape.pbaOffset
		if offset == 0 {
			offset = 1
		}
		cfg.SnapshotPBA = bestTape.ssetPba - offset
	} else if ds.SSETPBA > 0 {
		if !haveStartCart {
			return cfg, fmt.Errorf("cartridge for media sequence %d is not in the library; please insert tape %d of this media set and re-inventory", ds.FirstMediaSeq, ds.FirstMediaSeq)
		}
		offset := startCart.PbaOffset
		if offset == 0 {
			offset = 1
		}
		cfg.SnapshotPBA = ds.SSETPBA - offset
	} else {
		dsID := ds.ID
		storeRef := j.store.MtfStore
		cfg.OnSetMapRead = func(entry mtf.SetMapEntry) {
			if entry.SSETPBA == 0 {
				return
			}
			if err := storeRef.SetDataSetSsetPba(ctx, dsID, int64(entry.SSETPBA)); err != nil {
				j.logger.Error(err, "failed to persist sset_pba", "data_set_id", dsID)
			} else {
				j.logger.Info("persisted sset_pba from setmap", "data_set_id", dsID, "pba", entry.SSETPBA)
			}
		}
	}

	currentSeq := bestSeq
	cfg.SnapshotResolver = func(entries []mtf.SetMapEntry) int {
		for i, e := range entries {
			if int(e.SetNumber) == wantSet && int(e.MediaSeq) == currentSeq {
				return i
			}
		}
		for i, e := range entries {
			if int(e.SetNumber) == wantSet {
				return i
			}
		}
		for i, e := range entries {
			if wantTime != 0 && e.WriteTime.Unix() == wantTime {
				return i
			}
		}
		for i, e := range entries {
			for _, v := range e.Volumes {
				if v.MachineName == wantMachine {
					return i
				}
			}
		}
		return -1
	}
	return cfg, nil
}
