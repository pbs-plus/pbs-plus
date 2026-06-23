package bkf2pxar

import (
	"fmt"
	"os"

	mtf "github.com/pbs-plus/go-mtf"

	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type feeder struct {
	chg        *changer.Changer
	tapeDev    string
	driveIndex int

	status *changer.Status

	loadedBarcode string
	loadedSlot    int

	probed map[string]int

	processed map[string]bool
}

func newFeeder(changerDev, tapeDev string, driveIndex int) (*feeder, error) {
	chg, err := changer.Open(changerDev)
	if err != nil {
		return nil, fmt.Errorf("open changer %s: %w", changerDev, err)
	}
	st, err := chg.Status()
	if err != nil {
		if err := chg.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		return nil, fmt.Errorf("changer inventory: %w", err)
	}
	return &feeder{
		chg:        chg,
		tapeDev:    tapeDev,
		driveIndex: driveIndex,
		status:     st,
		probed:     make(map[string]int),
		processed:  make(map[string]bool),
	}, nil
}

func (f *feeder) close() {
	if err := f.chg.Close(); err != nil {
		syslog.L.Error(err).Write()
	}
}

func (f *feeder) markProcessed() {
	if f.loadedBarcode != "" {
		f.processed[f.loadedBarcode] = true
	}
}

func (f *feeder) loadFirst() (*tapeReader, error) {
	for i, s := range f.status.Slots {
		if !s.Full || s.ImportExport {
			continue
		}
		if isCleaningTape(s.VolumeTag) {
			continue
		}
		if f.processed[s.VolumeTag] {
			continue
		}
		return f.loadSlot(i + 1)
	}
	return nil, fmt.Errorf("no data tapes found in changer (only %d slot(s), %d full)",
		len(f.status.Slots), countFull(f.status))
}

func (f *feeder) loadSlot(slot int) (*tapeReader, error) {
	bc := f.status.Slots[slot-1].VolumeTag
	fmt.Fprintf(os.Stderr, "== loading slot %d (%s) into drive %d ==\n", slot, barcodeOrUnknown(bc), f.driveIndex)
	if err := f.chg.Load(f.status, slot, f.driveIndex); err != nil {
		return nil, fmt.Errorf("load slot %d: %w", slot, err)
	}
	rc, err := openTapeReader(f.tapeDev)
	if err != nil {
		return nil, err
	}
	f.loadedBarcode = bc
	f.loadedSlot = slot
	if bc != "" {
		f.probed[bc] = slot
	}
	return rc, nil
}

func (f *feeder) unloadCurrent() error {
	if f.loadedSlot == 0 {
		return nil
	}
	slot := f.loadedSlot
	bc := f.loadedBarcode
	fmt.Fprintf(os.Stderr, "== unloading drive %d -> slot %d (%s) ==\n", f.driveIndex, slot, barcodeOrUnknown(bc))
	if err := f.chg.Unload(f.status, f.driveIndex, slot); err != nil {
		return fmt.Errorf("unload to slot %d: %w", slot, err)
	}
	f.loadedSlot = 0
	f.loadedBarcode = ""
	return nil
}

func (f *feeder) asContinuation() func(mtf.Continuation) (mtf.Tape, error) {
	return func(c mtf.Continuation) (mtf.Tape, error) {
		return f.nextMedium(c)
	}
}

// c.Sequence+1, verifies its identity from the TAPE block, and returns
func (f *feeder) nextMedium(c mtf.Continuation) (mtf.Tape, error) {
	wantSeq := 0
	wantMFMID := uint32(0)
	if c.Media != nil {
		wantSeq = int(c.Media.Sequence) + 1
		wantMFMID = c.Media.MFMID
	}
	fmt.Fprintf(os.Stderr, "\n== EOTM: need next tape (family 0x%08X, sequence %d) ==\n", wantMFMID, wantSeq)

	if err := f.unloadCurrent(); err != nil {
		syslog.L.Error(err).Write()
	}

	for pass := range 2 {
		for i, s := range f.status.Slots {
			if !s.Full || s.ImportExport {
				continue
			}
			if isCleaningTape(s.VolumeTag) {
				continue
			}
			if f.processed[s.VolumeTag] {
				continue
			}
			slot := i + 1
			if slot == f.loadedSlot {
				continue
			}
			rc, err := f.loadSlot(slot)
			if err != nil {
				fmt.Fprintf(os.Stderr, "   slot %d: %v\n", slot, err)
				continue
			}
			ok, err := f.verifyTape(rc, wantMFMID, wantSeq)
			if err != nil {
				fmt.Fprintf(os.Stderr, "   slot %d verify: %v\n", slot, err)
				if err := rc.Close(); err != nil {
					syslog.L.Error(err).Write()
				}
				if err := f.unloadCurrent(); err != nil {
					syslog.L.Error(err).Write()
				}
				continue
			}
			if ok {
				fmt.Fprintf(os.Stderr, "   slot %d: matched sequence %d\n", slot, wantSeq)
				rc, err := openTapeReader(f.tapeDev)
				if err != nil {
					return nil, err
				}
				return rc, nil
			}
			fmt.Fprintf(os.Stderr, "   slot %d: not the wanted tape\n", slot)
			if err := rc.Close(); err != nil {
				syslog.L.Error(err).Write()
			}
			if err := f.unloadCurrent(); err != nil {
				syslog.L.Error(err).Write()
			}
		}
		if pass == 0 {
			st, err := f.chg.Status()
			if err == nil {
				f.status = st
			}
		}
	}
	return nil, fmt.Errorf("tape with family 0x%08X sequence %d not found in changer", wantMFMID, wantSeq)
}

// verifyTape peeks at the TAPE block and reports whether its MFMID/Sequence
// match. It closes the probe reader; the caller reopens on a match.
func (f *feeder) verifyTape(rc *tapeReader, wantMFMID uint32, wantSeq int) (bool, error) {
	r := mtf.NewReader(rc)
	blk, err := r.Next()
	if err := rc.Close(); err != nil {
		syslog.L.Error(err).Write()
	}
	if err != nil {
		return false, err
	}
	if blk.Kind != mtf.KindMedia || blk.Tape == nil {
		return false, fmt.Errorf("first block is not a TAPE block")
	}
	gotMFMID := blk.Tape.MFMID
	gotSeq := int(blk.Tape.Sequence)
	if wantMFMID != 0 && gotMFMID != wantMFMID {
		return false, nil
	}
	return gotSeq == wantSeq, nil
}

func isCleaningTape(barcode string) bool {
	b := barcode
	if len(b) >= 3 {
		switch b[:3] {
		case "CLN", "CCL", "CLG", "DCL":
			return true
		}
	}
	return false
}

func IsCleaningTape(barcode string) bool {
	return isCleaningTape(barcode)
}

func countFull(st *changer.Status) int {
	n := 0
	for _, s := range st.Slots {
		if s.Full {
			n++
		}
	}
	return n
}

func barcodeOrUnknown(bc string) string {
	if bc == "" {
		return "no barcode"
	}
	return bc
}
