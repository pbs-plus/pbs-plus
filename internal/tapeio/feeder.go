package tapeio

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	mtf "github.com/pbs-plus/go-mtf"

	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

type Feeder struct {
	chg        *changer.Changer
	tapeDev    string
	driveIndex int

	status *changer.Status

	loadedBarcode string
	loadedSlot    int
	loadedSrcSlot int

	probed    map[string]int
	processed map[string]bool
	skip      func(barcode string) bool
	logf      func(string)
}

type Option func(*Feeder)

func WithSkip(fn func(barcode string) bool) Option {
	return func(f *Feeder) { f.skip = fn }
}

func WithLog(fn func(string)) Option {
	return func(f *Feeder) { f.logf = fn }
}

func NewFeeder(changerDev, tapeDev string, driveIndex int, opts ...Option) (*Feeder, error) {
	chg, err := changer.Open(changerDev)
	if err != nil {
		if errors.Is(err, syscall.EBUSY) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("changer %s is in use by another operation", changerDev)
		}
		return nil, fmt.Errorf("open changer %s: %w", changerDev, err)
	}
	st, err := chg.Status()
	if err != nil {
		if err := chg.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("changer inventory: %w", err)
	}
	f := &Feeder{
		chg:        chg,
		tapeDev:    tapeDev,
		driveIndex: driveIndex,
		status:     st,
		probed:     make(map[string]int),
		processed:  make(map[string]bool),
	}
	for _, o := range opts {
		o(f)
	}
	return f, nil
}

func (f *Feeder) log(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	if f.logf != nil {
		f.logf(msg)
	}
}

func (f *Feeder) Close() {
	if err := f.chg.Close(); err != nil {
		log.Error(err, "")
	}
}

func (f *Feeder) RefreshStatus() error {
	st, err := f.chg.Status()
	if err != nil {
		return err
	}
	f.status = st
	return nil
}

func (f *Feeder) loadSlot(slot int) (*TapeReader, error) {
	bc := f.status.Slots[slot-1].VolumeTag
	f.log(fmt.Sprintf("Loading slot %d (%s) into drive %d", slot, barcodeOrUnknown(bc), f.driveIndex))
	if err := f.chg.Load(f.status, slot, f.driveIndex); err != nil {
		return nil, fmt.Errorf("load slot %d: %w", slot, err)
	}
	rc, err := OpenTapeReader(f.tapeDev)
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

func (f *Feeder) UnloadCurrent() error {
	if f.loadedSlot == 0 {
		return nil
	}
	slot := f.loadedSlot
	bc := f.loadedBarcode
	f.log(fmt.Sprintf("Unloading drive %d -> slot %d (%s)", f.driveIndex, slot, barcodeOrUnknown(bc)))
	if err := f.chg.Unload(f.status, f.driveIndex, slot); err != nil {
		return fmt.Errorf("unload to slot %d: %w", slot, err)
	}
	f.loadedSlot = 0
	f.loadedBarcode = ""
	return nil
}

func (f *Feeder) ForEachTape(visit func(rc *TapeReader, barcode string) error) error {
	if f.status != nil {
		for dIdx, drive := range f.status.Drives {
			if !drive.Full {
				continue
			}
			bc := drive.VolumeTag
			if bc == "" || IsCleaningTape(bc) || f.processed[bc] {
				continue
			}
			if f.skip != nil && f.skip(bc) {
				f.processed[bc] = true
				continue
			}
			f.log(fmt.Sprintf("Drive %d has %s loaded; reading in place", dIdx, barcodeOrUnknown(bc)))
			rc, err := OpenTapeReader(f.tapeDev)
			if err != nil {
				return fmt.Errorf("open tape in drive %d: %w", dIdx, err)
			}
			f.loadedBarcode = bc
			f.loadedSlot = 0
			f.loadedSrcSlot = slotIndexForAddr(f.status, drive.LoadedSlotAddr)
			vErr := visit(rc, bc)
			if cErr := rc.Close(); cErr != nil {
				log.Error(cErr, "")
			}
			f.processed[bc] = true
			if uErr := f.unloadInDrive(); uErr != nil {
				if vErr == nil {
					vErr = uErr
				}
			}
			f.loadedBarcode = ""
			f.loadedSrcSlot = 0
			if vErr != nil {
				return vErr
			}
		}
	}

	for i, s := range f.status.Slots {
		if !s.Full || s.ImportExport {
			continue
		}
		bc := s.VolumeTag
		if IsCleaningTape(bc) || f.processed[bc] {
			continue
		}
		if f.skip != nil && f.skip(bc) {
			f.processed[bc] = true
			continue
		}
		slot := i + 1
		rc, lErr := f.loadSlot(slot)
		if lErr != nil {
			return fmt.Errorf("load slot %d: %w", slot, lErr)
		}
		vErr := visit(rc, bc)
		if cErr := rc.Close(); cErr != nil {
			log.Error(cErr, "")
		}
		if uErr := f.UnloadCurrent(); uErr != nil {
			log.Error(uErr, "")
		}
		f.processed[bc] = true
		if vErr != nil {
			return vErr
		}
	}
	return nil
}

func slotIndexForAddr(st *changer.Status, addr uint16) int {
	if addr == 0 {
		return 0
	}
	for i, s := range st.Slots {
		if s.ElementAddress == addr {
			return i + 1
		}
	}
	return 0
}

func (f *Feeder) unloadInDrive() error {
	slot := f.loadedSrcSlot
	if slot == 0 {
		for i, s := range f.status.Slots {
			if !s.Full && !s.ImportExport {
				slot = i + 1
				break
			}
		}
	}
	if slot == 0 {
		return nil
	}
	f.log(fmt.Sprintf("Unloading drive %d -> slot %d (%s)", f.driveIndex, slot, barcodeOrUnknown(f.loadedBarcode)))
	if err := f.chg.Unload(f.status, f.driveIndex, slot); err != nil {
		return fmt.Errorf("unload drive -> slot %d: %w", slot, err)
	}
	return nil
}

func (f *Feeder) AsContinuation() func(mtf.Continuation) (mtf.Tape, error) {
	return func(c mtf.Continuation) (mtf.Tape, error) {
		return f.nextMedium(c)
	}
}

func (f *Feeder) nextMedium(c mtf.Continuation) (mtf.Tape, error) {
	wantSeq := 0
	wantMFMID := uint32(0)
	if c.Media != nil {
		wantSeq = int(c.Media.Sequence) + 1
		wantMFMID = c.Media.MFMID
	}
	f.log(fmt.Sprintf("End of tape: need next tape (family 0x%08X, sequence %d)", wantMFMID, wantSeq))

	if err := f.UnloadCurrent(); err != nil {
		log.Error(err, "")
	}

	for pass := range 2 {
		for i, s := range f.status.Slots {
			if !s.Full || s.ImportExport {
				continue
			}
			if IsCleaningTape(s.VolumeTag) {
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
				f.log(fmt.Sprintf("  slot %d: %v", slot, err))
				continue
			}
			ok, err := f.verifyTape(rc, wantMFMID, wantSeq)
			if err != nil {
				f.log(fmt.Sprintf("  slot %d verify: %v", slot, err))
				if err := rc.Close(); err != nil {
					log.Error(err, "")
				}
				if err := f.UnloadCurrent(); err != nil {
					log.Error(err, "")
				}
				continue
			}
			if ok {
				f.log(fmt.Sprintf("  slot %d: matched sequence %d", slot, wantSeq))
				rc, err := OpenTapeReader(f.tapeDev)
				if err != nil {
					return nil, err
				}
				return rc, nil
			}
			f.log(fmt.Sprintf("  slot %d: not the wanted tape", slot))
			if err := rc.Close(); err != nil {
				log.Error(err, "")
			}
			if err := f.UnloadCurrent(); err != nil {
				log.Error(err, "")
			}
		}
		if pass == 0 {
			if err := f.RefreshStatus(); err != nil {
				log.Error(err, "")
			}
		}
	}
	return nil, fmt.Errorf("tape with family 0x%08X sequence %d not found in changer", wantMFMID, wantSeq)
}

func (f *Feeder) verifyTape(rc *TapeReader, wantMFMID uint32, wantSeq int) (bool, error) {
	r := mtf.NewReader(rc)
	blk, err := r.Next()
	if err := rc.Close(); err != nil {
		log.Error(err, "")
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

func (f *Feeder) LoadBarcode(barcode string) error {
	if f.status == nil {
		return fmt.Errorf("feeder has no changer status")
	}

	if f.driveIndex < len(f.status.Drives) && f.status.Drives[f.driveIndex].Full {
		driveBarcode := f.status.Drives[f.driveIndex].VolumeTag
		if driveBarcode == barcode {
			f.log(fmt.Sprintf("Cartridge %s already in drive %d", barcode, f.driveIndex))
			return nil
		}
		unloadSlot := 0
		for i, s := range f.status.Slots {
			if !s.Full && !s.ImportExport {
				unloadSlot = i + 1
				break
			}
		}
		if unloadSlot == 0 {
			return fmt.Errorf("no empty slot to unload %s from drive %d", driveBarcode, f.driveIndex)
		}
		f.log(fmt.Sprintf("Unloading %s from drive %d -> slot %d", driveBarcode, f.driveIndex, unloadSlot))
		if err := f.chg.Unload(f.status, f.driveIndex, unloadSlot); err != nil {
			return fmt.Errorf("unload drive %d: %w", f.driveIndex, err)
		}
		f.status.Drives[f.driveIndex].Full = false
	}

	for i, s := range f.status.Slots {
		if s.Full && s.VolumeTag == barcode {
			slotIdx := i + 1
			f.log(fmt.Sprintf("Loading cartridge %s from slot %d into drive %d", barcode, slotIdx, f.driveIndex))
			if err := f.chg.Load(f.status, slotIdx, f.driveIndex); err != nil {
				return fmt.Errorf("load slot %d into drive %d: %w", slotIdx, f.driveIndex, err)
			}
			f.loadedBarcode = barcode
			f.loadedSlot = slotIdx
			return nil
		}
	}

	return fmt.Errorf("cartridge %s not found in changer slots", barcode)
}

func barcodeOrUnknown(bc string) string {
	if bc == "" {
		return "no barcode"
	}
	return bc
}
