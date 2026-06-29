package tapeio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	mtf "github.com/pbs-plus/go-mtf"

	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var ErrStateDesync = errors.New("tape state desync: drive contents do not match expected cartridge")

type Feeder struct {
	chg        *changer.Changer
	tapeDev    string
	driveIndex int
	ctx        context.Context
	keepLoaded bool

	loadedBarcode string
	loadedSlot    int

	probed      map[string]int
	processed   map[string]bool
	skip        func(barcode string) bool
	logf        func(string)
	seqResolver func(seq int) string
}

type Option func(*Feeder)

func WithSkip(fn func(barcode string) bool) Option {
	return func(f *Feeder) { f.skip = fn }
}

func WithLog(fn func(string)) Option {
	return func(f *Feeder) { f.logf = fn }
}

func WithContext(ctx context.Context) Option {
	return func(f *Feeder) { f.ctx = ctx }
}

func WithKeepLoaded(keep bool) Option {
	return func(f *Feeder) { f.keepLoaded = keep }
}

func WithSequenceResolver(fn func(seq int) string) Option {
	return func(f *Feeder) { f.seqResolver = fn }
}

func NewFeeder(changerDev, tapeDev string, driveIndex int, opts ...Option) (*Feeder, error) {
	chg, err := changer.Open(changerDev)
	if err != nil {
		if errors.Is(err, syscall.EBUSY) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("changer %s is in use by another operation", changerDev)
		}
		return nil, fmt.Errorf("open changer %s: %w", changerDev, err)
	}
	if _, err := chg.Status(); err != nil {
		if err := chg.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("changer inventory: %w", err)
	}
	f := &Feeder{
		chg:        chg,
		tapeDev:    tapeDev,
		driveIndex: driveIndex,
		ctx:        context.Background(),
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

func (f *Feeder) cancelled() bool {
	select {
	case <-f.ctx.Done():
		return true
	default:
		return false
	}
}

func (f *Feeder) currentStatus() (*changer.Status, error) {
	return f.chg.Status()
}

func findSlotByBarcode(st *changer.Status, barcode string) int {
	for i, s := range st.Slots {
		if s.Full && !s.ImportExport && s.VolumeTag == barcode {
			return i + 1
		}
	}
	return 0
}

func findFreeStorageSlot(st *changer.Status) int {
	for i, s := range st.Slots {
		if !s.Full && !s.ImportExport {
			return i + 1
		}
	}
	return 0
}

func (f *Feeder) loadSlot(slot int, expectedBarcode string) (*TapeReader, error) {
	st, err := f.currentStatus()
	if err != nil {
		return nil, fmt.Errorf("read status before load: %w", err)
	}
	bc := st.Slots[slot-1].VolumeTag
	f.log(fmt.Sprintf("Loading slot %d (%s) into drive %d", slot, barcodeOrUnknown(bc), f.driveIndex))

	after, err := f.chg.LoadSlot(slot, f.driveIndex)
	if err != nil {
		return nil, fmt.Errorf("load slot %d: %w", slot, err)
	}

	if err := f.verifyLoaded(after, expectedBarcode); err != nil {
		return nil, err
	}

	rc, err := OpenTapeReaderWithLog(f.tapeDev, f.logf)
	if err != nil {
		return nil, err
	}
	f.loadedBarcode = expectedBarcode
	f.loadedSlot = slot
	if bc != "" {
		f.probed[bc] = slot
	}
	return rc, nil
}

func (f *Feeder) verifyLoaded(st *changer.Status, expectedBarcode string) error {
	if f.driveIndex < 0 || f.driveIndex >= len(st.Drives) {
		return fmt.Errorf("%w: drive index %d out of range (%d drives reported)", ErrStateDesync, f.driveIndex, len(st.Drives))
	}
	drive := st.Drives[f.driveIndex]
	if !drive.Full {
		return fmt.Errorf("%w: drive %d is empty after load of slot %s", ErrStateDesync, f.driveIndex, barcodeOrUnknown(expectedBarcode))
	}
	if expectedBarcode != "" && drive.VolumeTag != "" && drive.VolumeTag != expectedBarcode {
		return fmt.Errorf("%w: drive %d reports %s, expected %s", ErrStateDesync, f.driveIndex, drive.VolumeTag, expectedBarcode)
	}
	return nil
}

func (f *Feeder) UnloadCurrent() error {
	if f.loadedSlot == 0 {
		return nil
	}
	slot := f.loadedSlot
	bc := f.loadedBarcode
	f.log(fmt.Sprintf("Unloading drive %d -> slot %d (%s)", f.driveIndex, slot, barcodeOrUnknown(bc)))
	if _, err := f.chg.Unload(slot, f.driveIndex); err != nil {
		return fmt.Errorf("unload to slot %d: %w", slot, err)
	}
	f.loadedSlot = 0
	f.loadedBarcode = ""
	return nil
}

func (f *Feeder) unloadDriveToFreeSlot(st *changer.Status) error {
	slot := findFreeStorageSlot(st)
	if slot == 0 {
		return fmt.Errorf("no empty slot to unload drive %d", f.driveIndex)
	}
	bc := ""
	if f.driveIndex < len(st.Drives) {
		bc = st.Drives[f.driveIndex].VolumeTag
	}
	f.log(fmt.Sprintf("Unloading drive %d -> slot %d (%s)", f.driveIndex, slot, barcodeOrUnknown(bc)))
	if _, err := f.chg.Unload(slot, f.driveIndex); err != nil {
		return fmt.Errorf("unload drive -> slot %d: %w", slot, err)
	}
	return nil
}

func (f *Feeder) ensureDriveEmpty(st *changer.Status) (*changer.Status, error) {
	if f.driveIndex < 0 || f.driveIndex >= len(st.Drives) {
		return st, nil
	}
	if !st.Drives[f.driveIndex].Full {
		return st, nil
	}
	if st.Drives[f.driveIndex].VolumeTag == f.loadedBarcode && f.loadedBarcode != "" {
		return st, nil
	}
	if err := f.unloadDriveToFreeSlot(st); err != nil {
		return nil, err
	}
	return f.currentStatus()
}

func (f *Feeder) ForEachTape(visit func(rc *TapeReader, barcode string) error) error {
	st, err := f.currentStatus()
	if err != nil {
		return fmt.Errorf("refresh changer: %w", err)
	}

	for dIdx, drive := range st.Drives {
		if f.cancelled() {
			return f.ctx.Err()
		}
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
		if dIdx != f.driveIndex {
			f.log(fmt.Sprintf("Drive %d has %s loaded; skipping (configured drive is %d)", dIdx, barcodeOrUnknown(bc), f.driveIndex))
			continue
		}
		f.log(fmt.Sprintf("Drive %d has %s loaded; reading in place", dIdx, barcodeOrUnknown(bc)))
		rc, err := OpenTapeReaderWithLog(f.tapeDev, f.logf)
		if err != nil {
			return fmt.Errorf("open tape in drive %d: %w", dIdx, err)
		}
		f.loadedBarcode = bc
		f.loadedSlot = 0
		vErr := visit(rc, bc)
		if cErr := rc.Close(); cErr != nil {
			log.Error(cErr, "")
		}
		f.processed[bc] = true
		if !f.keepLoaded {
			cur, _ := f.currentStatus()
			if cur != nil {
				if uErr := f.unloadDriveToFreeSlot(cur); uErr != nil {
					log.Error(uErr, "")
				}
			}
		}
		f.loadedBarcode = ""
		f.loadedSlot = 0
		if vErr != nil {
			return vErr
		}
	}

	for i, s := range st.Slots {
		if f.cancelled() {
			return f.ctx.Err()
		}
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
		rc, lErr := f.loadSlot(slot, bc)
		if lErr != nil {
			return fmt.Errorf("load slot %d: %w", slot, lErr)
		}
		vErr := visit(rc, bc)
		if cErr := rc.Close(); cErr != nil {
			log.Error(cErr, "")
		}
		if !f.keepLoaded {
			if uErr := f.UnloadCurrent(); uErr != nil {
				log.Error(uErr, "")
			}
		}
		f.processed[bc] = true
		if vErr != nil {
			return vErr
		}
	}
	return nil
}

func (f *Feeder) AsContinuation() func(mtf.Continuation) (mtf.Tape, error) {
	return func(c mtf.Continuation) (mtf.Tape, error) {
		return f.nextMedium(c)
	}
}

func (f *Feeder) nextMedium(c mtf.Continuation) (mtf.Tape, error) {
	if f.cancelled() {
		return nil, f.ctx.Err()
	}
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

	if f.seqResolver != nil {
		barcode := f.seqResolver(wantSeq)
		if barcode == "" {
			return nil, fmt.Errorf("tape with family 0x%08X sequence %d not found in inventory", wantMFMID, wantSeq)
		}
		st, err := f.currentStatus()
		if err != nil {
			return nil, fmt.Errorf("refresh changer: %w", err)
		}
		if findSlotByBarcode(st, barcode) == 0 && !tapeInDrives(st, barcode) {
			f.log(fmt.Sprintf("tape %s (family 0x%08X sequence %d) not in changer, waiting for operator", barcode, wantMFMID, wantSeq))
		}
		if err := f.LoadBarcodeWait(barcode); err != nil {
			return nil, fmt.Errorf("load %s: %w", barcode, err)
		}
		rc, err := OpenTapeReaderWithLog(f.tapeDev, f.logf)
		if err != nil {
			return nil, err
		}
		if wantMFMID != 0 {
			if ok, vErr := f.verifyTape(rc, wantMFMID, wantSeq); vErr != nil || !ok {
				if cerr := rc.Close(); cerr != nil {
					log.Error(cerr, "")
				}
				return nil, fmt.Errorf("tape %s is not family 0x%08X sequence %d", barcode, wantMFMID, wantSeq)
			}
		}
		f.log(fmt.Sprintf("Loaded sequence %d (%s)", wantSeq, barcode))
		return rc, nil
	}

	st, err := f.currentStatus()
	if err != nil {
		return nil, fmt.Errorf("refresh changer: %w", err)
	}
	for i, s := range st.Slots {
		if f.cancelled() {
			return nil, f.ctx.Err()
		}
		if !s.Full || s.ImportExport || IsCleaningTape(s.VolumeTag) || f.processed[s.VolumeTag] {
			continue
		}
		slot := i + 1
		if slot == f.loadedSlot {
			continue
		}
		rc, err := f.loadSlot(slot, s.VolumeTag)
		if err != nil {
			f.log(fmt.Sprintf("  slot %d: %v", slot, err))
			continue
		}
		ok, err := f.verifyTape(rc, wantMFMID, wantSeq)
		if err != nil || !ok {
			if err != nil {
				f.log(fmt.Sprintf("  slot %d verify: %v", slot, err))
			} else {
				f.log(fmt.Sprintf("  slot %d: not sequence %d", slot, wantSeq))
			}
			if cerr := rc.Close(); cerr != nil {
				log.Error(cerr, "")
			}
			if uerr := f.UnloadCurrent(); uerr != nil {
				log.Error(uerr, "")
			}
			continue
		}
		f.log(fmt.Sprintf("  slot %d: matched sequence %d", slot, wantSeq))
		rc2, err := OpenTapeReaderWithLog(f.tapeDev, f.logf)
		if err != nil {
			return nil, err
		}
		return rc2, nil
	}
	return nil, fmt.Errorf("tape with family 0x%08X sequence %d not found in changer", wantMFMID, wantSeq)
}

func tapeInDrives(st *changer.Status, barcode string) bool {
	for _, d := range st.Drives {
		if d.Full && d.VolumeTag == barcode {
			return true
		}
	}
	return false
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

func (f *Feeder) LoadBarcodeWait(barcode string) error {
	var lastReason string
	first := true
	for {
		if f.cancelled() {
			return f.ctx.Err()
		}
		if !first {
			for range 50 {
				if f.cancelled() {
					return f.ctx.Err()
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		first = false

		if err := f.LoadBarcode(barcode); err == nil {
			return nil
		} else {
			reason := err.Error()
			if reason != lastReason {
				f.log(fmt.Sprintf("Please insert media '%s': %s", barcode, reason))
				lastReason = reason
			}
		}
	}
}

func (f *Feeder) LoadBarcode(barcode string) error {
	st, err := f.currentStatus()
	if err != nil {
		return fmt.Errorf("refresh changer status: %w", err)
	}

	if f.driveIndex < 0 || f.driveIndex >= len(st.Drives) {
		return fmt.Errorf("%w: drive index %d out of range (%d drives reported)", ErrStateDesync, f.driveIndex, len(st.Drives))
	}
	drive := st.Drives[f.driveIndex]
	if drive.Full {
		if drive.VolumeTag == barcode {
			f.log(fmt.Sprintf("Cartridge %s already in drive %d", barcode, f.driveIndex))
			f.loadedBarcode = barcode
			f.loadedSlot = 0
			return nil
		}
		st, err = f.ensureDriveEmpty(st)
		if err != nil {
			return err
		}
	}

	slot := findSlotByBarcode(st, barcode)
	if slot == 0 {
		return fmt.Errorf("cartridge %s not found in changer slots", barcode)
	}
	f.log(fmt.Sprintf("Loading cartridge %s from slot %d into drive %d", barcode, slot, f.driveIndex))
	after, err := f.chg.LoadSlot(slot, f.driveIndex)
	if err != nil {
		return fmt.Errorf("load slot %d into drive %d: %w", slot, f.driveIndex, err)
	}
	if err := f.verifyLoaded(after, barcode); err != nil {
		return err
	}
	f.loadedBarcode = barcode
	f.loadedSlot = slot
	return nil
}

func barcodeOrUnknown(bc string) string {
	if bc == "" {
		return "no barcode"
	}
	return bc
}
