// Tape auto-feeder: bridges the SCSI Medium Changer to go-mtf's continuation
// callback. When an EOTM is reached, the feeder finds the next tape of the
// current media family (by MFMID + sequence), commands the changer to swap it
// into the drive, rewinds, and returns the new reader.
package bkf2pxar

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	mtf "github.com/pbs-plus/go-mtf"

	"github.com/pbs-plus/pbs-plus/internal/changer"
)

// feeder drives a robotic tape changer for unattended multi-tape migration.
type feeder struct {
	chg        *changer.Changer
	tapeDev    string // tape drive device, e.g. /dev/nst0
	driveIndex int

	status *changer.Status // cached inventory

	// loaded tracks the currently-loaded tape's identity (from its TAPE block)
	// so we can verify the next tape and know which slot to return it to.
	loadedBarcode string
	loadedSlot    int // 1-based virtual slot, 0 if drive was loaded externally

	// probed maps a barcode to the slot that holds it, so repeat lookups are
	// O(1). Built lazily as tapes are examined.
	probed map[string]int

	// processed tracks barcodes already migrated, so loadFirst advances
	// through the magazine one backup set at a time.
	processed map[string]bool
}

func newFeeder(changerDev, tapeDev string, driveIndex int) (*feeder, error) {
	chg, err := changer.Open(changerDev)
	if err != nil {
		return nil, fmt.Errorf("open changer %s: %w", changerDev, err)
	}
	st, err := chg.Status()
	if err != nil {
		_ = chg.Close()
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

func (f *feeder) close() { _ = f.chg.Close() }

// markProcessed records the cartridge just migrated so subsequent loadFirst
// calls skip it.
func (f *feeder) markProcessed() {
	if f.loadedBarcode != "" {
		f.processed[f.loadedBarcode] = true
	}
}

// loadFirst loads the first available data tape (skipping cleaning tapes) into
// the drive and returns an open reader positioned at BOT. It picks the first
// non-cleaning, full slot. The caller is responsible for closing the reader.
func (f *feeder) loadFirst() (*os.File, error) {
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

// loadSlot loads slot (1-based), rewinds, opens the drive, reads the TAPE block
// to identify the cartridge, and caches its barcode→slot mapping.
func (f *feeder) loadSlot(slot int) (*os.File, error) {
	bc := f.status.Slots[slot-1].VolumeTag
	fmt.Fprintf(os.Stderr, "== loading slot %d (%s) into drive %d ==\n", slot, barcodeOrUnknown(bc), f.driveIndex)
	if err := f.chg.Load(f.status, slot, f.driveIndex); err != nil {
		return nil, fmt.Errorf("load slot %d: %w", slot, err)
	}
	if err := rewind(f.tapeDev); err != nil {
		return nil, fmt.Errorf("rewind: %w", err)
	}
	rc, err := os.Open(f.tapeDev)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", f.tapeDev, err)
	}
	f.loadedBarcode = bc
	f.loadedSlot = slot
	if bc != "" {
		f.probed[bc] = slot
	}
	return rc, nil
}

// unloadCurrent returns the loaded tape to its home slot.
func (f *feeder) unloadCurrent() error {
	if f.loadedSlot == 0 {
		return nil // nothing loaded by us (e.g. operator-inserted first tape)
	}
	slot := f.loadedSlot
	bc := f.loadedBarcode
	fmt.Fprintf(os.Stderr, "== unloading drive %d -> slot %d (%s) ==\n", f.driveIndex, slot, barcodeOrUnknown(bc))
	// Close the drive fd first so the OS releases the tape.
	if err := f.chg.Unload(f.status, f.driveIndex, slot); err != nil {
		return fmt.Errorf("unload to slot %d: %w", slot, err)
	}
	f.loadedSlot = 0
	f.loadedBarcode = ""
	return nil
}

// asContinuation adapts the feeder into go-mtf's continuation callback.
// It is invoked with the context of the medium that just ended; it must
// return a reader over the next medium of the same family, or an error.
func (f *feeder) asContinuation() func(mtf.Continuation) (io.Reader, error) {
	return func(c mtf.Continuation) (io.Reader, error) {
		return f.nextMedium(c)
	}
}

// nextMedium finds and loads the tape carrying media-family sequence
// c.Sequence+1 (matching the MFMID of the tape that just ended), verifies its
// identity from the TAPE block, and returns a fresh reader.
func (f *feeder) nextMedium(c mtf.Continuation) (io.Reader, error) {
	wantSeq := 0
	wantMFMID := uint32(0)
	if c.Media != nil {
		wantSeq = int(c.Media.Sequence) + 1
		wantMFMID = c.Media.MFMID
	}
	fmt.Fprintf(os.Stderr, "\n== EOTM: need next tape (family 0x%08X, sequence %d) ==\n", wantMFMID, wantSeq)

	// Return the current tape first.
	_ = f.unloadCurrent()

	// Strategy: scan slots for a tape whose TAPE block matches family+sequence.
	// We identify by reading the cartridge header; barcode is a hint only.
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
				_ = rc.Close()
				_ = f.unloadCurrent()
				continue
			}
			if ok {
				fmt.Fprintf(os.Stderr, "   slot %d: matched sequence %d\n", slot, wantSeq)
				return rc, nil
			}
			fmt.Fprintf(os.Stderr, "   slot %d: not the wanted tape\n", slot)
			_ = rc.Close()
			_ = f.unloadCurrent()
		}
		if pass == 0 {
			// Refresh inventory in case the operator added tapes.
			st, err := f.chg.Status()
			if err == nil {
				f.status = st
			}
		}
	}
	return nil, fmt.Errorf("tape with family 0x%08X sequence %d not found in changer", wantMFMID, wantSeq)
}

// verifyTape peeks at the cartridge's TAPE block and reports whether its
// MFMID/Sequence match the wanted values. It rewinds the tape afterward so the
// reader is positioned at BOT for go-mtf.
func (f *feeder) verifyTape(rc *os.File, wantMFMID uint32, wantSeq int) (bool, error) {
	r := mtf.NewReader(rc)
	blk, err := r.Next()
	if err != nil {
		return false, err
	}
	if blk.Kind != mtf.KindMedia || blk.Tape == nil {
		return false, fmt.Errorf("first block is not a TAPE block")
	}
	gotMFMID := blk.Tape.MFMID
	gotSeq := int(blk.Tape.Sequence)
	// Rewind to BOT for the consumer.
	if _, err := rc.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	if err := rewind(f.tapeDev); err != nil {
		return false, err
	}
	if wantMFMID != 0 && gotMFMID != wantMFMID {
		return false, nil
	}
	return gotSeq == wantSeq, nil
}

// rewind issues `mt -f <dev> rewind`. go-mtf reads from current position, so
// the tape must be at BOT before the reader is created.
func rewind(dev string) error {
	cmd := exec.Command("mt", "-f", dev, "rewind")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %w (%s)", cmd.String(), err, string(out))
	}
	return nil
}

// isCleaningTape recognises cleaning-cartridge barcodes (CLN/CCL prefixes).
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
