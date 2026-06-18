// Raw LTO tape I/O for go-mtf via go-tapedrive (Linux st driver).
//
// go-mtf (>= v0.13.0) is tape-first and ships its own adapter from go-tapedrive
// to the mtf.Tape interface: mtf.NewDriveTape. It translates go-tapedrive's
// filemark (io.EOF) / end-of-recorded-data (ErrEndOfData) sentinels into mtf's
// (mtf.ErrFilemark / io.EOF), passes ReadBlock through to the st driver, and
// anchors MTF §3.4.3 fast-retrieval seeks at the SSET's live PBA via TellBlock
// (MTIOCPOS) + SeekBlock (MTSEEK).
//
// This file keeps the package-local tapeReader name as an alias for
// mtf.DriveTape so feeder/converter code is unchanged, and provides
// openTapeReader: open the non-rewinding device read-only, rewind to BOT, and
// verify logical object 0. A reader always begins at the start of the
// cartridge, so callers never need to rewind separately.
//
// Read-only because the st driver rejects an O_RDWR open of a write-protected
// cartridge with EROFS — the normal input for inventory/restore.
package bkf2pxar

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/go-tapedrive"
)

// tapeReader wraps mtf.DriveTape to count SeekBlock calls (diagnostic: proves
// the fast §3.4.3 seek path is used rather than read-discard) and to honor
// BKF2PXAR_NO_SEEK (force read-discard for comparison). It satisfies mtf.Tape
// by embedding *mtf.DriveTape; ReadBlock/TellBlock/NativePBA/Close pass through.
type tapeReader struct {
	*mtf.DriveTape
	seeks *int64
}

// globalSeekCount is the default seek counter for tapeReaders whose caller
// didn't supply one.
var globalSeekCount int64

// SeekBlock implements mtf.Tape, counting hardware seeks and honoring
// BKF2PXAR_NO_SEEK (diagnostic override).
func (r *tapeReader) SeekBlock(block int64) error {
	if os.Getenv("BKF2PXAR_NO_SEEK") != "" {
		return errSeekDisabled
	}
	if r.seeks != nil {
		atomic.AddInt64(r.seeks, 1)
	}
	if os.Getenv("BKF2PXAR_SEEK_DEBUG") != "" {
		fmt.Fprintf(os.Stderr, "[seek] -> block %d (count %d)\n", block, SeekCount())
	}
	return r.DriveTape.SeekBlock(block)
}

// openTapeReader opens a non-rewinding tape device read-only, rewinds it to
// BOT, and verifies the drive reports block 0. It returns the drive as an
// mtf.Tape (with Close), ready to hand to mtf.NewReader.
func openTapeReader(dev string) (*tapeReader, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	// Enable SCSI-2 logical block addressing so SeekBlock/TellBlock use logical
	// addresses (the same space tape writers record PBAs in). Required for
	// MTF catalog PBAs to be meaningful; st(4) calls it "highly advisable".
	// Best-effort: ignore errors on drives that don't support it.
	_ = d.SetLogicalAddressing()
	if err := d.Rewind(); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("rewind %s: %w", dev, err)
	}
	pos, err := d.TellBlock()
	if err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("read-position after rewind %s: %w", dev, err)
	}
	if pos != 0 {
		_ = d.Close()
		return nil, fmt.Errorf("rewind %s: drive reports block %d, want 0 (BOT)", dev, pos)
	}
	return &tapeReader{DriveTape: mtf.NewDriveTape(d), seeks: &globalSeekCount}, nil
}

// OpenTapeReader is the exported form of openTapeReader for callers outside
// this package (e.g. the inventory engine).
func OpenTapeReader(dev string) (*tapeReader, error) {
	return openTapeReader(dev)
}

// SeekCount returns the cumulative number of SeekBlock calls across all open
// tapeReaders (diagnostic).
func SeekCount() int64 { return atomic.LoadInt64(&globalSeekCount) }

// ResetSeekCount zeroes the global seek counter (diagnostic).
func ResetSeekCount() { atomic.StoreInt64(&globalSeekCount, 0) }

// errSeekDisabled is returned by SeekBlock when BKF2PXAR_NO_SEEK is set, so the
// seek path can be force-disabled for diagnosis (go-mtf then falls back to
// read-discard).
var errSeekDisabled = errors.New("bkf2pxar: seek disabled (BKF2PXAR_NO_SEEK)")
