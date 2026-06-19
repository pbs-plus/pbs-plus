// Raw LTO tape I/O for go-mtf via go-tapedrive (Linux st driver).
//
// go-mtf ships its own adapter from go-tapedrive to the mtf.Tape interface
// (mtf.NewDriveTape), so this file only opens the non-rewinding device
// read-only, enables logical block addressing, rewinds to BOT, verifies block
// 0, and returns the drive as an mtf.Tape ready for mtf.NewReader.
//
// Read-only because the st driver rejects an O_RDWR open of a write-protected
// cartridge with EROFS — the normal input for inventory/restore.
package bkf2pxar

import (
	"fmt"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/go-tapedrive"
)

// tapeReader is an mtf.DriveTape. The alias preserves the feeder/converter
// signatures that hand a *tapeReader to mtf.NewReader and SetContinuation.
type tapeReader = mtf.DriveTape

// openTapeReader opens a non-rewinding tape device read-only, enables logical
// block addressing, rewinds to BOT (go-tapedrive's Rewind verifies hardware
// BOT with retries), and checks the block counter for good measure.
func openTapeReader(dev string) (*tapeReader, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	// Required for stored MTF catalog PBAs (EOTM Last-ESET-PBA, SSET PBA) to
	// resolve; st(4) calls logical addressing "highly advisable". Best-effort.
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
	return mtf.NewDriveTape(d), nil
}

// OpenTapeReader is the exported form of openTapeReader for callers outside
// this package (e.g. the inventory engine).
func OpenTapeReader(dev string) (*tapeReader, error) {
	return openTapeReader(dev)
}
