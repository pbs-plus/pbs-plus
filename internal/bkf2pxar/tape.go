// Raw LTO tape I/O for go-mtf via go-tapedrive (Linux st driver).
//
// go-mtf ships its own adapter from go-tapedrive to the mtf.Tape interface
// (mtf.NewDriveTape), so this file only opens the non-rewinding device
// read-only, enables logical block addressing, rewinds to BOT, verifies block
// 0, and returns the drive as an mtf.Tape ready for mtf.NewReader.
//
// Read-only because the st driver rejects an O_RDWR open of a write-protected
// cartridge with EROFS  -  the normal input for inventory/restore.
package bkf2pxar

import (
	"bytes"
	"fmt"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/go-tapedrive"
)

// PBS block header magic (from proxmox-backup/pbs-tape/src/lib.rs).
// Any tape block written by PBS starts with these 8 bytes.
var pbsBlockMagic = []byte{220, 189, 175, 202, 235, 160, 165, 40}

// tapeReader is an mtf.DriveTape alias for feeder/converter signatures.
type tapeReader = mtf.DriveTape

// IsPBSTape opens dev read-only, reads the first block, and returns true if
// it starts with the Proxmox Backup Server block header magic.
func IsPBSTape(dev string) (bool, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", dev, err)
	}
	defer func() { _ = d.Close() }()

	_ = d.SetLogicalAddressing()
	if err := d.Rewind(); err != nil {
		return false, fmt.Errorf("rewind %s: %w", dev, err)
	}

	block, err := d.ReadBlock()
	if err != nil {
		return false, nil
	}

	return bytes.HasPrefix(block, pbsBlockMagic), nil
}

// openTapeReader opens a non-rewinding tape device read-only, enables logical
// block addressing, rewinds to BOT (go-tapedrive's Rewind verifies hardware
// BOT with retries), and checks the block counter for good measure.
func openTapeReader(dev string) (*tapeReader, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	// Required for stored MTF catalog PBAs to resolve. Best-effort.
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

// OpenTapeReader is the exported form of openTapeReader.
func OpenTapeReader(dev string) (*tapeReader, error) {
	return openTapeReader(dev)
}
