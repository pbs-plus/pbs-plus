// Raw LTO tape I/O for go-mtf via go-tapedrive (Linux st driver).
// read-only, enables logical block addressing, rewinds to BOT, verifies block
// Read-only because the st driver rejects an O_RDWR open of a write-protected
package bkf2pxar

import (
	"bytes"
	"fmt"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/go-tapedrive"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// PBS block header magic (from proxmox-backup/pbs-tape/src/lib.rs).
// Any tape block written by PBS starts with these 8 bytes.
var pbsBlockMagic = []byte{220, 189, 175, 202, 235, 160, 165, 40}

type tapeReader = mtf.DriveTape

// IsPBSTape opens dev read-only, reads the first block, and returns true if
// it starts with the Proxmox Backup Server block header magic.
func IsPBSTape(dev string) (bool, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", dev, err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	if err := d.SetLogicalAddressing(); err != nil {
		syslog.L.Error(err).Write()
	}
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
	if err := d.SetLogicalAddressing(); err != nil {
		syslog.L.Error(err).Write()
	}
	if err := d.Rewind(); err != nil {
		if err := d.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		return nil, fmt.Errorf("rewind %s: %w", dev, err)
	}
	pos, err := d.TellBlock()
	if err != nil {
		if err := d.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		return nil, fmt.Errorf("read-position after rewind %s: %w", dev, err)
	}
	if pos != 0 {
		if err := d.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		return nil, fmt.Errorf("rewind %s: drive reports block %d, want 0 (BOT)", dev, pos)
	}
	return mtf.NewDriveTape(d), nil
}

func OpenTapeReader(dev string) (*tapeReader, error) {
	return openTapeReader(dev)
}
