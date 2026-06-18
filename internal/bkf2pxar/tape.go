// Raw LTO tape I/O via go-tapedrive (Linux st driver).
//
// The tape is driven through go-tapedrive, which exposes the kernel SCSI tape
// (st) character driver (/dev/nst*) behind the standard io interfaces —
// io.Reader, io.Seeker, io.Closer and their compositions. There is no
// dependency on a SCSI-generic (/dev/sgN) node, raw SG_IO passthrough, or the
// external `mt` binary: the device node is opened directly. The
// *tapedrive.Tape returned here satisfies io.ReadSeekCloser, and go-mtf
// detects its io.Seeker to skip file data via Seek instead of reading it — the
// fast-retrieval path MTF is designed for (spec §3.4.3). Logical-block
// addressing (MT_ST_SCSI2LOGICAL) is enabled at open so Seek/Position are
// meaningful on HPE Ultrium (LTO) drives.
package bkf2pxar

import (
	"fmt"
	"io"

	"github.com/pbs-plus/go-tapedrive"
)

// openTapeReader opens a non-rewinding tape device (e.g. /dev/nst0), rewinds
// it to BOT, and verifies the drive positioned to logical object 0. It returns
// the drive as an io.ReadCloser. The underlying *tapedrive.Tape also
// implements io.Seeker, which go-mtf detects to skip file data via Seek
// instead of reading it. A reader always begins at the start of the cartridge,
// so callers never need to rewind separately. The caller Close()s the returned
// reader when done.
func openTapeReader(dev string) (io.ReadCloser, error) {
	// Open the st device directly. WithSCSI2Logical enables logical-block
	// addressing (MT_ST_SCSI2LOGICAL) so Seek/Position are meaningful on HPE
	// Ultrium (LTO) drives; variable-block mode is the driver default.
	t, err := tapedrive.Open(dev, tapedrive.WithSCSI2Logical(true))
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	// Rewind to BOT.
	if err := t.Rewind(); err != nil {
		_ = t.Close()
		return nil, fmt.Errorf("rewind %s: %w", dev, err)
	}
	// Verify the drive actually positioned to logical object 0.
	pos, err := t.Position()
	if err != nil {
		_ = t.Close()
		return nil, fmt.Errorf("read-position after rewind %s: %w", dev, err)
	}
	if pos != 0 {
		_ = t.Close()
		return nil, fmt.Errorf("rewind %s: drive reports logical object %d, want 0 (BOT)", dev, pos)
	}
	return t, nil
}

// OpenTapeReader is the exported form of openTapeReader for callers outside
// this package (e.g. the inventory engine).
func OpenTapeReader(dev string) (io.ReadCloser, error) {
	return openTapeReader(dev)
}
