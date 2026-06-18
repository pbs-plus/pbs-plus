// Raw LTO tape I/O via go-tape (SCSI-generic).
//
// The tape is driven entirely through go-tape's SG_IO wrapper: explicit SCSI
// commands (READ(6), LOCATE(16), REWIND, MODE SELECT for variable-block mode)
// sent to the drive's /dev/sgN node, with no dependency on the kernel st
// character driver or the external `mt` binary. go-tape's Reader bridges the
// fixed-block tape transport to the byte stream go-mtf expects, and its
// SkipForward (block LOCATE) satisfies go-mtf's BlockSkipper for fast
// header-only walks.
package bkf2pxar

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/go-tape"
)

// sgReader wraps a gotape.Reader and the Device it owns. It implements
// io.ReadSeekCloser: Read streams the recorded data, Seek forwards to a byte
// offset (go-mtf's skipStreamData / skipRemainingData Seek instead of reading,
// the fast-retrieval path MTF is designed for, spec §3.4.3), and Close releases
// the device.
type sgReader struct {
	dev *gotape.Device
	r   *gotape.Reader
}

func (s *sgReader) Read(p []byte) (int, error)                { return s.r.Read(p) }
func (s *sgReader) Seek(off int64, whence int) (int64, error) { return s.r.Seek(off, whence) }
func (s *sgReader) Close() error                              { return s.dev.Close() }

// sgNodeForNST returns the SCSI-generic device node (e.g. /dev/sg3) backing the
// given st character device (e.g. /dev/nst0), discovered via sysfs. It returns
// ("", error) when no sg node is associated.
func sgNodeForNST(dev string) (string, error) {
	name := strings.TrimPrefix(dev, "/dev/")
	link := filepath.Join("/sys/class/scsi_tape", name, "device", "scsi_generic")
	entries, err := os.ReadDir(link)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		return "/dev/" + e.Name(), nil
	}
	return "", fmt.Errorf("no scsi_generic entry under %s", link)
}

// openTapeReader opens a tape device (e.g. /dev/nst0), resolves its
// SCSI-generic node via sysfs, rewinds it to BOT (verifying BOT via READ
// POSITION), and returns a Reader wrapped as an io.ReadCloser. The underlying
// *sgReader also implements io.Seeker, which go-mtf detects to skip file data
// via Seek instead of reading it. A reader always begins at the start of the
// cartridge, so callers never need to rewind separately. The caller Close()s
// the returned reader when done.
func openTapeReader(dev string) (io.ReadCloser, error) {
	sg, err := sgNodeForNST(dev)
	if err != nil {
		return nil, fmt.Errorf("locate sg node for %s: %w", dev, err)
	}
	// Open handles drive readiness and variable-block mode selection, returning
	// a Device immediately ready for reads.
	ctx := context.Background()
	d, err := gotape.Open(ctx, sg)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", sg, err)
	}
	// Rewind to BOT and verify the drive actually positioned to logical object 0.
	if err := d.Rewind(ctx); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("rewind %s: %w", sg, err)
	}
	if rp, err := d.ReadPositionLong(ctx); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("read-position after rewind %s: %w", sg, err)
	} else if rp.LogicalObjectNumber != 0 {
		_ = d.Close()
		return nil, fmt.Errorf("rewind %s: drive reports logical object %d, want 0 (BOT)", sg, rp.LogicalObjectNumber)
	}
	if err := d.SetBufferedMode(ctx, true); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("set-buffered-mode %s: %w", sg, err)
	}
	return &sgReader{dev: d, r: gotape.NewReader(d)}, nil
}

// OpenTapeReader is the exported form of openTapeReader for callers outside
// this package (e.g. the inventory engine).
func OpenTapeReader(dev string) (io.ReadCloser, error) {
	return openTapeReader(dev)
}
