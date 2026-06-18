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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/go-tape"
)

// sgReader wraps a gotape.Reader and the Device it owns, implementing
// io.ReadCloser. The Reader drives the tape with explicit SCSI commands and
// satisfies go-mtf's BlockSkipper, so header-only walks skip file data via
// block LOCATE instead of reading it.
type sgReader struct {
	dev *gotape.Device
	r   *gotape.Reader
}

func (s *sgReader) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *sgReader) Close() error               { return s.dev.Close() }

// SkipForward delegates to the inner gotape.Reader so the wrapper satisfies
// go-mtf's BlockSkipper and header-only walks skip file data via block LOCATE.
func (s *sgReader) SkipForward(n int64) error { return s.r.SkipForward(n) }

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
// SCSI-generic node via sysfs, rewinds it to BOT, puts the drive in
// variable-block mode, and returns a Reader wrapped as an io.ReadCloser.
// A reader always begins at the start of the cartridge, so callers never need
// to rewind separately. The caller Close()s the returned reader when done.
func openTapeReader(dev string) (io.ReadCloser, error) {
	sg, err := sgNodeForNST(dev)
	if err != nil {
		return nil, fmt.Errorf("locate sg node for %s: %w", dev, err)
	}
	d, err := gotape.Open(sg)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", sg, err)
	}
	// Drives need a moment to settle after open; poll for readiness.
	timeout := 120 * time.Second
	if err := d.WaitUntilReady(&timeout); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("ready %s: %w", sg, err)
	}
	if err := d.Rewind(); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("rewind %s: %w", sg, err)
	}
	if err := d.SetVariableBlock(); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("set-variable-block %s: %w", sg, err)
	}
	return &sgReader{dev: d, r: gotape.NewReader(d)}, nil
}

// OpenTapeReader is the exported form of openTapeReader for callers outside
// this package (e.g. the inventory engine).
func OpenTapeReader(dev string) (io.ReadCloser, error) {
	return openTapeReader(dev)
}
