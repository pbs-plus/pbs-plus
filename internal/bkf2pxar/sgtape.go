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
// ("", nil) when no sg node is associated.
func sgNodeForNST(dev string) (string, error) {
	name := strings.TrimPrefix(dev, "/dev/")
	link := filepath.Join("/sys/class/scsi_tape", name, "device", "scsi_generic")
	entries, err := os.ReadDir(link)
	if err != nil {
		return "", nil // not fatal: fall back to the st char device
	}
	for _, e := range entries {
		return "/dev/" + e.Name(), nil
	}
	return "", nil
}

// openSGTapeReader opens the SCSI-generic node for dev (an /dev/nstN path),
// rewinds, puts the drive in variable-block mode, and returns a Reader. It
// returns an error if no sg node exists or the open/setup fails; callers fall
// back to the st char device on error.
func openSGTapeReader(dev string) (io.ReadCloser, error) {
	sg, err := sgNodeForNST(dev)
	if err != nil || sg == "" {
		return nil, fmt.Errorf("no sg node for %s", dev)
	}
	d, err := gotape.Open(sg)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", sg, err)
	}
	// Drives need a moment to settle after open; poll for readiness.
	if err := d.WaitUntilReady(func() *time.Duration { d := 120 * time.Second; return &d }()); err != nil {
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
