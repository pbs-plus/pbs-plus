// Raw LTO tape I/O adapter.
//
// SCSI tape drives (/dev/nst0) deliver data one fixed-size block per read(),
// with filemarks (0-byte reads) delimiting records. go-mtf's Reader, by
// contrast, expects a file-like byte stream and issues many small chunked
// reads (the common descriptor block is read 48 bytes at a time). On tape,
// each such read() consumes a whole 64 KiB block and discards the overflow,
// instantly desynchronizing the parser.
//
// tapeReader bridges the two: it reads whole tape blocks into an internal
// buffer and serves go-mtf's small reads from it, concatenating data blocks
// across filemarks into the continuous byte stream go-mtf expects. The
// filemark gaps that delimit MTF logical records on tape are removed; MTF
// blocks are self-delimiting (each carries a size in its common header), so
// their boundaries need no out-of-band signaling.
package bkf2pxar

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// maxTapeBlock is the largest block we expect on an LTO cartridge. LTO drives
// write 64 KiB records by default; 1 MiB leaves headroom for 1 MiB fixed-block
// configurations and future LTO generations.
const maxTapeBlock = 1 << 20

// tapeReader adapts a fixed-block tape device into a continuous byte stream.
type tapeReader struct {
	r      io.Reader
	closer io.Closer
	buf    []byte // unread bytes of the current tape block
	blk    []byte // scratch for the next whole-block read
	eof    bool
	marks  int // consecutive filemarks seen with no data delivered
	closed bool
}

// newTapeReader wraps a block-oriented reader (typically a tape device file).
// The source's position must be at BOT (rewound). If the source is an io.Closer
// (e.g. *os.File), Close releases it.
func newTapeReader(r io.Reader) *tapeReader {
	tr := &tapeReader{r: r, blk: make([]byte, maxTapeBlock)}
	if c, ok := r.(io.Closer); ok {
		tr.closer = c
	}
	return tr
}

// openTapeReader opens a tape device (e.g. /dev/nst0) and wraps it for
// block-oriented reading. The caller must rewind the tape to BOT before calling
// and Close the returned reader when done.
func openTapeReader(dev string) (*tapeReader, error) {
	f, err := os.Open(dev)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	return newTapeReader(f), nil
}

// fill loads the next tape block into the buffer, skipping filemarks. A tape
// read that returns (0, io.EOF) is a filemark — the end of one file/record —
// not a terminal end-of-data; the next record follows after it. The true
// end-of-data is signalled by two consecutive filemarks with no data between
// them (a long-standing SCSI-tape convention) or by a hardware fault.
func (t *tapeReader) fill() error {
	for len(t.buf) == 0 {
		if t.eof {
			return io.EOF
		}
		n, err := t.r.Read(t.blk)
		if n > 0 {
			t.buf = t.blk[:n]
			t.marks = 0
			continue
		}
		// n == 0: a filemark returns (0, io.EOF) on tape. Count consecutive
		// filemarks; two in a row with no intervening data mark end-of-data.
		t.marks++
		if t.marks >= 2 {
			t.eof = true
			return io.EOF
		}
		if err != nil && err != io.EOF {
			// A non-EOF error at n==0 is a hardware/blank-check fault, not a
			// retryable filemark.
			if isBlankCheck(err) {
				t.eof = true
				return io.EOF
			}
			return fmt.Errorf("tape read: %w", err)
		}
		// Filemark (err == nil or err == io.EOF): fall through and read again.
	}
	return nil
}

func (t *tapeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(t.buf) == 0 {
		if err := t.fill(); err != nil {
			return 0, err
		}
	}
	n := copy(p, t.buf)
	t.buf = t.buf[n:]
	return n, nil
}

// Close closes the underlying device file, if it is an io.Closer.
func (t *tapeReader) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true
	if t.closer != nil {
		return t.closer.Close()
	}
	return nil
}

// isBlankCheck reports whether a tape read error corresponds to a SCSI
// BLANK_CHECK / end-of-data condition rather than a true I/O fault.
func isBlankCheck(err error) bool {
	var pe *os.PathError
	if errors.As(err, &pe) {
		// Linux st reports EOD as ENOMEM (block-too-big on a blank region) or
		// EIO in some configurations. We only treat ENOMEM as EOD here; EIO is
		// a genuine media error the caller should see.
		if pe.Err == syscall.ENOMEM {
			return true
		}
	}
	return false
}
