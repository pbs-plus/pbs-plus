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
//
// Tape positioning (rewind) is performed in pure Go via the Linux st driver's
// MTIOCTOP ioctl — the same call the `mt` binary uses — so no external `mt`
// binary is required.
package bkf2pxar

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// OpenTapeReader opens a tape device, rewinds it to BOT via MTIOCTOP, and wraps
// it in a *tapeReader. Exported for the tapeprobe diagnostic tool so it can
// reuse the exact production tape path without duplicating the block-reading
// adapter. Not part of the stable converter API.

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

// openTapeReader opens a tape device (e.g. /dev/nst0), rewinds it to BOT, and
// wraps it for block-oriented reading. A reader always begins at the start of
// the cartridge, so callers never need to rewind separately. The caller Close()s
// the returned reader when done.
//
// When the drive's SCSI-generic node (/dev/sgN) can be located it is opened via
// gotape, which drives the tape with explicit SCSI commands and supports
// block-LOCATE skips (go-mtf BlockSkipper) for fast header-only walks; otherwise
// the kernel st character device is used as a fallback.
func openTapeReader(dev string) (io.ReadCloser, error) {
	rc, sgErr := openSGTapeReader(dev)
	if sgErr == nil {
		return rc, nil
	}
	// Fall back to the st char device if no sg node / sg open failed.
	// After a robotic load the drive may report EBUSY for a few seconds while it
	// recognises the cartridge; retry the open+rewind pair briefly.
	var f *os.File
	var err error
	for attempt := 0; ; attempt++ {
		f, err = os.Open(dev)
		if err == nil {
			rerr := rewindFd(f.Fd())
			if rerr == nil {
				break
			}
			_ = f.Close()
			err = rerr
		}
		if !errors.Is(err, syscall.EBUSY) || attempt >= 60 {
			return nil, fmt.Errorf("open/rewind %s: %w", dev, err)
		}
		time.Sleep(500 * time.Millisecond)
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

// --- Tape positioning via the Linux st driver (MTIOCTOP) ---
//
// golang.org/x/sys/unix does not expose MTIOCTOP or the mt_op codes, so they
// are defined here to match <linux/mtio.h>. This is the same ioctl `mt` uses.

const (
	ioctlMTIOCTOP = 0x40086d01 // _IOW('m', 1, struct mtop)
	mtOpRewind    = 0x06       // MTREW: rewind to BOT
	mtOpOffline   = 0x07       // MTOFFL: rewind and put the drive offline (eject)
)

// mtop matches the kernel's struct mtop: short mt_op followed by int mt_count,
// 8 bytes total with natural alignment.
type mtop struct {
	Op    int16
	Count int32
}

// rewindFd issues MTIOCTOP(MTREW) on an open tape device descriptor. It is the
// pure-Go equivalent of `mt -f <dev> rewind`.
// OpenTapeReader opens a tape device (e.g. /dev/nst0), rewinds it to BOT,
// and wraps it for block-oriented reading as a continuous byte stream. It is
// the reusable, exportable form of openTapeReader for callers outside this
// package (e.g. the inventory engine).
func OpenTapeReader(dev string) (io.ReadCloser, error) {
	return openTapeReader(dev)
}

func rewindFd(fd uintptr) error {
	op := mtop{Op: mtOpRewind, Count: 1}
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL, fd, ioctlMTIOCTOP, uintptr(unsafe.Pointer(&op)))
	if errno != 0 {
		return fmt.Errorf("MTREW: %w", errno)
	}
	return nil
}
