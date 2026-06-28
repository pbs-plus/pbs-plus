package tapeio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/go-tapedrive"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var pbsBlockMagic = []byte{220, 189, 175, 202, 235, 160, 165, 40}

type TapeReader struct {
	d    *tapedrive.Drive
	logf func(string)
}

func NewTapeReader(d *tapedrive.Drive) *TapeReader { return &TapeReader{d: d} }

func (t *TapeReader) WithLog(fn func(string)) { t.logf = fn }

func (t *TapeReader) ReadBlock(dst []byte) (int, error) {
	b, err := t.d.ReadBlock()
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return 0, mtf.ErrFilemark
		case errors.Is(err, tapedrive.ErrEndOfData):
			return 0, io.EOF
		default:
			return 0, err
		}
	}
	return copy(dst, b), nil
}

func (t *TapeReader) SeekBlock(block int64) error {
	t.logSCSI(fmt.Sprintf("LOCATE -> block %d", block))
	start := time.Now()
	err := t.d.SeekBlock(block)
	elapsed := time.Since(start)
	if err != nil {
		t.logSCSI(fmt.Sprintf("LOCATE -> block %d: FAILED after %v: %v", block, elapsed.Round(time.Millisecond), err))
		return err
	}
	pos, tellErr := t.d.TellBlock()
	if tellErr != nil {
		t.logSCSI(fmt.Sprintf("LOCATE -> block %d: OK after %v (tell failed: %v)", block, elapsed.Round(time.Millisecond), tellErr))
	} else {
		t.logSCSI(fmt.Sprintf("LOCATE -> block %d: OK after %v (now at %d)", block, elapsed.Round(time.Millisecond), pos))
	}
	return nil
}

func (t *TapeReader) TellBlock() (int64, error) { return t.d.TellBlock() }

func (t *TapeReader) EOM() error {
	t.logSCSI("EOM (space to end of data)")
	start := time.Now()
	err := t.d.EOM()
	elapsed := time.Since(start)
	if err != nil {
		t.logSCSI(fmt.Sprintf("EOM: FAILED after %v: %v", elapsed.Round(time.Millisecond), err))
		return err
	}
	pos, tellErr := t.d.TellBlock()
	if tellErr != nil {
		t.logSCSI(fmt.Sprintf("EOM: OK after %v (tell failed: %v)", elapsed.Round(time.Millisecond), tellErr))
	} else {
		t.logSCSI(fmt.Sprintf("EOM: OK after %v (now at %d)", elapsed.Round(time.Millisecond), pos))
	}
	return nil
}

func (t *TapeReader) Rewind() error {
	t.logSCSI("REWIND -> BOT")
	start := time.Now()
	err := t.d.Rewind()
	elapsed := time.Since(start)
	if err != nil {
		t.logSCSI(fmt.Sprintf("REWIND: FAILED after %v: %v", elapsed.Round(time.Millisecond), err))
		return err
	}
	t.logSCSI(fmt.Sprintf("REWIND: OK after %v", elapsed.Round(time.Millisecond)))
	return nil
}

func (t *TapeReader) Close() error { return t.d.Close() }

func (t *TapeReader) logSCSI(msg string) {
	if t.logf != nil {
		t.logf("[SCSI] " + msg)
	}
}

func OpenTapeReader(dev string) (*TapeReader, error) {
	return OpenTapeReaderWithLog(dev, nil)
}

func OpenTapeReaderWithLog(dev string, logf func(string)) (*TapeReader, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		if errors.Is(err, syscall.EBUSY) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("tape device %s is in use by another operation", dev)
		}
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	fmt.Fprintf(os.Stderr, "[SCSI] opened %s\n", dev)
	if err := d.SetLogicalAddressing(); err != nil {
		log.Error(err, "")
	}
	fmt.Fprintf(os.Stderr, "[SCSI] %s rewind -> BOT\n", dev)
	if err := d.Rewind(); err != nil {
		if err := d.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("rewind %s: %w", dev, err)
	}
	pos, err := d.TellBlock()
	if err != nil {
		if err := d.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("read-position after rewind %s: %w", dev, err)
	}
	if pos != 0 {
		if err := d.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("rewind %s: drive reports block %d, want 0 (BOT)", dev, pos)
	}
	fmt.Fprintf(os.Stderr, "[SCSI] %s rewind OK, at block 0\n", dev)
	tr := NewTapeReader(d)
	tr.logf = logf
	return tr, nil
}

func IsPBSTape(dev string) (bool, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", dev, err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	if err := d.SetLogicalAddressing(); err != nil {
		log.Error(err, "")
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

func IsCleaningTape(barcode string) bool {
	if len(barcode) >= 3 {
		switch barcode[:3] {
		case "CLN", "CCL", "CLG", "DCL":
			return true
		}
	}
	return false
}
