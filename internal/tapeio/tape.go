package tapeio

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	mtf "github.com/pbs-plus/go-mtf"
	"github.com/pbs-plus/go-tapedrive"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var pbsBlockMagic = []byte{220, 189, 175, 202, 235, 160, 165, 40}

type TapeReader struct{ d *tapedrive.Drive }

func NewTapeReader(d *tapedrive.Drive) *TapeReader { return &TapeReader{d: d} }

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

func (t *TapeReader) SeekBlock(block int64) error { return t.d.SeekBlock(block) }

func (t *TapeReader) TellBlock() (int64, error) { return t.d.TellBlock() }

func (t *TapeReader) EOM() error { return t.d.EOM() }

func (t *TapeReader) Rewind() error { return t.d.Rewind() }

func (t *TapeReader) Close() error { return t.d.Close() }

func OpenTapeReader(dev string) (*TapeReader, error) {
	d, err := tapedrive.Open(dev)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dev, err)
	}
	if err := d.SetLogicalAddressing(); err != nil {
		log.Error(err, "")
	}
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
	return NewTapeReader(d), nil
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
