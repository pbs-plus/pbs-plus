package pxar

import (
	"io"
	"testing"
)

type bytesReaderAt struct{}

func (bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	for i := range p {
		p[i] = 0xAA
	}
	return len(p), nil
}

func TestNopCloserHidesSeeker(t *testing.T) {
	section := io.NewSectionReader(bytesReaderAt{}, 0, 8<<20)
	rc := io.NopCloser(section)

	_, ok := rc.(io.Seeker)
	if ok {
		t.Log("io.NopCloser preserved io.Seeker (Go stdlib changed)")
	} else {
		t.Log("io.NopCloser strips io.Seeker — handleReadContentAt must not require it")
	}
}

func TestSequentialReadNoSeeker(t *testing.T) {
	const fileSize = 8 << 20
	section := io.NewSectionReader(bytesReaderAt{}, 0, fileSize)
	rc := io.NopCloser(section)

	h := &contentHandle{rc: rc, fileSize: fileSize}

	buf := make([]byte, 4<<20)

	n, err := io.ReadFull(h.rc, buf)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	h.bytesRead += int64(n)

	if n != 4<<20 {
		t.Fatalf("first read: got %d bytes, want %d", n, 4<<20)
	}

	n, err = io.ReadFull(h.rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		t.Fatalf("second read: %v", err)
	}
	h.bytesRead += int64(n)

	if h.bytesRead != fileSize {
		t.Fatalf("total read: %d, want %d", h.bytesRead, fileSize)
	}
}
