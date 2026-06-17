package bkf2pxar

import (
	"io"
	"testing"
)

// scriptedTape replays a fixed sequence of read results, modeling a fixed-block
// SCSI tape: a step with n>0 yields n bytes from a backing buffer; a step with
// n==0 yields (0, io.EOF) — exactly what a filemark looks like under Linux st.
type scriptedTape struct {
	steps []tapeStep
	data  []byte
	off   int
}

type tapeStep struct{ n int }

func (s *scriptedTape) Read(p []byte) (int, error) {
	if len(s.steps) == 0 {
		return 0, io.EOF
	}
	st := s.steps[0]
	s.steps = s.steps[1:]
	if st.n == 0 {
		return 0, io.EOF // filemark
	}
	n := copy(p, s.data[s.off:s.off+st.n])
	s.off += n
	return n, nil
}

// TestTapeReaderFilemarkIsNotEOD is the regression test for the bug that made
// the real HP MSL G3 LTO-8 tape appear to contain only a TAPE block: a read
// returning (0, io.EOF) is a *filemark* (end of one record), not end-of-data.
// The adapter must skip it and continue to the next block.
func TestTapeReaderFilemarkIsNotEOD(t *testing.T) {
	// block1 "TAPE00", filemark, block2 "SSET0", filemark, filemark(EOD).
	src := &scriptedTape{
		data: []byte("TAPE00SSET0"),
		steps: []tapeStep{
			{6}, {0}, // block1, filemark
			{5}, {0}, {0}, // block2, filemark, filemark(EOD)
		},
	}
	tr := newTapeReader(src)
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want := "TAPE00SSET0"; string(got) != want {
		t.Errorf("got %q, want %q (filemark wrongly treated as EOD)", got, want)
	}
}

// TestTapeReaderEODAfterTwoFilemarks confirms two consecutive filemarks with no
// data between them terminate the stream.
func TestTapeReaderEODAfterTwoFilemarks(t *testing.T) {
	src := &scriptedTape{
		data: []byte("AB"),
		steps: []tapeStep{
			{2}, {0}, {0}, // block, filemark, filemark(EOD)
		},
	}
	tr := newTapeReader(src)
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "AB" {
		t.Errorf("got %q, want \"AB\"", got)
	}
}
