package pxarmount

import (
	"io"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

type payloadSpan struct {
	start    uint64
	end      uint64
	injected bool
}

type refCall struct {
	name  string
	start uint64
	end   uint64
}

type mockInjectionWriter struct {
	noopWriter
	enc   *encoder.Encoder
	spans []payloadSpan
	refs  []refCall
}

type recordingProgress struct {
	noopProgress
	messages []string
}

func (p *recordingProgress) SetMsg(msg string) {
	p.messages = append(p.messages, msg)
}

func newMockInjectionWriter(t *testing.T, startPos uint64) *mockInjectionWriter {
	t.Helper()
	meta := &pxar.Metadata{Stat: format.Stat{Mode: format.ModeIFDIR | 0o755}}
	enc := encoder.NewEncoder(io.Discard, io.Discard, meta, nil)
	if startPos > 0 {
		_ = enc.Advance(startPos)
	}
	return &mockInjectionWriter{enc: enc}
}

func (w *mockInjectionWriter) Encoder() *encoder.Encoder { return w.enc }

func (w *mockInjectionWriter) WriteEntryRef(entry *pxar.Entry, off uint64) error {
	w.refs = append(w.refs, refCall{
		name:  entry.FileName(),
		start: off,
		end:   off + entry.FileSize + uint64(format.HeaderSize),
	})
	return nil
}

func (w *mockInjectionWriter) InjectChunks(chunks []backupproxy.KnownChunkRef) error {
	var total uint64
	for _, c := range chunks {
		total += c.Size
	}
	start := w.enc.PayloadPosition()
	w.spans = append(w.spans, payloadSpan{start: start, end: start + total, injected: true})
	return w.enc.Advance(total)
}

func (w *mockInjectionWriter) WriteEntryReader(_ *pxar.Entry, r io.Reader, size uint64) error {
	if r != nil {
		if _, err := io.Copy(io.Discard, r); err != nil {
			return err
		}
	}
	start := w.enc.PayloadPosition()
	n := size + uint64(format.HeaderSize)
	w.spans = append(w.spans, payloadSpan{start: start, end: start + n})
	return w.enc.Advance(n)
}

func spanCovered(spans []payloadSpan, start, end uint64) bool {
	pos := start
	for pos < end {
		advanced := false
		for _, s := range spans {
			if s.injected && s.start <= pos && pos < s.end {
				pos = s.end
				advanced = true
				break
			}
		}
		if !advanced {
			return false
		}
	}
	return true
}

func assertRefsBacked(t *testing.T, w *mockInjectionWriter) {
	t.Helper()
	if len(w.refs) == 0 {
		t.Fatal("no payload refs recorded")
	}
	for _, ref := range w.refs {
		if !spanCovered(w.spans, ref.start, ref.end) {
			t.Errorf("ref %q [%d,%d) not backed by injected chunks; spans=%+v",
				ref.name, ref.start, ref.end, w.spans)
		}
	}
}

func TestFlushPendingRefsReportsReuseProgress(t *testing.T) {
	const chunkSize = 4000
	progress := &recordingProgress{}
	writer := newMockInjectionWriter(t, 10000)
	state := &commitWalkState{
		mfs:            &MutableFS{},
		writer:         writer,
		prog:           progress,
		origChunkIndex: buildSyntheticDIDX(t, 3, chunkSize),
		pendingRefs: []commitEntry{{
			name:    "unchanged",
			sortKey: 100,
			pxarSlim: &dirEntrySlim{
				payloadOffset: 100,
				fileSize:      7500,
			},
			cachedEntry: &pxar.Entry{
				Path:          "unchanged",
				Kind:          pxar.KindFile,
				FileSize:      7500,
				PayloadOffset: 100,
			},
		}},
	}

	if err := state.flushPendingRefs(false); err != nil {
		t.Fatal(err)
	}
	if len(progress.messages) != 1 || progress.messages[0] != "Processing unchanged files (1 scanned)" {
		t.Fatalf("progress messages = %q, want reuse progress", progress.messages)
	}
}

func TestPayloadRefsAlwaysBackedByInjectedChunks(t *testing.T) {
	const chunkSize = 4000
	idx := buildSyntheticDIDX(t, 3, chunkSize)

	ce := func(name string, offset, size uint64) commitEntry {
		return commitEntry{
			name:    name,
			sortKey: offset,
			pxarSlim: &dirEntrySlim{
				payloadOffset: offset,
				fileSize:      size,
			},
			cachedEntry: &pxar.Entry{
				Path:          name,
				Kind:          pxar.KindFile,
				FileSize:      size,
				PayloadOffset: offset,
			},
		}
	}

	newState := func(w *mockInjectionWriter) *commitWalkState {
		return &commitWalkState{
			mfs:            &MutableFS{},
			writer:         w,
			origChunkIndex: idx,
		}
	}

	t.Run("single batch spanning two chunks", func(t *testing.T) {
		w := newMockInjectionWriter(t, 10000)
		ow := newState(w)

		ow.pendingRefs = []commitEntry{ce("a", 100, 7500)}
		if err := ow.flushPendingRefs(false); err != nil {
			t.Fatal(err)
		}
		assertRefsBacked(t, w)
	})

	t.Run("final flush backs held chunk", func(t *testing.T) {
		w := newMockInjectionWriter(t, 10000)
		ow := newState(w)

		ow.pendingRefs = []commitEntry{ce("a", 100, 7500)}
		if err := ow.flushPendingRefs(true); err != nil {
			t.Fatal(err)
		}
		if err := ow.flushPendingRefs(false); err != nil {
			t.Fatal(err)
		}
		assertRefsBacked(t, w)
	})

	t.Run("continued ranges inject shared chunk once", func(t *testing.T) {
		w := newMockInjectionWriter(t, 10000)
		ow := newState(w)

		ow.pendingRefs = []commitEntry{ce("a", 100, 7500)}
		if err := ow.flushPendingRefs(true); err != nil {
			t.Fatal(err)
		}
		ow.pendingRefs = []commitEntry{ce("b", 7616, 200)}
		if err := ow.flushPendingRefs(false); err != nil {
			t.Fatal(err)
		}

		var injected uint64
		for _, span := range w.spans {
			if span.injected {
				injected += span.end - span.start
			}
		}
		if injected != 2*chunkSize {
			t.Fatalf("injected %d bytes, want %d", injected, 2*chunkSize)
		}
		assertRefsBacked(t, w)
	})

	t.Run("payload write between batches", func(t *testing.T) {
		w := newMockInjectionWriter(t, 10000)
		ow := newState(w)

		ow.pendingRefs = []commitEntry{ce("a", 100, 7500)}
		if err := ow.flushPendingRefs(false); err != nil {
			t.Fatal(err)
		}

		modified := &pxar.Entry{Path: "modified", Kind: pxar.KindFile}
		if err := w.WriteEntryReader(modified, nil, 484); err != nil {
			t.Fatal(err)
		}

		ow.pendingRefs = []commitEntry{ce("d", 8100, 3800)}
		if err := ow.flushPendingRefs(false); err != nil {
			t.Fatal(err)
		}

		if len(w.refs) != 2 {
			t.Fatalf("expected 2 refs, got %d", len(w.refs))
		}
		if w.refs[1].start <= w.refs[0].start {
			t.Errorf("ref offsets not strictly increasing: %d then %d", w.refs[0].start, w.refs[1].start)
		}
		assertRefsBacked(t, w)
	})
}
