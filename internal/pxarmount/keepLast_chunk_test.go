package pxarmount

import (
	"io"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
)

type mockInjectionWriter struct {
	noopWriter
	enc         *encoder.Encoder
	injectCalls []injectCall
}

type injectCall struct {
	chunks     []backupproxy.KnownChunkRef
	encoderPos uint64
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

func (w *mockInjectionWriter) WriteEntryRef(_ *pxar.Entry, _ uint64) error {
	return nil
}

func (w *mockInjectionWriter) InjectChunks(chunks []backupproxy.KnownChunkRef) error {
	w.injectCalls = append(w.injectCalls, injectCall{
		chunks:     chunks,
		encoderPos: w.enc.PayloadPosition(),
	})
	var total uint64
	for _, c := range chunks {
		total += c.Size
	}
	return w.enc.Advance(total)
}

func (w *mockInjectionWriter) Encoder() *encoder.Encoder { return w.enc }

// TestKeepLastChunkInvariant verifies that keepLastChunk=true is only safe
// when no payload-writing entry intervenes between flushes. When a payload
// entry advances the encoder position between two batches, the saved chunk
// from batch 1 would be injected at the wrong position, corrupting refs.
//
// The fix: all flushPendingRefs calls before payload-writing entries use
// keepLastChunk=false (matching Rust's flush_cached_reusing_if_below_threshold
// with keep_last_chunk=false for non-reusable entries).
func TestKeepLastChunkInvariant(t *testing.T) {
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

	t.Run("false_before_payload_all_chunks_injected_contiguously", func(t *testing.T) {
		const startPos = 10000
		w := newMockInjectionWriter(t, startPos)

		// Batch 1: file spanning 2 chunks, keepLastChunk=false
		ow := &commitWalkState{
			mfs:            &MutableFS{},
			writer:         w,
			origChunkIndex: idx,
			pendingRefs: []commitEntry{
				ce("a", 100, 7500),
			},
		}
		if err := ow.flushPendingRefs("", false); err != nil {
			t.Fatal(err)
		}
		if ow.hasLastChunk {
			t.Error("expected hasLastChunk=false")
		}

		posAfterBatch1 := w.enc.PayloadPosition()

		// Simulate payload-writing entry (e.g. modified file)
		_ = w.enc.Advance(500)

		// Batch 2: file filling most of third chunk
		ow.pendingRefs = []commitEntry{ce("d", 8100, 3800)}
		if err := ow.flushPendingRefs("", false); err != nil {
			t.Fatal(err)
		}

		// All injections should be contiguous — no gap
		// Batch 1 injected 2 chunks at startPos (size 8000 total)
		// Batch 2 injected 1 chunk at posAfterBatch1+500
		if len(w.injectCalls) != 2 {
			t.Fatalf("expected 2 inject calls, got %d", len(w.injectCalls))
		}

		// Batch 1: both chunks at startPos (encoder starts at startPos+16 due to payload start marker)
		encoderStart := startPos + uint64(format.HeaderSize)
		batch1 := w.injectCalls[0]
		if batch1.encoderPos != encoderStart {
			t.Errorf("batch 1 at %d, expected %d", batch1.encoderPos, encoderStart)
		}
		if len(batch1.chunks) != 2 {
			t.Errorf("batch 1: %d chunks, expected 2", len(batch1.chunks))
		}

		// Batch 2: chunk after the 500-byte payload entry
		batch2 := w.injectCalls[1]
		expectedBatch2Pos := posAfterBatch1 + 500
		if batch2.encoderPos != expectedBatch2Pos {
			t.Errorf("batch 2 at %d, expected %d", batch2.encoderPos, expectedBatch2Pos)
		}
	})

	t.Run("true_before_payload_creates_gap", func(t *testing.T) {
		const startPos = 10000
		w := newMockInjectionWriter(t, startPos)

		ow := &commitWalkState{
			mfs:            &MutableFS{},
			writer:         w,
			origChunkIndex: idx,
			pendingRefs: []commitEntry{
				ce("a", 100, 7500),
			},
		}
		if err := ow.flushPendingRefs("", true); err != nil {
			t.Fatal(err)
		}

		posAfterBatch1 := w.enc.PayloadPosition()
		_ = w.enc.Advance(500) // payload entry

		ow.pendingRefs = []commitEntry{ce("d", 8100, 3800)}
		if err := ow.flushPendingRefs("", false); err != nil {
			t.Fatal(err)
		}

		// The saved chunk from batch 1 is injected after the 500-byte payload,
		// creating a gap where refs from batch 1 expect contiguous chunk data.
		var savedChunkPos uint64
		for _, call := range w.injectCalls {
			if call.encoderPos > posAfterBatch1 {
				savedChunkPos = call.encoderPos
				break
			}
		}

		gap := savedChunkPos - posAfterBatch1
		if gap != 500 {
			t.Errorf("expected 500-byte gap from payload entry, got %d", gap)
		}

		// This demonstrates WHY keepLastChunk=true before payload entries is wrong:
		// ref 'a' at [10100, 17616) expects chunk data at [14016, 18016)
		// but the saved chunk is at [14516, 18516) — 500-byte gap of payload data
		refA := uint64(startPos + 100)
		refAEnd := refA + 7500 + uint64(format.HeaderSize) // 17616
		injectedEnd := posAfterBatch1                      // 14016

		if refAEnd > injectedEnd && refAEnd <= injectedEnd+500 {
			t.Logf("CONFIRMED: %d bytes of ref 'a' [%d,%d) land in the 500-byte payload gap — data corruption", refAEnd-injectedEnd, injectedEnd, refAEnd)
		}
	})
}
