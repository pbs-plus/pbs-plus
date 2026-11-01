package arpc

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

type cleanupTestStream struct {
	input     *bytes.Reader
	writes    int
	failWrite int
}

func (s *cleanupTestStream) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return s.input.Read(p)
}

func (s *cleanupTestStream) Write(p []byte) (int, error) {
	s.writes++
	if s.writes == s.failWrite {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func (*cleanupTestStream) Close() error                { return nil }
func (*cleanupTestStream) SetDeadline(time.Time) error { return nil }

func TestResponseCleanupOnEveryExit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ready      []byte
		failWrite  int
		handlerErr bool
		panicRaw   bool
		wantStream bool
	}{
		{name: "response write fails", failWrite: 1},
		{name: "peer closes before ready"},
		{name: "invalid ready byte", ready: []byte{0}},
		{name: "ack write fails", ready: readySignal, failWrite: 2},
		{name: "handler error", handlerErr: true},
		{name: "success", ready: readySignal, wantStream: true},
		{name: "stream panics", ready: readySignal, wantStream: true, panicRaw: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter()
			cleaned, streamed := 0, false
			r.Handle("test", func(*Request) (Response, error) {
				resp := Response{
					Status: StatusRawStream,
					Cleanup: func() {
						cleaned++
					},
					RawStream: func(ARPCStream) {
						streamed = true
						if tc.panicRaw {
							panic("stream failure")
						}
					},
				}
				if tc.handlerErr {
					return resp, errors.New("handler failure")
				}
				return resp, nil
			})
			input, err := cbor.Marshal(Request{Method: "test"})
			if err != nil {
				t.Fatal(err)
			}
			s := &cleanupTestStream{input: bytes.NewReader(append(input, tc.ready...)), failWrite: tc.failWrite}
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				r.serveStream(s)
			}()
			if cleaned != 1 {
				t.Fatalf("cleanup ran %d times, want once", cleaned)
			}
			if streamed != tc.wantStream || panicked != tc.panicRaw {
				t.Fatalf("streamed=%v panic=%v, want %v %v", streamed, panicked, tc.wantStream, tc.panicRaw)
			}
		})
	}
}

func TestResponseCleanupIsNotSerialized(t *testing.T) {
	encoded, err := cbor.Marshal(Response{Status: 200, Cleanup: func() { t.Fatal("unexpected cleanup") }})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := cbor.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 {
		t.Fatalf("wire response changed: %v", fields)
	}
}
