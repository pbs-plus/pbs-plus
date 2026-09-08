package sevenzip

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/sevenzip/internal/lzma"
	ulzma "github.com/ulikunitz/xz/lzma"
)

func benchStream(b *testing.B) []byte {
	b.Helper()
	payload := makePayload("mixed", 8<<20, 99)
	stream := xzCompressB(b, payload)
	return stream
}

func xzCompressB(b *testing.B, payload []byte) []byte {
	b.Helper()
	tb := &testing.T{}
	stream := xzCompress(tb, payload, "-6")
	return stream
}

func BenchmarkEngineUlikunitz(b *testing.B) {
	stream := benchStream(b)
	b.SetBytes(8 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		r, err := ulzma.NewReader(bytes.NewReader(stream))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineFused(b *testing.B) {
	stream := benchStream(b)
	props := stream[:5]
	size := binary.LittleEndian.Uint64(stream[5:13])
	b.SetBytes(8 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		r, err := lzma.NewReader(props, size, []io.ReadCloser{io.NopCloser(bytes.NewReader(stream[13:]))})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			b.Fatal(err)
		}
	}
}
