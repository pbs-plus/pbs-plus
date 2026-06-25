package agentfs

import (
	"bytes"
	"sync"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

func BenchmarkAttrEncodeViaMarshal(b *testing.B) {
	info := types.AgentFileInfo{
		Name: "test_file.txt",
		Size: 1048576,
		Mode: 0644,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sinkBytes, _ = cbor.Marshal(info)
	}
}

func BenchmarkAttrEncodePooled(b *testing.B) {
	info := types.AgentFileInfo{
		Name: "test_file.txt",
		Size: 1048576,
		Mode: 0644,
	}
	pool := &sync.Pool{New: func() any { return new(bytes.Buffer) }}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buf := pool.Get().(*bytes.Buffer)
		buf.Reset()
		cbor.NewEncoder(buf).Encode(info)
		sinkBytes = make([]byte, buf.Len())
		copy(sinkBytes, buf.Bytes())
		pool.Put(buf)
	}
}

var sinkBytes []byte
