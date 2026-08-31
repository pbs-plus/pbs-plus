package arpc

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
)

// benchRoundTrip measures the codec work of one batch status probe round trip.
func benchRoundTrip(b *testing.B, headers http.Header) {
	enc, _ := cbor.EncOptions{}.EncMode()
	dec, _ := cbor.DecOptions{}.DecMode()

	drives := make([]fswire.TargetStatusReq, 10)
	for i := range drives {
		drives[i] = fswire.TargetStatusReq{Drive: "C:"}
	}
	reqPayload, err := enc.Marshal(fswire.TargetStatusBatchReq{Drives: drives})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wire bytes.Buffer

		if err := enc.NewEncoder(&wire).Encode(Request{Method: "target_status_batch", Payload: reqPayload, Headers: headers}); err != nil {
			b.Fatal(err)
		}

		var req Request
		if err := dec.NewDecoder(&wire).Decode(&req); err != nil {
			b.Fatal(err)
		}
		var batchReq fswire.TargetStatusBatchReq
		if err := cbor.Unmarshal(req.Payload, &batchReq); err != nil {
			b.Fatal(err)
		}

		respDrives := make(map[string]fswire.TargetDriveStatus, len(batchReq.Drives))
		for _, d := range batchReq.Drives {
			respDrives[d.Drive] = fswire.TargetDriveStatus{Reachable: new(true), Message: "ready (Type: 3)"}
		}
		respData, err := cbor.Marshal(fswire.TargetStatusBatchResp{Version: "1.0.1", Drives: respDrives})
		if err != nil {
			b.Fatal(err)
		}
		if err := enc.NewEncoder(&wire).Encode(Response{Status: 200, Data: respData}); err != nil {
			b.Fatal(err)
		}

		var resp Response
		if err := dec.NewDecoder(&wire).Decode(&resp); err != nil {
			b.Fatal(err)
		}
		var batchResp fswire.TargetStatusBatchResp
		if err := dec.Unmarshal(resp.Data, &batchResp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProbeRoundTripNoHeaders(b *testing.B) {
	benchRoundTrip(b, nil)
}

func BenchmarkProbeRoundTripWithHeaders(b *testing.B) {
	h := http.Header{}
	h.Set("ARPCVersion", "2")
	h.Set("Authorization", "Bearer some-agent-token-value")
	h.Set("X-Client", "agent")
	benchRoundTrip(b, h)
}
