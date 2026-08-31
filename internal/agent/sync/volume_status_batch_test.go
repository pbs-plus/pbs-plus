package sync

import (
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

func TestStatusBatchHandler(t *testing.T) {
	dir := t.TempDir()
	payload, err := cbor.Marshal(fswire.TargetStatusBatchReq{Drives: []fswire.TargetStatusReq{
		{Drive: dir},
		{Drive: "/nonexistent-probe-path-xyz"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := StatusBatchHandler(&arpc.Request{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}

	var out fswire.TargetStatusBatchResp
	if err := cbor.Unmarshal(resp.Data, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Drives) != 2 {
		t.Fatalf("want 2 drive results, got %d", len(out.Drives))
	}
	if st := out.Drives[dir]; st.Reachable == nil || !*st.Reachable {
		t.Fatalf("existing path should be reachable: %#v", st)
	}
	if st := out.Drives["/nonexistent-probe-path-xyz"]; st.Reachable == nil || *st.Reachable {
		t.Fatalf("bogus path should be unreachable: %#v", st)
	}
}
