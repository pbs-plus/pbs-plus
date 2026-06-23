//go:build linux

package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
)

func TestFlattenMtfJobShape(t *testing.T) {
	job := store.MTFJob{
		ID:         "mtf-1",
		SourceKind: "family",
		SourceRef:  "42",
		Datastore:  "local",
		Namespace:  "base",
		History: store.JobHistory{
			LastRunUpid:    "UPID:abc",
			LastRunState:   "OK",
			LastRunStatus:  database.JobStatusSuccess,
			LastRunEndtime: 1700000000,
		},
	}

	flat := flattenMtfJob(job)

	if flat.LastRunUpid != "UPID:abc" {
		t.Errorf("LastRunUpid = %q", flat.LastRunUpid)
	}
	if flat.LastRunState != "OK" {
		t.Errorf("LastRunState = %q", flat.LastRunState)
	}
	if flat.LastRunStatus != int(database.JobStatusSuccess) {
		t.Errorf("LastRunStatus = %d", flat.LastRunStatus)
	}
	if flat.StatusParsed.Category != "ok" || flat.StatusParsed.Icon == "" {
		t.Errorf("StatusParsed not populated: %+v", flat.StatusParsed)
	}

	// The JS model + grid read flat `last-run-*` and `status_parsed` keys.
	b, err := json.Marshal(flat)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(b)
	for _, key := range []string{
		`"last-run-upid":"UPID:abc"`,
		`"last-run-state":"OK"`,
		`"last-run-endtime":1700000000`,
		`"status_parsed":{`,
		`"id":"mtf-1"`,
		`"datastore":"local"`,
	} {
		if !strings.Contains(body, key) {
			t.Errorf("missing %q in JSON: %s", key, body)
		}
	}
}

func TestParseTaskStatusStates(t *testing.T) {
	cases := map[string]string{
		"OK":           "ok",
		"":             "",
		"WARNINGS: x":  "warning",
		"unknown":      "unknown",
		"FAILED: boom": "error",
		"QUEUED: wait": "queued",
	}
	for in, wantCat := range cases {
		got := ParseTaskStatus(in)
		if got.Category != wantCat {
			t.Errorf("ParseTaskStatus(%q).Category = %q, want %q", in, got.Category, wantCat)
		}
	}
}
