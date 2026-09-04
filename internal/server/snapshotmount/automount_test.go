//go:build linux

package snapshotmount

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGroupKeyOfCollidesOnlyOnSameGroup(t *testing.T) {
	a := groupKeyOf("ns", "host", "id1")
	if a != groupKeyOf("ns", "host", "id1") {
		t.Fatal("same group produced different keys")
	}
	for _, other := range []string{
		groupKeyOf("ns2", "host", "id1"),
		groupKeyOf("ns", "vm", "id1"),
		groupKeyOf("ns", "host", "id2"),
	} {
		if a == other {
			t.Fatal("different groups produced same key")
		}
	}
}

func TestFollowGateDue(t *testing.T) {
	loc := time.FixedZone("test", 0)
	oldLocal := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = oldLocal })

	tick := time.Date(2026, 3, 1, 2, 5, 0, 0, loc)
	gate := &followGate{}

	if !gate.due(Profile{Schedule: ""}, tick) {
		t.Fatal("empty schedule must always be due")
	}
	if !gate.due(Profile{Schedule: "total nonsense ;;;"}, tick) {
		t.Fatal("unparseable schedule falls back to always due")
	}

	p := Profile{Datastore: "ds1", Namespace: "ns", Schedule: "02:00"}
	if gate.due(p, tick) {
		t.Fatal("first tick before any event since ref should not be due")
	}
	if gate.lastRun[p.ID()] != (time.Time{}) {
		t.Fatal("non-due check must not consume the event")
	}

	next := time.Date(2026, 3, 2, 2, 0, 0, 0, loc)
	if !gate.due(p, next.Add(time.Minute)) {
		t.Fatal("tick after the scheduled event must be due")
	}
	if gate.due(p, next.Add(6*time.Minute)) {
		t.Fatal("same event must not fire twice")
	}
	if !gate.due(p, next.Add(25*time.Hour)) {
		t.Fatal("next day event must fire")
	}
}

func TestPlanBatchSubPaths(t *testing.T) {
	groups := []NamespaceGroup{
		{Namespace: "", BackupType: "host", BackupID: "root-host"},
		{Namespace: "1988-PROJECTS", BackupType: "host", BackupID: "x"},
		{Namespace: "1990-PROJECTS", BackupType: "host", BackupID: "x"},
		{Namespace: "1990-PROJECTS", BackupType: "ct", BackupID: "y"},
		{Namespace: "1990-PROJECTS/inner", BackupType: "host", BackupID: "x"},
	}
	subs := planBatch(groups, "")

	want := map[string]string{
		groupKeyOf("", "host", "root-host"):            "host-root-host",
		groupKeyOf("1988-PROJECTS", "host", "x"):       "1988-PROJECTS",
		groupKeyOf("1990-PROJECTS", "host", "x"):       "1990-PROJECTS/host-x",
		groupKeyOf("1990-PROJECTS", "ct", "y"):         "1990-PROJECTS/ct-y",
		groupKeyOf("1990-PROJECTS/inner", "host", "x"): "1990-PROJECTS/inner",
	}
	for k, want := range want {
		if subs[k] != want {
			t.Errorf("sub[%s] = %q, want %q", k, subs[k], want)
		}
	}

	subs = planBatch([]NamespaceGroup{{Namespace: "parent/child", BackupType: "host", BackupID: "x"}}, "parent")
	if s := subs[groupKeyOf("parent/child", "host", "x")]; s != "child" {
		t.Fatalf("relative sub = %q, want child", s)
	}
}

func TestListNamespaceGroups(t *testing.T) {
	root := t.TempDir()
	mkdir := func(parts ...string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(append([]string{root}, parts...)...), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mkdir("host", "live-id", "2026-01-01T00:00:00Z")
	mkdir("ns", "1988-PROJECTS", "host", "xmedia", "2026-01-02T00:00:00Z")
	mkdir("ns", "1988-PROJECTS", "ns", "inner", "host", "x", "2026-01-03T00:00:00Z")
	mkdir("ns", "empty-ns")
	mkdir("ns", "no-snaps", "host", "gone")

	groups, err := ListNamespaceGroups(root, "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, g := range groups {
		got[g.Namespace+"\x00"+g.BackupType+"\x00"+g.BackupID] = true
	}
	for _, want := range []string{
		"\x00host\x00live-id",
		"1988-PROJECTS\x00host\x00xmedia",
		"1988-PROJECTS/inner\x00host\x00x",
	} {
		if !got[want] {
			t.Errorf("missing group %q in %v", want, got)
		}
	}
	if len(groups) != 3 {
		t.Fatalf("groups = %+v", groups)
	}

	if _, err := ListNamespaceGroups(root, "1988-PROJECTS"); err != nil {
		t.Fatal(err)
	}
	groups, err = ListNamespaceGroups(root, "1988-PROJECTS")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("scoped groups = %+v", groups)
	}
	if _, err := ListNamespaceGroups(root, "missing"); err == nil {
		t.Fatal("missing namespace accepted")
	}
}
