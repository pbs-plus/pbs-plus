//go:build linux

package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeSnapshot(t *testing.T, group, stamp string, files map[string]string, age time.Duration) string {
	t.Helper()
	dir := filepath.Join(group, stamp)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(dir, when, when); err != nil {
		t.Fatalf("chtimes %s: %v", dir, err)
	}
	return dir
}

func TestPruneIncompleteSnapshotsRemovesAbandonedRuns(t *testing.T) {
	group := t.TempDir()
	old := writeSnapshot(t, group, "2025-08-19T15:34:03Z", map[string]string{
		"DP-D030---D.mpxar.tmp_didx": "",
		"DP-D030---D.ppxar.tmp_didx": "x",
	}, 48*time.Hour)
	newer := writeSnapshot(t, group, "2026-10-12T23:30:38Z", map[string]string{
		"DP-D030---D.mpxar.tmp_didx": "x",
	}, time.Hour)

	if got := pruneIncompleteSnapshots(group); got != 2 {
		t.Fatalf("removed = %d, want 2", got)
	}
	for _, d := range []string{old, newer} {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Fatalf("%s still exists", d)
		}
	}
}

func TestPruneIncompleteSnapshotsKeepsCompletedSnapshots(t *testing.T) {
	group := t.TempDir()
	done := writeSnapshot(t, group, "2026-10-12T23:30:38Z", map[string]string{
		"index.json.blob":            "{}",
		"DP-D030---D.mpxar.didx":     "x",
		"DP-D030---D.ppxar.tmp_didx": "stray",
	}, 48*time.Hour)

	if got := pruneIncompleteSnapshots(group); got != 0 {
		t.Fatalf("removed = %d, want 0", got)
	}
	if _, err := os.Stat(done); err != nil {
		t.Fatalf("a snapshot with a manifest must survive: %v", err)
	}
}

func TestPruneIncompleteSnapshotsSparesALiveRun(t *testing.T) {
	group := t.TempDir()
	live := writeSnapshot(t, group, "2026-11-01T15:54:18Z", map[string]string{
		"DP-D030---D.ppxar.tmp_didx": "writing",
	}, time.Minute)

	if got := pruneIncompleteSnapshots(group); got != 0 {
		t.Fatalf("removed = %d, want 0 while inside the grace window", got)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("a snapshot being written must survive: %v", err)
	}
}

func TestPruneIncompleteSnapshotsIgnoresNonSnapshotEntries(t *testing.T) {
	group := t.TempDir()
	if err := os.WriteFile(filepath.Join(group, "owner"), []byte("plus-user@pbs!server"), 0o644); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	notes := writeSnapshot(t, group, "scratch", map[string]string{"a.tmp_didx": "x"}, 48*time.Hour)

	if got := pruneIncompleteSnapshots(group); got != 0 {
		t.Fatalf("removed = %d, want 0", got)
	}
	if _, err := os.Stat(notes); err != nil {
		t.Fatalf("a directory that is not a snapshot timestamp must survive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(group, "owner")); err != nil {
		t.Fatalf("the owner file must survive: %v", err)
	}
}

func TestPruneIncompleteSnapshotsToleratesAMissingGroup(t *testing.T) {
	if got := pruneIncompleteSnapshots(filepath.Join(t.TempDir(), "absent")); got != 0 {
		t.Fatalf("removed = %d, want 0", got)
	}
}

func TestSnapshotIsIncompleteCoversTheIndexVariants(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
		want  bool
	}{
		{"fixed index archive", map[string]string{"vm.img.tmp_fidx": "x"}, true},
		{"dynamic index archive", map[string]string{"host.pxar.tmp_didx": "x"}, true},
		{"finished", map[string]string{"index.json.blob": "{}", "host.pxar.didx": "x"}, false},
		{"empty directory", map[string]string{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeSnapshot(t, t.TempDir(), "2026-10-12T23:30:38Z", tc.files, time.Hour)
			if got := snapshotIsIncomplete(dir); got != tc.want {
				t.Fatalf("snapshotIsIncomplete = %v, want %v", got, tc.want)
			}
		})
	}
}
