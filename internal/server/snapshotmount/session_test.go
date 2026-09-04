//go:build linux

package snapshotmount

import (
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func TestSessionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	s := Session{
		Datastore:  "ds1",
		Namespace:  "ns/a",
		BackupType: "host",
		BackupID:   "id1",
		BackupTime: "2026-01-02T03:04:05Z",
		FileName:   "root.mpxar.didx",
		Mode:       ModeRW,
		MountPoint: "/mnt/custom",
		OverlayDir: filepath.Join(dir, "mount-overlays", "k"),
		SocketPath: "/var/run/pbs-plus-mounts/k.sock",
		ServiceKey: "k",
	}
	if err := SaveSession(s); err != nil {
		t.Fatal(err)
	}

	got, err := LoadSession("k")
	if err != nil {
		t.Fatal(err)
	}
	if got != s {
		t.Fatalf("session mismatch: %+v != %+v", got, s)
	}
	if got.ServiceName() != "pbs-plus-snapshot-mount-k.service" {
		t.Fatalf("service name = %s", got.ServiceName())
	}

	list, err := ListSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ServiceKey != "k" {
		t.Fatalf("list = %+v", list)
	}

	found, ok, err := FindSessionByMountPoint("/mnt/custom")
	if err != nil || !ok || found.ServiceKey != "k" {
		t.Fatalf("find = %+v ok=%v err=%v", found, ok, err)
	}

	if _, ok, _ := FindSessionByMountPoint("/mnt/other"); ok {
		t.Fatal("unexpected session found")
	}

	if err := DeleteSession("k"); err != nil {
		t.Fatal(err)
	}
	list, err = ListSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("list after delete = %+v", list)
	}
}

func TestValidateMountPath(t *testing.T) {
	cases := []struct {
		path string
		ok   bool
	}{
		{"", true},
		{"/mnt/foo", true},
		{"/mnt/deep/nested/path", true},
		{"/mnt", false},
		{"/", false},
		{"/var/mnt/foo", false},
		{"/mnt/../etc", false},
		{"mnt/foo", false},
		{"", true},
	}
	for _, c := range cases {
		err := ValidateMountPath(c.path)
		if (err == nil) != c.ok {
			t.Errorf("ValidateMountPath(%q) = %v, want ok=%v", c.path, err, c.ok)
		}
	}
}

func TestShareNameOverride(t *testing.T) {
	s := Session{BackupType: "host", BackupID: "vm1", BackupTime: "2026-01-02T03:04:05Z", ServiceKey: "k"}
	auto := ShareName(s)
	s.ShareName = "restore-latest"
	if got := ShareName(s); got != "restore-latest" {
		t.Fatalf("ShareName override ignored: %q (auto was %q)", got, auto)
	}
	if got := ShareName(Session{BackupType: "host", BackupID: "vm1", ServiceKey: "k"}); got == "" {
		t.Fatal("generated ShareName empty")
	}
}

func TestEnsureShareNameFree(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	if err := SaveSession(Session{
		Datastore: "ds1", BackupType: "host", BackupID: "vm1",
		BackupTime: "2026-01-02T03:04:05Z", FileName: "root.mpxar.didx",
		Mode: ModeRO, Outpost: "edge", ShareName: "restore",
		ServiceKey: "k1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := ensureShareNameFree("edge", "RESTORE", "k2"); err == nil {
		t.Fatal("duplicate share name accepted")
	}
	if err := ensureShareNameFree("edge", "restore", "k1"); err != nil {
		t.Fatalf("own preserved session rejected: %v", err)
	}
	if err := ensureShareNameFree("other", "restore", "k2"); err != nil {
		t.Fatalf("other outpost rejected: %v", err)
	}
	if err := ensureShareNameFree("edge", "other", "k2"); err != nil {
		t.Fatalf("distinct name rejected: %v", err)
	}
}
