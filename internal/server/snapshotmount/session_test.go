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
