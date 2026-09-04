//go:build linux

package snapshotmount

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func TestProfileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	p := Profile{
		Datastore: "ds1", Namespace: "ns/a", Outpost: "edge-smb", ShareName: "archive",
		Mode: ModeRW, Schedule: "mon..fri 02:00", AutoMount: true, Replace: true,
	}
	id := p.ID()
	if id == "" {
		t.Fatal("empty profile id")
	}
	if err := SaveProfile(p); err != nil {
		t.Fatal(err)
	}

	got, ok, err := LoadProfile(id)
	if err != nil || !ok {
		t.Fatalf("load ok=%v err=%v", ok, err)
	}
	if got != p {
		t.Fatalf("profile mismatch: %+v != %+v", got, p)
	}

	list, err := ListProfiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID() != id {
		t.Fatalf("list = %+v", list)
	}

	if err := DeleteProfile(id); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := LoadProfile(id); ok {
		t.Fatal("profile survived delete")
	}

	p.Outpost = "edge-nfs"
	if p.ID() == id {
		t.Fatal("id not target-scoped")
	}
	p.Outpost = ""
	if p.ID() == id {
		t.Fatal("local batch id collides with outpost id")
	}
}

func TestValidateProfile(t *testing.T) {
	valid := Profile{Datastore: "ds1"}
	cases := []struct {
		mutate func(*Profile)
		ok     bool
	}{
		{func(p *Profile) {}, true},
		{func(p *Profile) { p.Datastore = "" }, false},
		{func(p *Profile) { p.Datastore = "../x" }, false},
		{func(p *Profile) { p.Namespace = "bad ns" }, false},
		{func(p *Profile) { p.Mode = "rw" }, true},
		{func(p *Profile) { p.Mode = "readwrite" }, false},
		{func(p *Profile) { p.Outpost = "edge-nfs" }, true},
		{func(p *Profile) { p.Outpost = "Bad_Name" }, false},
		{func(p *Profile) { p.ShareName = "arch" }, false},
		{func(p *Profile) { p.Outpost = "edge"; p.ShareName = "arch" }, true},
		{func(p *Profile) { p.Outpost = "edge"; p.ShareName = "bad name" }, false},
		{func(p *Profile) { p.MountPath = "/mnt/x" }, true},
		{func(p *Profile) { p.MountPath = "/var/x" }, false},
		{func(p *Profile) { p.Outpost = "edge"; p.MountPath = "/mnt/x" }, false},
		{func(p *Profile) { p.Schedule = "mon..fri 02:00" }, true},
		{func(p *Profile) { p.Schedule = "not a schedule" }, false},
	}
	for i, c := range cases {
		p := valid
		c.mutate(&p)
		err := ValidateProfile(p)
		if (err == nil) != c.ok {
			t.Errorf("case %d: ValidateProfile = %v, want ok=%v", i, err, c.ok)
		}
	}
}

func TestLoadProfileRejectsPathEscape(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	if _, _, err := LoadProfile("../evil"); err == nil {
		t.Fatal("path escape accepted")
	}
	if _, _, err := LoadProfile("a/b"); err == nil {
		t.Fatal("slash in id accepted")
	}
}

func TestLatestSnapshotIn(t *testing.T) {
	root := t.TempDir()
	group := filepath.Join(root, "ns", "a", "host", "id1")
	write := func(dir, file string) {
		if err := os.MkdirAll(filepath.Join(group, dir), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(group, dir, file), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("2026-01-01T00:00:00Z", "root.pxar.didx")
	write("2026-02-01T00:00:00Z", "notes.txt")
	write("2026-03-01T00:00:00Z", "root.mpxar.didx")
	if err := os.MkdirAll(filepath.Join(group, "not-a-time"), 0o755); err != nil {
		t.Fatal(err)
	}

	backupTime, fileName, err := LatestSnapshotIn(root, "a", "host", "id1")
	if err != nil {
		t.Fatal(err)
	}
	if fileName != "root.mpxar.didx" {
		t.Fatalf("fileName = %s", fileName)
	}
	if backupTime != "2026-03-01T00:00:00Z" {
		t.Fatalf("backupTime = %s", backupTime)
	}

	if _, _, err := LatestSnapshotIn(root, "", "host", "missing"); err == nil {
		t.Fatal("missing group accepted")
	}
}

func TestProfileSkipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	p := Profile{Datastore: "nonexistent", Namespace: "ns1", Mode: "ro"}
	s := Session{Profile: p.ID(), Datastore: p.Datastore, Namespace: "ns1", BackupType: "host", BackupID: "vm1"}
	RecordProfileSkip(s)

	skips := loadSkips(p.ID())
	if len(skips) != 1 {
		t.Fatalf("expected one skip entry, got %d", len(skips))
	}
	if _, ok := skips[groupKeyOf("ns1", "host", "vm1")]; !ok {
		t.Errorf("skip keyed by wrong group: %v", skips)
	}

	ReconcileProfileNow(context.Background(), nil, p)
	if skips := loadSkips(p.ID()); len(skips) != 0 {
		t.Errorf("mount-now must clear manual unmount skips, got %v", skips)
	}
}
