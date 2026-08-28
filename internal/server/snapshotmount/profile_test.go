//go:build linux

package snapshotmount

import (
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
		Datastore: "ds1", Namespace: "ns/a", BackupType: "host", BackupID: "id1",
		Mode: ModeRW, MountPath: "/mnt/p", Schedule: "mon..fri 02:00", AutoMount: true,
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

	p.BackupID = "id2"
	if p.ID() == id {
		t.Fatal("id not group-scoped")
	}
}

func TestValidateProfile(t *testing.T) {
	valid := Profile{Datastore: "ds1", BackupType: "host", BackupID: "id1"}
	cases := []struct {
		mutate func(*Profile)
		ok     bool
	}{
		{func(p *Profile) {}, true},
		{func(p *Profile) { p.Datastore = "" }, false},
		{func(p *Profile) { p.Datastore = "../x" }, false},
		{func(p *Profile) { p.Namespace = "bad ns" }, false},
		{func(p *Profile) { p.BackupType = "weird" }, false},
		{func(p *Profile) { p.BackupID = "" }, false},
		{func(p *Profile) { p.Mode = "rw" }, true},
		{func(p *Profile) { p.Mode = "readwrite" }, false},
		{func(p *Profile) { p.MountPath = "/mnt/x" }, true},
		{func(p *Profile) { p.MountPath = "/var/x" }, false},
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
	write("2026-01-01_00-00-00", "root.pxar.didx")
	write("2026-02-01_00-00-00", "notes.txt")
	write("2026-03-01_00-00-00", "root.mpxar.didx")
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
