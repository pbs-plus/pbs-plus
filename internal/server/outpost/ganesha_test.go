//go:build linux

package outpost

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

type fakeGaneshaBus struct {
	pingErr    error
	addErr     error
	removeErr  error
	added      map[uint16]string
	removedIDs []uint16
}

func (f *fakeGaneshaBus) Ping() error { return f.pingErr }

func (f *fakeGaneshaBus) AddExport(confPath, expr string) error {
	if f.addErr != nil {
		return f.addErr
	}
	data, err := os.ReadFile(confPath)
	if err != nil {
		return err
	}
	if f.added == nil {
		f.added = map[uint16]string{}
	}
	var id uint16
	if _, err := fmt.Sscanf(string(data), "EXPORT {\n\tExport_Id = %d;", &id); err != nil {
		return err
	}
	f.added[id] = string(data)
	return nil
}

func (f *fakeGaneshaBus) RemoveExport(id uint16) error {
	if f.removeErr != nil {
		return f.removeErr
	}
	f.removedIDs = append(f.removedIDs, id)
	delete(f.added, id)
	return nil
}

func withStatePrefix(t *testing.T) string {
	t.Helper()
	old := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = old })
	return conf.StatePrefix
}

func TestGaneshaValidateSectype(t *testing.T) {
	d := ganeshaDriver{}
	valid := []string{"", "sys", "krb5,krb5i", "krb5p"}
	for _, sectype := range valid {
		o := Outpost{Name: "edge", Type: TypeGanesha, Sectype: sectype}
		if err := d.Validate(o); err != nil {
			t.Fatalf("sectype %q: %v", sectype, err)
		}
	}
	o := Outpost{Name: "edge", Type: TypeGanesha, Sectype: "tls"}
	if err := d.Validate(o); err == nil {
		t.Fatal("invalid sectype accepted")
	}
}

func TestGaneshaStartUnreachable(t *testing.T) {
	withStatePrefix(t)
	oldDial := dialGanesha
	t.Cleanup(func() { dialGanesha = oldDial })
	dialGanesha = func() (ganeshaExportMgr, error) {
		return &fakeGaneshaBus{pingErr: errors.New("service unknown")}, nil
	}
	if _, err := (ganeshaDriver{}).Start(t.Context(), Outpost{Name: "edge", Type: TypeGanesha}); err == nil {
		t.Fatal("start succeeded without a reachable ganesha")
	}
}

func TestGaneshaAttachDetach(t *testing.T) {
	withStatePrefix(t)
	bus := &fakeGaneshaBus{}
	inst, err := startGanesha(t, bus)
	if err != nil {
		t.Fatal(err)
	}

	path := "/var/run/pbs-plus-mounts/shares/key-one"
	if err := inst.Attach(Attachment{Name: "snap-one", ReadOnly: true, Path: path}); err != nil {
		t.Fatal(err)
	}
	id := ganeshaExportID("edge", "snap-one")
	frag, ok := bus.added[id]
	if !ok {
		t.Fatalf("export %d not added via dbus", id)
	}
	for _, want := range []string{
		"Export_Id =", path, "Pseudo = /snap-one;", "Access_Type = RO;",
		"SecType = krb5i;", "Protocols = 3;", "Name = VFS;",
	} {
		if !strings.Contains(frag, want) {
			t.Fatalf("fragment missing %q:\n%s", want, frag)
		}
	}
	if _, err := os.Stat(ganeshaFragmentPath("edge", "snap-one")); err != nil {
		t.Fatalf("fragment file missing: %v", err)
	}
	if got := inst.Attached(); len(got) != 1 || got[0] != "snap-one" {
		t.Fatalf("attached = %v", got)
	}
	if ep := inst.Endpoint("snap-one"); !strings.HasSuffix(ep, path) {
		t.Fatalf("endpoint = %q", ep)
	}

	if err := inst.Detach("snap-one"); err != nil {
		t.Fatal(err)
	}
	if len(bus.added) != 0 {
		t.Fatalf("export not removed: %v", bus.added)
	}
	if _, err := os.Stat(ganeshaFragmentPath("edge", "snap-one")); !os.IsNotExist(err) {
		t.Fatal("fragment file not removed")
	}
	if got := inst.Attached(); len(got) != 0 {
		t.Fatalf("attached after detach = %v", got)
	}
}

func TestGaneshaAttachRequiresPath(t *testing.T) {
	withStatePrefix(t)
	inst, err := startGanesha(t, &fakeGaneshaBus{})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Attach(Attachment{Name: "snap"}); err == nil {
		t.Fatal("attach without path accepted")
	}
}

func TestGaneshaAttachFailureCleansFragment(t *testing.T) {
	withStatePrefix(t)
	inst, err := startGanesha(t, &fakeGaneshaBus{addErr: errors.New("boom")})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Attach(Attachment{Name: "snap", Path: "/x"}); err == nil {
		t.Fatal("attach succeeded despite dbus error")
	}
	if _, err := os.Stat(ganeshaFragmentPath("edge", "snap")); !os.IsNotExist(err) {
		t.Fatal("fragment survived a failed AddExport")
	}
}

func TestGaneshaExportIDRange(t *testing.T) {
	seen := map[uint16]bool{}
	for _, name := range []string{"a", "b", "edge-nfs", "ganesha-1"} {
		for _, share := range []string{"snap-one", "snap-two", "host-2026"} {
			id := ganeshaExportID(name, share)
			if id < 4096 || id > 65535 {
				t.Fatalf("id %d out of range", id)
			}
			if seen[id] {
				t.Fatalf("collision on id %d", id)
			}
			seen[id] = true
		}
	}
}

func startGanesha(t *testing.T, bus ganeshaExportMgr) (*ganeshaInstance, error) {
	t.Helper()
	oldDial := dialGanesha
	t.Cleanup(func() { dialGanesha = oldDial })
	dialGanesha = func() (ganeshaExportMgr, error) { return bus, nil }
	inst, err := (ganeshaDriver{}).Start(t.Context(), Outpost{Name: "edge", Type: TypeGanesha})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = inst.Stop() })
	return inst.(*ganeshaInstance), nil
}
