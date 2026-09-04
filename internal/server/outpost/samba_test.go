//go:build linux

package outpost

import (
	"errors"
	"os"
	"strings"
	"testing"
)

type fakeSmbcontrol struct {
	calls   [][]string
	failOn  string
	failErr error
}

func (f *fakeSmbcontrol) run(args ...string) error {
	f.calls = append(f.calls, args)
	if len(args) >= 2 && args[0]+" "+args[1] == f.failOn {
		return f.failErr
	}
	return nil
}

func withFakeSmbcontrol(t *testing.T) *fakeSmbcontrol {
	t.Helper()
	fake := &fakeSmbcontrol{}
	old := runSmbcontrol
	runSmbcontrol = fake.run
	t.Cleanup(func() { runSmbcontrol = old })
	return fake
}

func startSamba(t *testing.T) Instance {
	t.Helper()
	withStatePrefix(t)
	withFakeSmbcontrol(t)
	inst, err := (sambaDriver{}).Start(t.Context(), Outpost{Name: "edge", Type: TypeSamba})
	if err != nil {
		t.Fatalf("start samba: %v", err)
	}
	t.Cleanup(func() { _ = inst.Stop() })
	return inst
}

func TestSambaStartRequiresSmbd(t *testing.T) {
	withStatePrefix(t)
	fake := withFakeSmbcontrol(t)
	fake.failOn = "smbd ping"
	fake.failErr = errors.New("no reply")
	if _, err := (sambaDriver{}).Start(t.Context(), Outpost{Name: "edge", Type: TypeSamba}); err == nil {
		t.Fatal("start succeeded without a running smbd")
	}
}

func TestSambaAttachDetach(t *testing.T) {
	inst := startSamba(t)

	path := "/var/run/pbs-plus-mounts/shares/key-two"
	if err := inst.Attach(Attachment{Name: "snap-two", ReadOnly: false, Path: path}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sambaIncludePath("edge"))
	if err != nil {
		t.Fatalf("include file missing: %v", err)
	}
	for _, want := range []string{
		"[snap-two]", "path = " + path, "read only = no",
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("include file missing %q:\n%s", want, data)
		}
	}
	if got := inst.Attached(); len(got) != 1 || got[0] != "snap-two" {
		t.Fatalf("attached = %v", got)
	}
	if ep := inst.Endpoint("snap-two"); !strings.HasPrefix(ep, "smb://") || !strings.HasSuffix(ep, "/snap-two") {
		t.Fatalf("endpoint = %q", ep)
	}

	if err := inst.Attach(Attachment{Name: "snap-ro", ReadOnly: true, Path: "/x"}); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(sambaIncludePath("edge"))
	if !strings.Contains(string(data), "read only = yes") {
		t.Fatalf("read-only flag missing:\n%s", data)
	}

	if err := inst.Detach("snap-two"); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(sambaIncludePath("edge"))
	if strings.Contains(string(data), "[snap-two]") {
		t.Fatalf("share survived detach:\n%s", data)
	}
	if got := inst.Attached(); len(got) != 1 || got[0] != "snap-ro" {
		t.Fatalf("attached after detach = %v", got)
	}
}

func TestSambaAttachRequiresPath(t *testing.T) {
	inst := startSamba(t)
	if err := inst.Attach(Attachment{Name: "snap"}); err == nil {
		t.Fatal("attach without path accepted")
	}
}

func TestSambaAttachRollsBackOnReloadFailure(t *testing.T) {
	withStatePrefix(t)
	fake := withFakeSmbcontrol(t)
	fake.failOn = "smbd reload-config"
	fake.failErr = errors.New("reload failed")
	inst, err := (sambaDriver{}).Start(t.Context(), Outpost{Name: "edge", Type: TypeSamba})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Attach(Attachment{Name: "snap", Path: "/x"}); err == nil {
		t.Fatal("attach succeeded despite reload failure")
	}
	if got := inst.Attached(); len(got) != 0 {
		t.Fatalf("share not rolled back: %v", got)
	}
}
