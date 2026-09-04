//go:build linux

package outpost

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func withStatePrefix(t *testing.T) string {
	t.Helper()
	old := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = old })
	return conf.StatePrefix
}

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
	inst, err := (sambaDriver{}).Start(t.Context(), Outpost{Name: "edge", Type: TypeSamba, Guest: true})
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

func withFakeNet(t *testing.T, err error) *[][]string {
	t.Helper()
	var calls [][]string
	old := runNet
	runNet = func(args ...string) error {
		calls = append(calls, args)
		return err
	}
	t.Cleanup(func() { runNet = old })
	return &calls
}

func TestSambaValidateAccessPolicy(t *testing.T) {
	d := sambaDriver{}
	cases := []struct {
		name    string
		o       Outpost
		wantErr string
	}{
		{"guest only", Outpost{Guest: true}, ""},
		{"local users only", Outpost{ValidUsers: "restore, ops"}, ""},
		{"both", Outpost{Guest: true, ValidUsers: "restore"}, "mutually exclusive"},
		{"neither", Outpost{}, "set valid users or enable guest access"},
		{"stanza injection", Outpost{ValidUsers: "ops\n[evil]\npath = /"}, "valid users must not"},
		{"force user injection", Outpost{Guest: true, ForceUser: "root]\n["}, "force user must not"},
		{"hosts allow injection", Outpost{Guest: true, HostsAllow: "10.0.0.0/8\n[x]"}, "hosts allow must not"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := d.Validate(tc.o)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestSambaValidateDomainRequiresJoin(t *testing.T) {
	calls := withFakeNet(t, errors.New("not joined"))
	err := (sambaDriver{}).Validate(Outpost{ValidUsers: `CORP\restore-ops`})
	if err == nil || !strings.Contains(err.Error(), "not joined to a domain") {
		t.Fatalf("err = %v, want domain join error", err)
	}
	if len(*calls) != 1 || strings.Join((*calls)[0], " ") != "ads testjoin" {
		t.Fatalf("net calls = %v", *calls)
	}
}

func TestSambaValidateDomainAcceptsJoinedHost(t *testing.T) {
	withFakeNet(t, nil)
	for _, users := range []string{`CORP\restore`, "restore@CORP.EXAMPLE", `@CORP\backup-admins`} {
		if err := (sambaDriver{}).Validate(Outpost{ValidUsers: users}); err != nil {
			t.Fatalf("%s: %v", users, err)
		}
	}
}

func TestSambaValidateLocalUsersSkipDomainCheck(t *testing.T) {
	calls := withFakeNet(t, errors.New("not joined"))
	if err := (sambaDriver{}).Validate(Outpost{ValidUsers: "restore, @operators"}); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 0 {
		t.Fatalf("net consulted for local accounts: %v", *calls)
	}
}

func TestSambaShareStanzaAppliesAccessPolicy(t *testing.T) {
	got := sambaShareStanza(Outpost{
		ValidUsers: `CORP\restore, @CORP\backup-admins`,
		ForceUser:  "root",
		HostsAllow: "10.0.0.0/8",
	}, Attachment{Name: "snap", Path: "/mnt/snap", ReadOnly: true})
	for _, want := range []string{
		"[snap]", "path = /mnt/snap", "browseable = no", "read only = yes",
		"guest ok = no", `valid users = CORP\restore, @CORP\backup-admins`,
		"force user = root", "hosts allow = 10.0.0.0/8",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stanza missing %q:\n%s", want, got)
		}
	}
}

func TestSambaShareStanzaBrowseable(t *testing.T) {
	hidden := sambaShareStanza(Outpost{Guest: true}, Attachment{Name: "snap", Path: "/mnt/snap"})
	if !strings.Contains(hidden, "browseable = no") {
		t.Fatalf("default stanza not hidden:\n%s", hidden)
	}
	shown := sambaShareStanza(Outpost{Guest: true, Browseable: true}, Attachment{Name: "snap", Path: "/mnt/snap"})
	if !strings.Contains(shown, "browseable = yes") {
		t.Fatalf("browseable stanza hidden:\n%s", shown)
	}
}

func TestSambaShareStanzaGuestWritable(t *testing.T) {
	got := sambaShareStanza(Outpost{Guest: true}, Attachment{Name: "snap", Path: "/mnt/snap"})
	for _, want := range []string{"guest ok = yes", "read only = no"} {
		if !strings.Contains(got, want) {
			t.Fatalf("stanza missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "valid users") {
		t.Fatalf("guest share got valid users:\n%s", got)
	}
}
