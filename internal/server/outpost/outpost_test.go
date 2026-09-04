//go:build linux

package outpost

import (
	"context"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	nfs "github.com/willscott/go-nfs"
)

func testOutpost() Outpost {
	return Outpost{Name: "test-outpost", Type: TypeNFS, ListenAddr: "127.0.0.1:0"}
}

func TestOutpostCRUD(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	o := testOutpost()
	if err := SaveOutpost(o); err != nil {
		t.Fatal(err)
	}
	got, ok, err := LoadOutpost(o.Name)
	if err != nil || !ok {
		t.Fatalf("load ok=%v err=%v", ok, err)
	}
	if got.Name != o.Name || got.Type != o.Type || got.ListenAddr != o.ListenAddr {
		t.Fatalf("mismatch: %+v", got)
	}

	list, err := ListOutposts()
	if err != nil || len(list) != 1 {
		t.Fatalf("list len=%d err=%v", len(list), err)
	}
	if err := DeleteOutpost(o.Name); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := LoadOutpost(o.Name); ok {
		t.Fatal("outpost survived delete")
	}
}

func TestValidateOutpost(t *testing.T) {
	cases := []struct {
		name string
		o    Outpost
		want string
	}{
		{"ok", testOutpost(), ""},
		{"bad name", Outpost{Name: "Bad_Name", Type: TypeNFS, ListenAddr: "0.0.0.0:2049"}, "invalid outpost name"},
		{"unknown type", Outpost{Name: "a", Type: "s3", ListenAddr: "0.0.0.0:2049"}, "unknown outpost type"},
		{"missing listen", Outpost{Name: "a", Type: TypeNFS}, "listen_addr is required"},
		{"bad listen", Outpost{Name: "a", Type: TypeNFS, ListenAddr: "no-port"}, "invalid listen_addr"},
	}
	for _, tc := range cases {
		err := ValidateOutpost(tc.o)
		if tc.want == "" && err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
		}
		if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.want)
		}
	}
}

func TestNFSMultiShareDispatch(t *testing.T) {
	inst, err := nfsDriver{}.Start(context.Background(), testOutpost())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Stop() })

	share1 := memfs.New()
	share2 := memfs.New()
	if err := inst.Attach(Attachment{Name: "snap-one", FS: share1}); err != nil {
		t.Fatal(err)
	}
	if err := inst.Attach(Attachment{Name: "snap-two", FS: share2}); err != nil {
		t.Fatal(err)
	}

	attached := inst.Attached()
	if len(attached) != 2 || attached[0] != "snap-one" || attached[1] != "snap-two" {
		t.Fatalf("attached = %v", attached)
	}

	ninst := inst.(*nfsInstance)
	mux := ninst.mux
	mount := func(dirpath string) (nfs.MountStatus, billy.Filesystem) {
		st, fs, _ := mux.Mount(context.Background(), nil, nfs.MountRequest{Dirpath: []byte(dirpath)})
		return st, fs
	}

	st1, fs1 := mount("/snap-one")
	if st1 != nfs.MountStatusOk || fs1 == nil {
		t.Fatalf("snap-one: status=%v", st1)
	}
	_, fs2 := mount("snap-two/")
	if fs2 == nil {
		t.Fatal("snap-two did not mount")
	}
	if fs1 == fs2 {
		t.Fatal("distinct shares must expose distinct filesystem values")
	}

	if f, err := fs1.Create("probe.txt"); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := share1.Stat("probe.txt"); err != nil {
		t.Fatalf("probe missing from share1: %v", err)
	}
	if _, err := share2.Stat("probe.txt"); err == nil {
		t.Fatal("probe leaked into share2")
	}

	if st, _ := mount("/"); st != nfs.MountStatusErrNoEnt {
		t.Fatalf("root export should not exist, got status %v", st)
	}
	if st, _ := mount("/missing"); st != nfs.MountStatusErrNoEnt {
		t.Fatalf("missing share should be NoEnt, got status %v", st)
	}
	if st, _ := mount("/snap-one/sub"); st != nfs.MountStatusErrNoEnt {
		t.Fatalf("nested path should be NoEnt, got status %v", st)
	}

	h1 := mux.ToHandle(fs1, []string{})
	h2 := mux.ToHandle(fs2, []string{})
	got1, _, err := mux.FromHandle(h1)
	if err != nil || got1 != fs1 {
		t.Fatalf("from handle 1: fs-mismatch=%v err=%v", got1 != fs1, err)
	}
	got2, _, err := mux.FromHandle(h2)
	if err != nil || got2 != fs2 {
		t.Fatalf("from handle 2: fs-mismatch=%v err=%v", got2 != fs2, err)
	}

	if err := inst.Detach("snap-one"); err != nil {
		t.Fatal(err)
	}
	if st, _ := mount("/snap-one"); st != nfs.MountStatusErrNoEnt {
		t.Fatalf("detached share still mountable, status %v", st)
	}
	if _, _, err := mux.FromHandle(h1); err == nil {
		t.Fatal("stale handle for detached share should error")
	}
}

func TestManagerAttachDetach(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() {
		StopAll()
		conf.StatePrefix = old
	})

	ctx := context.Background()
	o := testOutpost()
	o.ListenAddr = "127.0.0.1:0"
	if err := ApplyConfig(ctx, o); err != nil {
		t.Fatal(err)
	}

	if err := Attach("not-running", Attachment{Name: "x", FS: memfs.New()}); err == nil {
		t.Fatal("attach to unknown outpost should fail")
	}

	released := false
	rel := func() { released = true }
	if err := Attach(o.Name, Attachment{Name: "share", FS: memfs.New(), Release: rel}); err != nil {
		t.Fatal(err)
	}

	statuses := StatusAll()
	if len(statuses) != 1 || !statuses[0].Running {
		t.Fatalf("status = %+v", statuses)
	}
	if len(statuses[0].Attached) != 1 || statuses[0].Attached[0] != "share" {
		t.Fatalf("attached = %v", statuses[0].Attached)
	}
	if len(statuses[0].Endpoints) != 1 || statuses[0].Endpoints[0] == "" {
		t.Fatalf("endpoints = %v", statuses[0].Endpoints)
	}

	Detach(o.Name, "share")
	if !released {
		t.Fatal("Release was not called on Detach")
	}
	if att := StatusAll()[0].Attached; len(att) != 0 {
		t.Fatalf("share survived detach: %v", att)
	}

	StopAll()
	if s := StatusAll(); s[0].Running {
		t.Fatal("outpost still running after StopAll")
	}
}
