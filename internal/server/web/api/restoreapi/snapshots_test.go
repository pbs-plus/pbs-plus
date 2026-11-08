//go:build linux

package restoreapi

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/pbsapi"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func snapshot(id string, time int64, files ...string) pbsapi.Snapshot {
	s := pbsapi.Snapshot{BackupType: "host", BackupID: id, BackupTime: time}
	for _, f := range files {
		s.Files = append(s.Files, pbsapi.SnapshotFile{Filename: f})
	}
	return s
}

func TestRestoreSnapshots(t *testing.T) {
	targets := []coredb.Target{
		{Name: "web.example", Type: coredb.TargetTypeFilesystem},
		{Name: "pg", Type: coredb.TargetTypePostgreSQL},
		{Name: "mail", Type: coredb.TargetTypeDovecot},
	}
	snapshots := []pbsapi.Snapshot{
		snapshot("web", 1, "web-example.pxar.didx", "web-example.mpxar.didx", "index.json.blob"),
		snapshot("db", 3, "pg.pxar.didx"),
		snapshot("mail", 2, "mail.pxar.didx"),
		snapshot("orphan", 4, "gone.pxar.didx"),
	}

	ids := func(list []RestoreSnapshot) []string {
		out := make([]string, len(list))
		for i, s := range list {
			out[i] = s.BackupID
		}
		return out
	}
	equal := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	for _, tc := range []struct {
		dest string
		want []string
	}{
		{"", []string{"orphan", "db", "mail", "web"}},
		{"web.example", []string{"orphan", "web"}},
		{"pg", []string{"db"}},
		{"mail", []string{"mail"}},
	} {
		if got := ids(restoreSnapshots(snapshots, targets, tc.dest)); !equal(got, tc.want) {
			t.Errorf("dest %q: got %v, want %v", tc.dest, got, tc.want)
		}
	}

	all := restoreSnapshots(snapshots, targets, "")
	web := all[len(all)-1]
	if web.SourceTarget != "web.example" || web.Archive != "web-example.mpxar.didx" || web.Value != "host/web/1" {
		t.Errorf("web snapshot = %#v", web)
	}
	if all[0].SourceTarget != "" || all[0].Archive != "gone.pxar.didx" {
		t.Errorf("orphan snapshot = %#v", all[0])
	}
}
