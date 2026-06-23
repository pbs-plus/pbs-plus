package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
)

func testDB(t *testing.T) *Database {
	t.Helper()
	dir := t.TempDir()
	db, err := Initialize(context.Background(), filepath.Join(dir, "mtf.db"))
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMapperRegexCapturesAndTokens(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if _, err := db.CreateMapping(ctx, NamespaceMapping{
		Name:       "unc-to-ns",
		Priority:   10,
		MatchRegex: `^\\\\([^.\\]+)\.[^\\]+\\([A-Za-z]):$`,
		Template:   "{machine.label}/$1/$2",
		Enabled:    true,
	}); err != nil {
		t.Fatalf("CreateMapping: %v", err)
	}

	m := NewMapper(db)
	vol := DataSetVolume{
		Device:      `\\DP-D010.sgl.lan\D:`,
		MachineName: "DP-D010.sgl.lan",
		VolumeLabel: "Data",
	}
	got, err := m.Map(ctx, vol)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if want := "DP/DP-D010/D"; got != want {
		t.Errorf("Map namespace = %q, want %q", got, want)
	}
}

func TestMapperTokenFallback(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	m := NewMapper(db)
	vol := DataSetVolume{
		Device:      `\\SRV01\D:`,
		MachineName: "SRV01",
	}
	got, err := m.Map(ctx, vol)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if want := "tape/SRV01/D"; got != want {
		t.Errorf("default Map = %q, want %q", got, want)
	}
}

func TestMapperRegexNoMatchFallsThrough(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.CreateMapping(ctx, NamespaceMapping{
		Name: "only-c-drive", Priority: 1, MatchRegex: `\\C:`, Template: "system", Enabled: true,
	}); err != nil {
		t.Fatalf("CreateMapping: %v", err)
	}
	m := NewMapper(db)
	got, err := m.Map(ctx, DataSetVolume{Device: `\\HOST\D:`, MachineName: "HOST"})
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if want := "tape/HOST/D"; got != want {
		t.Errorf("fallback Map = %q, want %q", got, want)
	}
}

func TestMapperInvalidRegexRejected(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	m := NewMapper(db)
	if _, err := db.CreateMapping(ctx, NamespaceMapping{
		Name: "bad", Priority: 1, MatchRegex: `(unclosed`, Template: "x", Enabled: true,
	}); err != nil {
		t.Fatalf("CreateMapping: %v", err)
	}
	if _, err := m.Map(ctx, DataSetVolume{Device: "x"}); err == nil {
		t.Fatalf("expected error for invalid regex")
	}
}

func TestMapAllBatch(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	vols := []DataSetVolume{
		{Device: `\\H1\D:`, MachineName: "H1"},
		{Device: `\\H2\E:`, MachineName: "H2"},
	}
	m := NewMapper(db)
	out, err := m.MapAll(ctx, vols)
	if err != nil {
		t.Fatalf("MapAll: %v", err)
	}
	want := []string{"tape/H1/D", "tape/H2/E"}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("MapAll[%d] = %q, want %q", i, out[i], want[i])
		}
	}
}

func TestSanitizeNS(t *testing.T) {
	cases := map[string]string{
		"/a/b/":     "a/b",
		"a//b":      "a/_/b",
		"a/../b":    "a/_/b",
		"a/./b":     "a/_/b",
		"  spaced ": "spaced",
		"":          "",
	}
	for in, want := range cases {
		if got := sanitizeNS(in); got != want {
			t.Errorf("sanitizeNS(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDriveLetter(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`\\DP-D010.sgl.lan\D:`, "D"},
		{`\\H\C$`, "C"},
		{`\\H\E$\`, "E"},
		{`C:`, "C"},
		{"", ""},
	}
	for _, c := range cases {
		if got := driveLetter(c.in); got != c.want {
			t.Errorf("driveLetter(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestJobCRUD(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if _, err := db.queries.UpsertMediaFamily(ctx, mtfquery.UpsertMediaFamilyParams{
		ID:         42,
		Name:       sql.NullString{String: "fam", Valid: true},
		TotalTapes: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("UpsertMediaFamily: %v", err)
	}

	job, err := db.CreateMtfJob(ctx, MTFJob{
		ID: "my-mtf-job", SourceKind: "family", SourceRef: "42",
		Datastore: "local", Namespace: "base", Spanning: true,
		Retry: 2, RetryInterval: 5,
	})
	if err != nil {
		t.Fatalf("CreateMtfJob: %v", err)
	}
	if job.Retry != 2 || job.RetryInterval != 5 {
		t.Errorf("retry config = %d/%d, want 2/5", job.Retry, job.RetryInterval)
	}

	got, err := db.GetMtfJob(ctx, "my-mtf-job")
	if err != nil {
		t.Fatalf("GetMtfJob: %v", err)
	}
	if got.Datastore != "local" {
		t.Errorf("datastore = %q", got.Datastore)
	}

	got.Comment = "updated"
	if err := db.UpdateMtfJob(ctx, got); err != nil {
		t.Fatalf("UpdateMtfJob: %v", err)
	}
	got2, _ := db.GetMtfJob(ctx, "my-mtf-job")
	if got2.Comment != "updated" {
		t.Errorf("comment = %q", got2.Comment)
	}

	hist := JobHistory{LastRunUpid: "UPID:...", LastRunStatus: database.JobStatusSuccess}
	if err := db.UpdateMtfJobHistory(ctx, "my-mtf-job", hist, ""); err != nil {
		t.Fatalf("UpdateMtfJobHistory: %v", err)
	}

	jobs, err := db.ListMtfJobs(ctx)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListMtfJobs = %v (len %d), err %v", jobs, len(jobs), err)
	}

	if err := db.DeleteMtfJob(ctx, "my-mtf-job"); err != nil {
		t.Fatalf("DeleteMtfJob: %v", err)
	}
}
