//go:build linux

package arpcfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
)

func watchFS(t *testing.T, subpath string, stat func(context.Context, string) error) (*ARPCFS, func() string) {
	t.Helper()
	fs := &ARPCFS{VFSBase: vfs.InjectBase(vfs.VFSBase{Backup: coredb.Backup{ID: "job", Subpath: subpath}})}
	fs.sourceStat = stat
	wasServer := log.L.Server
	log.L.Server = true
	t.Cleanup(func() { log.L.Server = wasServer })
	jobID := "pbsplus-test-" + strings.NewReplacer("/", "-", " ", "-", ",", "-").Replace(t.Name())
	fs.jobLog = log.WithScope(log.Scope{JobID: jobID})
	if err := fs.jobLog.FlushJobLog(); err != nil {
		t.Fatalf("flush job log: %v", err)
	}
	t.Cleanup(func() {
		fs.jobLog.Close()
		_ = os.Remove(filepath.Join(os.TempDir(), "job-"+jobID+"-stdout"))
	})
	read := func() string {
		if err := fs.jobLog.FlushJobLog(); err != nil {
			t.Fatalf("flush job log: %v", err)
		}
		p := fs.jobLog.JobLogPath()
		if p == "" {
			return ""
		}
		b, err := os.ReadFile(p)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read job log: %v", err)
		}
		return string(b)
	}
	return fs, read
}

func TestSourceRootJoinsSubpath(t *testing.T) {
	for sub, want := range map[string]string{
		"":                                       "/",
		"run/ingest/backup/SER/part1-container1": "/run/ingest/backup/SER/part1-container1",
		"/Data/":                                 "/Data",
		`Users\alice`:                            "/Users/alice",
	} {
		fs, _ := watchFS(t, sub, nil)
		if got := fs.sourceRoot(); got != want {
			t.Errorf("sourceRoot(%q) = %q, want %q", sub, got, want)
		}
	}
}

func TestNoteMissingWarnsOnceWhenSourceVanishes(t *testing.T) {
	var checks atomic.Int32
	fs, logged := watchFS(t, "run/ingest/backup/SER/part1-container1", func(_ context.Context, root string) error {
		checks.Add(1)
		return &os.PathError{Op: "stat", Path: root, Err: os.ErrNotExist}
	})
	gone := &os.PathError{Op: "xstat", Path: "/run/ingest/backup/SER/part1-container1/WeeklyReport", Err: os.ErrNotExist}
	for range 5 {
		fs.noteMissing(t.Context(), "/run/ingest/backup/SER/part1-container1/WeeklyReport", gone)
	}
	if !fs.sourceGone.Load() {
		t.Fatal("vanished source not detected")
	}
	if n := checks.Load(); n != 1 {
		t.Errorf("source checked %d times, want once", n)
	}
	out := logged()
	if strings.Count(out, "backup source disappeared") != 1 || !strings.Contains(out, "/run/ingest/backup/SER/part1-container1") {
		t.Errorf("want exactly one warning naming the source, got:\n%s", out)
	}
}

func TestNoteMissingIgnoresLiveSourceAndUnknowns(t *testing.T) {
	cases := map[string]struct {
		path string
		err  error
		stat error
	}{
		"file deleted, source still there": {"/data/a.txt", os.ErrNotExist, nil},
		"pxarexclude probe":                {"/data/.pxarexclude", os.ErrNotExist, &os.PathError{Err: os.ErrNotExist}},
		"session lost":                     {"/data/a.txt", fmt.Errorf("%w: pipe", ErrSessionLost), &os.PathError{Err: os.ErrNotExist}},
		"transport error on check":         {"/data/a.txt", os.ErrNotExist, errors.New("rpc timeout")},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fs, logged := watchFS(t, "data", func(context.Context, string) error { return tc.stat })
			fs.noteMissing(t.Context(), tc.path, tc.err)
			if out := logged(); fs.sourceGone.Load() || strings.Contains(out, "disappeared") {
				t.Fatalf("unexpected vanish detection, log:\n%s", out)
			}
		})
	}
}

func TestNoteMissingWarningIsCountedAsTaskWarning(t *testing.T) {
	fs, logged := watchFS(t, "data", func(_ context.Context, root string) error {
		return &os.PathError{Op: "stat", Path: root, Err: os.ErrNotExist}
	})
	fs.noteMissing(t.Context(), "/data/x", os.ErrNotExist)
	if out := logged(); !strings.Contains(out, "WARNING: ") {
		t.Errorf("warning lacks the marker mergePBSLogs counts:\n%s", out)
	}
}
