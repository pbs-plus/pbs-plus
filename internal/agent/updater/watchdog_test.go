package updater

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func writeFakeExe(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestAtomicWriteFileReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "target")
	if err := os.WriteFile(dst, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := atomicWriteFile(dst, []byte("new"), 0o755); err != nil {
		t.Fatalf("atomicWriteFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("got %q want new", got)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".tmp-update-*"))
	if len(matches) != 0 {
		t.Fatalf("leftover temp files: %v", matches)
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := writeFakeExe(t, dir, "src", "good-binary")
	dst := filepath.Join(dir, "dst")
	if err := copyFile(dst, src, 0o755); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "good-binary" {
		t.Fatalf("got %q want good-binary", got)
	}
}

func TestSwapBinary(t *testing.T) {
	dir := t.TempDir()
	exe := writeFakeExe(t, dir, "agent", "current")
	staged := writeFakeExe(t, dir, "agent.staged", "candidate")

	if err := swapBinary(staged, exe); err != nil {
		t.Fatalf("swapBinary: %v", err)
	}

	got, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "candidate" {
		t.Fatalf("exe = %q, want candidate", got)
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatalf("staged file should be gone after swap, got err=%v", err)
	}
}

func TestSanityCheckBinary(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		ok   bool
	}{
		{"too small", []byte{1, 2}, false},
		{"html", []byte("<!doctype html><html>"), false},
		{"empty", []byte{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := sanityCheckBinary(c.data)
			if c.ok && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !c.ok && err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}

	var magic []byte
	switch runtime.GOOS {
	case "windows":
		magic = append([]byte{'M', 'Z'}, make([]byte, 100)...)
	default:
		magic = append([]byte{0x7f, 'E', 'L', 'F'}, make([]byte, 100)...)
	}
	if err := sanityCheckBinary(magic); err != nil {
		t.Fatalf("expected magic to pass: %v", err)
	}
}

func TestPendingRoundTrip(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	p := &pendingUpdate{Version: "v1.2.3", PreviousPath: exe + previousSuffix, Attempts: 2}
	if err := writePending(exe, p); err != nil {
		t.Fatalf("writePending: %v", err)
	}
	got, ok := readPending(exe)
	if !ok {
		t.Fatal("readPending: marker not found")
	}
	if got.Version != p.Version || got.Attempts != p.Attempts || got.PreviousPath != p.PreviousPath {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	removePending(exe)
	if _, ok := readPending(exe); ok {
		t.Fatal("marker still present after removePending")
	}
}

func TestEvaluatePendingCommitsWithinBudget(t *testing.T) {
	dir := t.TempDir()
	exe := writeFakeExe(t, dir, "agent", "new")
	prev := writeFakeExe(t, dir, "agent"+previousSuffix, "old")

	if err := writePending(exe, &pendingUpdate{Version: "v9", PreviousPath: prev}); err != nil {
		t.Fatal(err)
	}

	for range maxBootAttempts {
		pending, rollback := evaluatePending(exe)
		if rollback {
			t.Fatalf("should not roll back within budget")
		}
		if !pending {
			t.Fatalf("should report pending while under evaluation")
		}
	}

	removePending(exe)
	if _, ok := readPending(exe); ok {
		t.Fatal("marker should be gone after commit")
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "new" {
		t.Fatalf("exe changed unexpectedly: %q", got)
	}
}

func TestEvaluatePendingRollsBackPastBudget(t *testing.T) {
	dir := t.TempDir()
	exe := writeFakeExe(t, dir, "agent", "new-bad")
	prev := writeFakeExe(t, dir, "agent"+previousSuffix, "old-good")

	if err := writePending(exe, &pendingUpdate{Version: "v9", PreviousPath: prev, Attempts: maxBootAttempts}); err != nil {
		t.Fatal(err)
	}

	pending, rollback := evaluatePending(exe)
	if pending {
		t.Fatal("should not report pending after rollback")
	}
	if !rollback {
		t.Fatal("should report rollback after exceeding budget")
	}

	if _, ok := readPending(exe); ok {
		t.Fatal("marker should be cleared after rollback")
	}
	got, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old-good" {
		t.Fatalf("exe = %q, want old-good", got)
	}
}

func TestEvaluatePendingRollbackWithoutPreviousFailsGracefully(t *testing.T) {
	dir := t.TempDir()
	exe := writeFakeExe(t, dir, "agent", "new-bad")
	if err := writePending(exe, &pendingUpdate{Version: "v9", PreviousPath: filepath.Join(dir, "missing"), Attempts: maxBootAttempts}); err != nil {
		t.Fatal(err)
	}

	pending, rollback := evaluatePending(exe)
	if pending || rollback {
		t.Fatalf("expected (false,false) on missing rollback artifact, got (%v,%v)", pending, rollback)
	}
	if _, ok := readPending(exe); ok {
		t.Fatal("marker should be cleared to avoid a permanent loop")
	}
}
