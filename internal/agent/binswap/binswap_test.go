package binswap

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func writeFakeExe(t *testing.T, p, body string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

func TestAtomicWriteCreatesNew(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "target")
	if err := atomicWriteFile(dst, []byte("hello"), 0o755); err != nil {
		t.Fatalf("atomicWriteFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q want hello", got)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".tmp-binswap-*"))
	if len(matches) != 0 {
		t.Fatalf("leftover temp files: %v", matches)
	}
}

func TestAtomicWriteReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "target")
	writeFakeExe(t, dst, "old")
	if err := atomicWriteFile(dst, []byte("new"), 0o755); err != nil {
		t.Fatalf("atomicWriteFile: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "new" {
		t.Fatalf("got %q want new", got)
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	writeFakeExe(t, src, "good-binary")
	dst := filepath.Join(dir, "dst")
	if err := copyFile(dst, src, 0o755); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "good-binary" {
		t.Fatalf("got %q want good-binary", got)
	}
}

func TestStageAndSwap(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "current")

	m := New(exe)

	if err := m.Stage([]byte("candidate")); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	if err := m.Swap(); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	got, _ := os.ReadFile(exe)
	if string(got) != "candidate" {
		t.Fatalf("exe = %q, want candidate", got)
	}
	if _, err := os.Stat(m.stagedPath()); !os.IsNotExist(err) {
		t.Fatalf("staged should be gone, err=%v", err)
	}
}

func TestSwapFailsWithoutStaged(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "current")

	m := New(exe)
	if err := m.Swap(); err != ErrNotStaged {
		t.Fatalf("Swap without staged: got %v, want ErrNotStaged", err)
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "current" {
		t.Fatalf("exe changed on failed swap: %q", got)
	}
}

func TestSnapshotCurrent(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "known-good")

	m := New(exe)
	if err := m.SnapshotCurrent(); err != nil {
		t.Fatalf("SnapshotCurrent: %v", err)
	}
	if !m.PreviousExists() {
		t.Fatal("PreviousExists should be true")
	}
	got, _ := os.ReadFile(m.previousPath())
	if string(got) != "known-good" {
		t.Fatalf("previous = %q, want known-good", got)
	}
}

func TestRollback(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "bad-new")

	m := New(exe)
	writeFakeExe(t, m.previousPath(), "old-good")

	if err := m.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "old-good" {
		t.Fatalf("exe = %q, want old-good", got)
	}
}

func TestRollbackNoPrevious(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "current")

	m := New(exe)
	if err := m.Rollback(); err == nil {
		t.Fatal("Rollback without .previous should fail")
	}
}

func TestPendingRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := New(filepath.Join(dir, "agent"))

	if m.HasPending() {
		t.Fatal("should not have pending initially")
	}

	if err := m.MarkPending("v1.2.3"); err != nil {
		t.Fatalf("MarkPending: %v", err)
	}
	if !m.HasPending() {
		t.Fatal("should have pending after MarkPending")
	}
	if v := m.PendingVersion(); v != "v1.2.3" {
		t.Fatalf("PendingVersion = %q, want v1.2.3", v)
	}

	m.Commit()
	if m.HasPending() {
		t.Fatal("should not have pending after Commit")
	}
}

func TestCheckPendingNormalBoot(t *testing.T) {
	dir := t.TempDir()
	m := New(filepath.Join(dir, "agent"))

	pending, rollback := m.CheckPending()
	if pending || rollback {
		t.Fatalf("normal boot: got pending=%v rollback=%v, want false/false", pending, rollback)
	}
}

func TestCheckPendingWithinBudget(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	m := New(exe)

	writeFakeExe(t, m.previousPath(), "old-good")

	if err := m.MarkPending("v9"); err != nil {
		t.Fatal(err)
	}

	for i := range MaxBootAttempts {
		pending, rollback := m.CheckPending()
		if rollback {
			t.Fatalf("attempt %d: should not roll back within budget", i)
		}
		if !pending {
			t.Fatalf("attempt %d: should report pending", i)
		}
	}

	m.Commit()
	if m.HasPending() {
		t.Fatal("marker should be gone after Commit")
	}
}

func TestCheckPendingRollsBack(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "new-bad")

	m := New(exe)
	writeFakeExe(t, m.previousPath(), "old-good")

	if err := m.MarkPending("v9"); err != nil {
		t.Fatal(err)
	}

	for range MaxBootAttempts {
		m.CheckPending()
	}

	pending, rollback := m.CheckPending()
	if pending {
		t.Fatal("should not report pending after rollback")
	}
	if !rollback {
		t.Fatal("should report rollback after exceeding budget")
	}

	if m.HasPending() {
		t.Fatal("marker should be cleared after rollback")
	}
	got, _ := os.ReadFile(exe)
	if string(got) != "old-good" {
		t.Fatalf("exe = %q, want old-good", got)
	}
}

func TestCheckPendingRollbackMissingArtifact(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "new-bad")

	m := New(exe)
	if err := m.MarkPending("v9"); err != nil {
		t.Fatal(err)
	}

	for range MaxBootAttempts {
		m.CheckPending()
	}

	pending, rollback := m.CheckPending()
	if pending || rollback {
		t.Fatalf("missing artifact: got pending=%v rollback=%v, want false/false", pending, rollback)
	}
	if m.HasPending() {
		t.Fatal("marker should be cleared to avoid permanent loop")
	}
}

func TestPrune(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	m := New(exe)

	writeFakeExe(t, m.stagedPath(), "staged")
	writeFakeExe(t, m.swapTempPath(), "swapping")
	writeFakeExe(t, m.badPath(), "bad")
	writeFakeExe(t, m.previousPath(), "previous")

	m.Prune()

	for _, suffix := range []string{stagedSuffix, swapTempSuffix, badSuffix} {
		if _, err := os.Stat(exe + suffix); !os.IsNotExist(err) {
			t.Fatalf("artifact %s should be pruned, err=%v", suffix, err)
		}
	}
	if _, err := os.Stat(m.previousPath()); err != nil {
		t.Fatalf(".previous should be retained after Prune: %v", err)
	}
}

func TestCheckBinaryMagicValid(t *testing.T) {
	var valid []byte
	switch runtime.GOOS {
	case "windows":
		valid = append([]byte{'M', 'Z'}, make([]byte, 100)...)
	default:
		valid = append([]byte{0x7f, 'E', 'L', 'F'}, make([]byte, 100)...)
	}
	if err := CheckBinaryMagic(valid); err != nil {
		t.Fatalf("valid magic rejected: %v", err)
	}
}

func TestCheckBinaryMagicInvalid(t *testing.T) {
	cases := [][]byte{
		{},
		{1, 2},
		[]byte("<!doctype html><html>"),
		[]byte("NOT-A-BINARY-PAYLOAD"),
	}
	for _, data := range cases {
		if err := CheckBinaryMagic(data); err == nil {
			t.Fatalf("expected error for %q", data)
		}
	}
}

func TestFullSwapFlow(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "agent")
	writeFakeExe(t, exe, "original")

	m := New(exe)

	if err := m.Stage([]byte("new-version")); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if err := m.SnapshotCurrent(); err != nil {
		t.Fatalf("SnapshotCurrent: %v", err)
	}
	if err := m.MarkPending("v2.0"); err != nil {
		t.Fatalf("MarkPending: %v", err)
	}
	if err := m.Swap(); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	got, _ := os.ReadFile(exe)
	if string(got) != "new-version" {
		t.Fatalf("exe = %q, want new-version", got)
	}
	if !m.HasPending() {
		t.Fatal("should have pending marker")
	}
	if !m.PreviousExists() {
		t.Fatal("should have previous artifact")
	}

	pending, _ := m.CheckPending()
	if !pending {
		t.Fatal("first boot check should report pending")
	}

	m.Commit()
	if m.HasPending() {
		t.Fatal("should not have pending after Commit")
	}

	m.Prune()
	if !m.PreviousExists() {
		t.Fatal("previous should survive prune")
	}
}
