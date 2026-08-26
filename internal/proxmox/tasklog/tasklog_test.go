//go:build linux

package tasklog

import (
	"fmt"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func setupTaskDirs(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("PBS_PLUS_TEST_TASKS", dir)
	origTaskDir, origActive, origArchive, origLock := taskDir, activeTasks, archivePath, lockPath
	taskDir, activeTasks, archivePath, lockPath = dir, filepath.Join(dir, "active"), filepath.Join(dir, "archive"), filepath.Join(dir, ".active.lock")
	t.Cleanup(func() {
		taskDir, activeTasks, archivePath, lockPath = origTaskDir, origActive, origArchive, origLock
	})
}

func TestWorkerTask_Lifecycle(t *testing.T) {
	setupTaskDirs(t)

	wt, err := NewWorkerTask("pbsplus", "test", "abc")
	if err != nil {
		t.Fatal(err)
	}
	upid := wt.UPID()

	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].UPID != upid || active[0].State != nil {
		t.Fatalf("active = %#v, want single running %s", active, upid)
	}

	wt.LogString("working")
	wt.CloseErr(fmt.Errorf("boom"))

	active, err = readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active after close = %#v, want empty", active)
	}

	arch, err := readTaskFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(arch) != 1 || arch[0].UPID != upid || arch[0].State == nil || arch[0].State.Status != StatusError {
		t.Fatalf("archive = %#v, want finished error entry for %s", arch, upid)
	}

	state, err := ReadStatusFromLog(upid)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != StatusError || state.Message != "boom" {
		t.Fatalf("ReadStatusFromLog = %#v, want error boom", state)
	}

	task, err := GetTaskByUPID(upid)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "stopped" || task.ExitStatus != "boom" {
		t.Fatalf("GetTaskByUPID = %#v, want stopped/boom", task)
	}
}

func TestReconcile_FoldsDeadWorkers(t *testing.T) {
	setupTaskDirs(t)

	dead := NewTask("pbsplus", "dead", "x")
	dead.PID = 999999999
	dead.PStart = 1
	upid := dead.GenerateUPID()

	path := filepath.Join(taskDir, fmt.Sprintf("%02X", dead.PStart&0xFF), upid)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	logLine := time.Now().Add(-time.Minute).Format(time.RFC3339) + ": TASK OK\n"
	if err := os.WriteFile(path, []byte(logLine), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(activeTasks, []byte(upid+"\n"), 0660); err != nil {
		t.Fatal(err)
	}

	if err := Reconcile(""); err != nil {
		t.Fatal(err)
	}

	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active = %#v, want empty after reconcile", active)
	}
	arch, err := readTaskFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(arch) != 1 || arch[0].State == nil || arch[0].State.Status != StatusOK {
		t.Fatalf("archive = %#v, want folded OK entry", arch)
	}
}

func TestConcurrentInterop_ExternalFlock(t *testing.T) {
	setupTaskDirs(t)

	ext, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(ext.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		wt, err := NewWorkerTask("pbsplus", "interop", "z")
		if err != nil {
			done <- err
			return
		}
		wt.CloseOK()
		done <- nil
	}()

	select {
	case err := <-done:
		t.Fatalf("NewWorkerTask finished while external lock held: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := syscall.Flock(int(ext.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("NewWorkerTask never completed after lock release")
	}

	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active = %#v, want empty", active)
	}
}

func TestConcurrentWorkers(t *testing.T) {
	setupTaskDirs(t)

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wt, err := NewWorkerTask("pbsplus", "conc", fmt.Sprintf("w%d", i))
			if err != nil {
				errs <- err
				return
			}
			wt.LogString("hello")
			if i%2 == 0 {
				wt.CloseOK()
			} else {
				wt.CloseWarn(2)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active = %#v, want empty", active)
	}
	arch, err := readTaskFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(arch) != n {
		t.Fatalf("archive entries = %d, want %d", len(arch), n)
	}

	listed, err := ListTasks(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != n {
		t.Fatalf("ListTasks = %d, want %d", len(listed), n)
	}
}

func TestRotateArchive(t *testing.T) {
	setupTaskDirs(t)

	wt, err := NewWorkerTask("pbsplus", "rot", "r")
	if err != nil {
		t.Fatal(err)
	}
	wt.LogString("pad")
	wt.CloseOK()

	if err := os.Truncate(archivePath, 0); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	line := RenderStatusLine(wt.UPID(), &TaskState{Status: StatusOK, EndTime: time.Now().Unix()})
	if err := os.WriteFile(archivePath, []byte(strings.Repeat(line, 10)), 0660); err != nil {
		t.Fatal(err)
	}

	rotated, err := RotateArchive(1, false, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !rotated {
		t.Fatal("expected rotation")
	}
	if _, err := os.Stat(archivePath + ".1"); err != nil {
		t.Fatalf("archive.1 missing: %v", err)
	}

	fresh := NewTask("pbsplus", "rot", "r2")
	if err := os.WriteFile(archivePath, []byte(RenderStatusLine(fresh.GenerateUPID(), &TaskState{Status: StatusOK, EndTime: time.Now().Unix()})), 0660); err != nil {
		t.Fatal(err)
	}
	if _, err := RotateArchive(1, true, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(archivePath + ".1.gz"); err != nil {
		t.Fatalf("archive.1.gz missing: %v", err)
	}

	listed, err := ListTasks(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) == 0 {
		t.Fatal("rotated entries not visible via ListTasks")
	}
}

func TestEncodeToHexEscapes(t *testing.T) {
	cases := map[string]string{
		"plain":       "plain",
		"has.dots":    "has.dots",
		"under_score": "under_score",
		"a/b":         "a-b",
		":":           "\\x3a",
		"sp ace":      "sp\\x20ace",
		".leading":    "\\x2eleading",
		"mid.dle":     "mid.dle",
		"héllo":       "h\\xc3\\xa9llo",
	}
	for in, want := range cases {
		if got := proxmox.EncodeToHexEscapes(in); got != want {
			t.Errorf("escape(%q) = %q, want %q", in, got, want)
		}
	}
}
