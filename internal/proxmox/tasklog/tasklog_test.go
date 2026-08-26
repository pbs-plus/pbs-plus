//go:build linux

package tasklog

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
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

func TestQueuedTask_CloseRemovesTaskWithoutArchiving(t *testing.T) {
	setupTaskDirs(t)

	queued, err := NewQueuedTask("backup", "abc", true)
	if err != nil {
		t.Fatal(err)
	}
	upid := queued.UPID()

	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].UPID != upid || active[0].State != nil {
		t.Fatalf("active = %#v, want single running %s", active, upid)
	}
	listed, err := ListTasks(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].UPID != upid || listed[0].State != nil {
		t.Fatalf("ListTasks(true) = %#v, want single running %s", listed, upid)
	}

	queued.Close()

	active, err = readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active after close = %#v, want empty", active)
	}
	archive, err := readTaskFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(archive) != 0 {
		t.Fatalf("archive after close = %#v, want empty", archive)
	}
	path, err := UPIDLogPath(upid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("queued task log still exists: %v", err)
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

// pbsUPIDRegex is PBS's PROXMOX_UPID_REGEX from proxmox-schema/src/upid.rs,
// verbatim, translated to RE2 syntax.
const pbsUPIDRegex = `^UPID:([a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?):([0-9A-Fa-f]{8}):` +
	`([0-9A-Fa-f]{8,9}):([0-9A-Fa-f]{8,16}):([0-9A-Fa-f]{8}):` +
	`([^:\s]+):([^:\s]*):([^:\s]+):$`

func TestPBSRegexAcceptsOurUPIDs(t *testing.T) {
	re := regexp.MustCompile(pbsUPIDRegex)

	wids := []string{
		formatTestWid("ds.store", "backup", "host.example.com"),
		formatTestWid("ds", "mtf-", "id"),
		"mtfscan-abc123",
		"",
	}
	nodes := []string{"pbsplus", "pbsplusgen-queue", "pbsplusgen-ok", "pbsplusgen-error"}
	for _, node := range nodes {
		for _, wid := range wids {
			task := NewTask(node, "backup", wid)
			if !re.MatchString(task.UPID) {
				t.Errorf("PBS regex rejects %q (wid %q)", task.UPID, wid)
			}
		}
	}
}

func formatTestWid(store, prefix, id string) string {
	return proxmox.EncodeToHexEscapes(store) +
		proxmox.EncodeToHexEscapes(":") +
		prefix + proxmox.EncodeToHexEscapes(id)
}

// pbsParseStatusLine is PBS's parse_worker_status_line: splitn(3, ' ').
func pbsParseStatusLine(t *testing.T, line string) (string, *TaskState) {
	t.Helper()
	data := strings.SplitN(line, " ", 3)
	switch len(data) {
	case 1:
		return data[0], nil
	case 3:
		endtime, err := strconv.ParseInt(data[1], 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		st, err := FromEndtimeAndMessage(endtime, data[2])
		if err != nil {
			t.Fatal(err)
		}
		return data[0], &st
	default:
		t.Fatalf("PBS parser rejects line %q", line)
		return "", nil
	}
}

func TestStatusLinePBSInterop(t *testing.T) {
	upid := "UPID:pbsplus:0000ABCD:000000AB:00001234:56789ABC:backup:ds\\x3astore:plus-user@pbs!server:"

	states := []TaskState{
		{Status: StatusOK, EndTime: 0x12345678},
		{Status: StatusWarning, EndTime: 0x12345678, WarnCount: 3},
		{Status: StatusError, EndTime: 0x12345678, Message: "mount failed at /mnt/data: no such file"},
		{Status: StatusUnknown, EndTime: 0x12345678},
	}
	for _, st := range states {
		line := strings.TrimRight(RenderStatusLine(upid, &st), "\n")

		gotUPID, gotState := pbsParseStatusLine(t, line)
		if gotUPID != upid {
			t.Errorf("PBS parser got upid %q, want %q", gotUPID, upid)
		}
		if gotState == nil || gotState.Status != st.Status || gotState.EndTime != st.EndTime ||
			gotState.Message != st.Message || gotState.WarnCount != st.WarnCount {
			t.Errorf("PBS parser roundtrip: got %+v, want %+v (line %q)", gotState, st, line)
		}

		ourUPID, ourState, err := ParseStatusLine(line)
		if err != nil || ourUPID != upid || ourState == nil ||
			ourState.Status != st.Status || ourState.Message != st.Message {
			t.Errorf("our parser: %v %+v for line %q", err, ourState, line)
		}
	}

	upidOnly := strings.TrimRight(RenderStatusLine(upid, nil), "\n")
	got, st := pbsParseStatusLine(t, upidOnly)
	if got != upid || st != nil {
		t.Errorf("running-entry roundtrip failed: %q %+v", got, st)
	}
}

func TestReadStatusFromLogPBSFindMapSemantics(t *testing.T) {
	setupTaskDirs(t)

	wt, err := NewWorkerTask("pbsplus", "findmap", "f")
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Now().Format(time.RFC3339)
	wt.mu.Lock()
	_, _ = wt.file.WriteString(ts + ": TASK WARNINGS: notanumber\n")
	_, _ = wt.file.WriteString(ts + ": TASK ERROR: real failure\n")
	wt.mu.Unlock()

	state, err := ReadStatusFromLog(wt.UPID())
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != StatusError || state.Message != "real failure" {
		t.Fatalf("ReadStatusFromLog = %+v, want error 'real failure'", state)
	}

	task, err := GetTaskByUPID(wt.UPID())
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "stopped" || task.ExitStatus != "real failure" {
		t.Fatalf("GetTaskByUPID = %+v, want stopped/real failure", task)
	}

	unregisterWorker(wt.Task.TaskId)
	if err := Reconcile(""); err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(archive), wt.UPID()+" ") {
		t.Fatalf("archive = %q, want %q", archive, wt.UPID())
	}
}

func TestChangeUPIDStartTimeArchivesTerminalTask(t *testing.T) {
	setupTaskDirs(t)

	wt, err := NewWorkerTask("pbsplus", "rename", "task")
	if err != nil {
		t.Fatal(err)
	}
	wt.CloseOK()

	start := time.Unix(wt.Task.StartTime+1, 0)
	newUPID, err := ChangeUPIDStartTime(wt.UPID(), start)
	if err != nil {
		t.Fatal(err)
	}
	if newUPID == wt.UPID() {
		t.Fatal("ChangeUPIDStartTime did not change the UPID")
	}
	active, err := readTaskFile(activeTasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active = %#v, want no tasks", active)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(archive), wt.UPID()+" ") || !strings.Contains(string(archive), newUPID+" ") {
		t.Fatalf("archive = %q, want only %q", archive, newUPID)
	}
}

func TestControlSocketPBSProtocol(t *testing.T) {
	setupTaskDirs(t)

	wt, err := NewWorkerTask("pbsplus", "ctrl", "c")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("unix", controlSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Fatal(cerr)
		}
	}()

	send := func(line string) string {
		t.Helper()
		if _, err := conn.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
		rd := bufio.NewReader(conn)
		reply, err := rd.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(reply)
	}

	reply := send(`{"command":"worker-task-status","args":{"upid":"` + wt.UPID() + `"}}`)
	if reply != "OK: true" {
		t.Fatalf("status reply = %q, want OK: true", reply)
	}

	reply = send(`{"command":"worker-task-abort","args":{"upid":"` + wt.UPID() + `"}}`)
	if reply != "OK: null" {
		t.Fatalf("abort reply = %q, want OK: null", reply)
	}
	if !wt.AbortRequested() {
		t.Fatal("abort command did not set abort flag")
	}

	foreign := NewTask("pbsplus", "other", "o")
	foreign.PID = os.Getpid() + 1
	reply = send(`{"command":"worker-task-status","args":{"upid":"` + foreign.GenerateUPID() + `"}}`)
	if !strings.HasPrefix(reply, "ERROR:") {
		t.Fatalf("foreign upid reply = %q, want ERROR", reply)
	}
}
