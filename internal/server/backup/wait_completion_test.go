//go:build linux

package backup

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func TestWaitForCompletionReturnsWhenClientExits(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	b := &backupJob{logger: log.WithScope(log.Scope{JobID: "test"})}
	done := make(chan error, 1)
	go func() {
		done <- b.waitForCompletion(context.Background(), cmd, "missing-task")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForCompletion: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitForCompletion did not return after client exited")
	}
}

func TestWaitForCompletionKillsClientWhenTaskStops(t *testing.T) {
	restoreTask := tasklog.UseTaskDir(t.TempDir())
	defer restoreTask()

	queued, err := tasklog.NewQueuedTask("backup", "host/test/StoppedTask", false)
	if err != nil {
		t.Fatal(err)
	}
	queued.Close()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	b := &backupJob{logger: log.WithScope(log.Scope{JobID: "test"})}
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.waitForCompletion(context.Background(), cmd, queued.UPID())
	}()

	select {
	case err := <-errCh:
		if err == nil || err.Error() != jobs.ErrCanceled.Error() {
			t.Fatalf("want jobs.ErrCanceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("waitForCompletion did not return after task stopped")
	}

	if err := waitForExit(cmd); err == nil {
		t.Fatal("client process still running after task stopped")
	}
}

func waitForExit(cmd *exec.Cmd) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return nil
	}
}
