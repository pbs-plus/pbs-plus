//go:build linux

package snapshotmount

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/systemd"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

const mountSettleAttempts = 30

func openTask(upid, workerType, wid string) (*tasklog.WorkerTask, error) {
	if upid != "" {
		return tasklog.ReopenWorkerTask(upid)
	}
	return tasklog.NewWorkerTask("pbsplus", workerType, wid)
}

func Register(engine *jobs.Engine) error {
	if err := engine.RegisterVersion(jobs.WorkflowSnapshotMount, "1", func(w *jobs.WorkflowContext) error {
		var in jobs.SnapshotMountInput
		if err := json.Unmarshal(w.Execution.Payload, &in); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding mount workflow input: %w", err))
		}
		return w.Step("mount", func(ctx context.Context) error {
			return runMount(ctx, in)
		})
	}); err != nil {
		return fmt.Errorf("registering snapshot mount workflow: %w", err)
	}
	if err := engine.RegisterVersion(jobs.WorkflowSnapshotUnmount, "1", func(w *jobs.WorkflowContext) error {
		var in jobs.SnapshotUnmountInput
		if err := json.Unmarshal(w.Execution.Payload, &in); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding unmount workflow input: %w", err))
		}
		return w.Step("unmount", func(ctx context.Context) error {
			return runUnmount(ctx, in)
		})
	}); err != nil {
		return fmt.Errorf("registering snapshot unmount workflow: %w", err)
	}
	if err := engine.RegisterVersion(jobs.WorkflowSnapshotCommit, "1", func(w *jobs.WorkflowContext) error {
		var in jobs.SnapshotCommitInput
		if err := json.Unmarshal(w.Execution.Payload, &in); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding commit workflow input: %w", err))
		}
		return w.Step("commit", func(ctx context.Context) error {
			return runCommit(ctx, in)
		})
	}); err != nil {
		return fmt.Errorf("registering snapshot commit workflow: %w", err)
	}
	if err := engine.RegisterVersion(jobs.WorkflowSnapshotInit, "1", func(w *jobs.WorkflowContext) error {
		var in jobs.SnapshotInitInput
		if err := json.Unmarshal(w.Execution.Payload, &in); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding init workflow input: %w", err))
		}
		return w.Step("init", func(ctx context.Context) error {
			return runInit(ctx, in)
		})
	}); err != nil {
		return fmt.Errorf("registering snapshot init workflow: %w", err)
	}
	if err := engine.RegisterVersion(jobs.WorkflowSnapshotCompose, "1", func(w *jobs.WorkflowContext) error {
		var in jobs.SnapshotComposeInput
		if err := json.Unmarshal(w.Execution.Payload, &in); err != nil {
			return jobs.NonRetryable(fmt.Errorf("decoding compose workflow input: %w", err))
		}
		return w.Step("compose", func(ctx context.Context) error {
			return runCompose(ctx, in)
		})
	}); err != nil {
		return fmt.Errorf("registering snapshot compose workflow: %w", err)
	}
	return nil
}

func runInit(ctx context.Context, in jobs.SnapshotInitInput) error {
	key := Key(in.Datastore, in.Namespace, in.BackupType, in.BackupID, "init")

	task, err := openTask(in.UPID, "init", tasklog.FormatWorkerID(in.Datastore, "init-", key))
	if err != nil {
		return jobs.NonRetryable(err)
	}

	runErr := func() error {
		session, err := initSession(ctx, task, in, key)
		if err != nil {
			return err
		}
		task.LogString(fmt.Sprintf("initialized %s/%s at %s (rw)", in.BackupType, in.BackupID, session.MountPoint))
		return nil
	}()
	if runErr != nil {
		task.CloseErr(runErr)
		if errors.Is(runErr, context.Canceled) {
			return runErr
		}
		return jobs.NonRetryable(runErr)
	}
	task.CloseOK()
	return nil
}

func initSession(ctx context.Context, task *tasklog.WorkerTask, in jobs.SnapshotInitInput, key string) (Session, error) {
	if err := validate.ValidateDatastore(in.Datastore); err != nil {
		return Session{}, fmt.Errorf("invalid datastore: %w", err)
	}
	if err := validate.ValidateNamespace(in.Namespace); err != nil {
		return Session{}, err
	}
	if err := validate.ValidateBackupType(in.BackupType); err != nil {
		return Session{}, err
	}
	if err := validate.ValidateBackupID(in.BackupID); err != nil {
		return Session{}, err
	}
	if err := ValidateMountPath(in.MountPath); err != nil {
		return Session{}, err
	}

	if existing, err := LoadSession(key); err == nil && IsMounted(existing.MountPoint) {
		return Session{}, fmt.Errorf("group %s/%s already has an active init mount at %s", in.BackupType, in.BackupID, existing.MountPoint)
	}

	dsInfo, err := cli.GetDatastoreInfo(in.Datastore)
	if err != nil {
		return Session{}, err
	}
	pbsStoreRoot := dsInfo.Path
	if pbsStoreRoot == "" {
		return Session{}, errors.New("invalid datastore configuration")
	}

	mountPoint := in.MountPath
	managed := false
	if mountPoint == "" {
		mountPoint = DefaultMountPoint(in.Datastore, in.Namespace, in.BackupType, in.BackupID, time.Time{})
		managed = true
		if err := validatePath(mountPoint, conf.RestoreMountBasePath); err != nil {
			return Session{}, err
		}
	} else if entries, err := os.ReadDir(mountPoint); err == nil && len(entries) > 0 {
		return Session{}, fmt.Errorf("mount path %s exists and is not empty", mountPoint)
	}

	session := Session{
		Datastore:  in.Datastore,
		Namespace:  in.Namespace,
		BackupType: in.BackupType,
		BackupID:   in.BackupID,
		Mode:       ModeRW,
		MountPoint: mountPoint,
		OverlayDir: OverlayDir(key),
		SocketPath: SocketPath(key),
		ServiceKey: key,
		CreatedAt:  time.Now().Unix(),
	}

	task.LogString(fmt.Sprintf("initializing %s/%s/%s at %s", in.Datastore, in.Namespace, in.BackupID, mountPoint))

	if err := os.MkdirAll(session.OverlayDir, 0o700); err != nil {
		return Session{}, fmt.Errorf("create overlay dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(session.SocketPath), 0o755); err != nil {
		return Session{}, fmt.Errorf("create socket dir: %w", err)
	}

	ns := in.Namespace
	if ns == "" {
		ns = "-"
	}
	args := []string{
		"init",
		"--pbs-store", pbsStoreRoot,
		"--passthrough", session.OverlayDir,
		"--socket", session.SocketPath,
		"--namespace", ns,
		"--options", "rw,allow_other,default_permissions",
		mountPoint,
	}
	return startSession(ctx, session, mountPoint, managed, args)
}

func runMount(ctx context.Context, in jobs.SnapshotMountInput) error {
	parsedTime, err := time.Parse(time.RFC3339, in.BackupTime)
	if err != nil {
		return jobs.NonRetryable(fmt.Errorf("invalid backup-time format: %w", err))
	}
	safeTime := DirTime(parsedTime)
	key := Key(in.Datastore, in.Namespace, in.BackupType, in.BackupID, safeTime)

	task, err := openTask(in.UPID, "mount", tasklog.FormatWorkerID(in.Datastore, "mount-", key))
	if err != nil {
		return jobs.NonRetryable(err)
	}

	runErr := func() error {
		session, err := mountSession(ctx, task, in, parsedTime, key)
		if err != nil {
			return err
		}
		task.LogString(fmt.Sprintf("mounted %s/%s at %s (%s)", in.BackupType, in.BackupID, session.MountPoint, session.Mode))
		return nil
	}()
	if runErr != nil {
		task.CloseErr(runErr)
		if errors.Is(runErr, context.Canceled) {
			return runErr
		}
		return jobs.NonRetryable(runErr)
	}
	task.CloseOK()
	return nil
}

func mountSession(ctx context.Context, task *tasklog.WorkerTask, in jobs.SnapshotMountInput, parsedTime time.Time, key string) (Session, error) {
	mode := in.Mode
	if mode == "" {
		mode = ModeRO
	}
	if mode != ModeRO && mode != ModeRW {
		return Session{}, fmt.Errorf("invalid mode %q", mode)
	}
	if err := ValidateMountPath(in.MountPath); err != nil {
		return Session{}, err
	}

	dsInfo, err := cli.GetDatastoreInfo(in.Datastore)
	if err != nil {
		return Session{}, err
	}
	pbsStoreRoot := dsInfo.Path
	if pbsStoreRoot == "" {
		return Session{}, errors.New("invalid datastore configuration")
	}

	mountPoint := in.MountPath
	managed := false
	if mountPoint == "" {
		mountPoint = DefaultMountPoint(in.Datastore, in.Namespace, in.BackupType, in.BackupID, parsedTime)
		managed = true
		if err := validatePath(mountPoint, conf.RestoreMountBasePath); err != nil {
			return Session{}, err
		}
	} else if entries, err := os.ReadDir(mountPoint); err == nil && len(entries) > 0 {
		return Session{}, fmt.Errorf("mount path %s exists and is not empty", mountPoint)
	}

	session := Session{
		Datastore:  in.Datastore,
		Namespace:  in.Namespace,
		BackupType: in.BackupType,
		BackupID:   in.BackupID,
		BackupTime: in.BackupTime,
		FileName:   in.FileName,
		Mode:       mode,
		MountPoint: mountPoint,
		ServiceKey: key,
		CreatedAt:  time.Now().Unix(),
	}

	task.LogString(fmt.Sprintf("mounting %s/%s/%s %s (%s) at %s",
		in.Datastore, in.Namespace, in.BackupType, in.BackupID, in.FileName, mountPoint))

	dirTime := DirTime(parsedTime)
	mpxarPath, ppxarPath, isMetadataSplit, err := proxmox.BuildPxarPaths(pbsStoreRoot, in.Namespace, in.BackupType, in.BackupID, dirTime, in.FileName)
	if err != nil {
		return Session{}, err
	}

	args := []string{"--pbs-store", pbsStoreRoot}
	if isMetadataSplit {
		args = append(args, "--mpxar-didx", mpxarPath, "--ppxar-didx", ppxarPath)
	} else {
		args = append(args, "--ppxar-didx", ppxarPath)
	}
	if mode == ModeRW {
		session.OverlayDir = OverlayDir(key)
		session.SocketPath = SocketPath(key)
		if err := os.MkdirAll(session.OverlayDir, 0o700); err != nil {
			return Session{}, fmt.Errorf("create overlay dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(session.SocketPath), 0o755); err != nil {
			return Session{}, fmt.Errorf("create socket dir: %w", err)
		}
		args = append(args,
			"--passthrough", session.OverlayDir,
			"--socket", session.SocketPath,
			"--options", "rw,allow_other,default_permissions",
		)
	}
	args = append(args, mountPoint)

	return startSession(ctx, session, mountPoint, managed, args)
}

func startSession(ctx context.Context, session Session, mountPoint string, managed bool, args []string) (Session, error) {
	serviceName := session.ServiceName()
	if err := systemd.StopMountService(ctx, serviceName); err != nil {
		log.Error(err, "")
	}
	if IsMounted(mountPoint) {
		if err := UnmountPath(mountPoint); err != nil {
			log.Error(err, "")
		}
	}
	if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return Session{}, fmt.Errorf("failed to create mount-point: %w", err)
	}

	if err := systemd.CreateMountService(ctx, serviceName, mountPoint, args); err != nil {
		cleanupMountPoint(mountPoint, managed)
		return Session{}, fmt.Errorf("start mount service: %w", err)
	}

	for range mountSettleAttempts {
		if IsMounted(mountPoint) {
			if err := SaveSession(session); err != nil {
				return Session{}, fmt.Errorf("persist mount session: %w", err)
			}
			return session, nil
		}
		select {
		case <-ctx.Done():
			_ = systemd.StopMountService(context.WithoutCancel(ctx), serviceName)
			cleanupMountPoint(mountPoint, managed)
			return Session{}, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	_ = systemd.StopMountService(context.WithoutCancel(ctx), serviceName)
	cleanupMountPoint(mountPoint, managed)
	return Session{}, errors.New("mount failed")
}

func validatePath(mountPoint, base string) error {
	clean := filepath.Clean(mountPoint)
	if clean == base {
		return fmt.Errorf("mount point cannot be the base path itself")
	}
	return nil
}

func cleanupMountPoint(mountPoint string, managed bool) {
	if managed {
		if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	}
}

func runUnmount(ctx context.Context, in jobs.SnapshotUnmountInput) error {
	session, key, err := resolveSession(in)
	if err != nil {
		return jobs.NonRetryable(err)
	}

	task, err := openTask(in.UPID, "unmount", tasklog.FormatWorkerID(in.Datastore, "unmount-", key))
	if err != nil {
		return jobs.NonRetryable(err)
	}

	runErr := unmountSession(ctx, task, session, in)
	if runErr != nil {
		task.CloseErr(runErr)
		if errors.Is(runErr, context.Canceled) {
			return runErr
		}
		return jobs.NonRetryable(runErr)
	}
	task.LogString("unmounted " + session.MountPoint)
	task.CloseOK()
	return nil
}

func resolveSession(in jobs.SnapshotUnmountInput) (Session, string, error) {
	if in.MountPath != "" {
		session, found, err := FindSessionByMountPoint(in.MountPath)
		if err != nil {
			return Session{}, "", err
		}
		if found {
			return session, session.ServiceKey, nil
		}
		return Session{MountPoint: in.MountPath}, "", nil
	}

	parsedTime, err := time.Parse(time.RFC3339, in.BackupTime)
	if err != nil {
		return Session{}, "", fmt.Errorf("invalid backup-time format: %w", err)
	}
	key := Key(in.Datastore, in.Namespace, in.BackupType, in.BackupID, DirTime(parsedTime))
	session, err := LoadSession(key)
	if err != nil {
		if os.IsNotExist(err) {
			return Session{
				Datastore:  in.Datastore,
				Namespace:  in.Namespace,
				BackupType: in.BackupType,
				BackupID:   in.BackupID,
				BackupTime: in.BackupTime,
				FileName:   in.FileName,
				Mode:       ModeRO,
				MountPoint: DefaultMountPoint(in.Datastore, in.Namespace, in.BackupType, in.BackupID, parsedTime),
				ServiceKey: key,
			}, key, nil
		}
		return Session{}, "", err
	}
	return session, key, nil
}

func unmountSession(ctx context.Context, task *tasklog.WorkerTask, session Session, in jobs.SnapshotUnmountInput) error {
	if session.Mode == ModeRW && !in.Force {
		return fmt.Errorf("read-write mount %s may have uncommitted changes; retry with force to discard them", session.MountPoint)
	}

	task.LogString("unmounting " + session.MountPoint)

	if session.ServiceKey != "" {
		if err := systemd.StopMountService(ctx, session.ServiceName()); err != nil {
			log.Error(err, "")
		}
	}
	if IsMounted(session.MountPoint) {
		if err := UnmountPath(session.MountPoint); err != nil {
			log.Error(err, "")
		}
	}
	if err := os.RemoveAll(session.MountPoint); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	RemoveEmptyDirsToBase(filepath.Dir(session.MountPoint), filepath.Join(conf.RestoreMountBasePath, session.Datastore))
	if session.ServiceKey != "" {
		cleanupSessionFiles(session)
	}
	return nil
}

func runCommit(ctx context.Context, in jobs.SnapshotCommitInput) error {
	session, found, err := FindSessionByMountPoint(in.MountPath)
	if err != nil {
		return jobs.NonRetryable(err)
	}
	if !found {
		return jobs.NonRetryable(fmt.Errorf("no mount session at %s", in.MountPath))
	}
	if !session.CommitCapable() {
		return jobs.NonRetryable(fmt.Errorf("mount at %s is not commit-capable (read-only or offline)", in.MountPath))
	}

	task, err := openTask(in.UPID, "commit", tasklog.FormatWorkerID(session.Datastore, "commit-", session.ServiceKey))
	if err != nil {
		return jobs.NonRetryable(err)
	}

	runErr := commitSession(ctx, task, session)
	if runErr != nil {
		task.CloseErr(runErr)
		if errors.Is(runErr, context.Canceled) {
			return runErr
		}
		return jobs.NonRetryable(runErr)
	}
	task.LogString(fmt.Sprintf("committed %s/%s", session.BackupType, session.BackupID))
	task.CloseOK()
	return nil
}

func commitSession(ctx context.Context, task *tasklog.WorkerTask, session Session) error {
	conn, err := net.DialTimeout("unix", session.SocketPath, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect mount control socket: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	ns := session.Namespace
	if ns == "" {
		ns = "-"
	}
	cmd := fmt.Sprintf("COMMIT %s %s %s %s %s %s\n", "", session.Datastore, "", ns, session.BackupType, session.BackupID)
	if _, err := fmt.Fprint(conn, cmd); err != nil {
		return fmt.Errorf("send commit command: %w", err)
	}
	if _, err := fmt.Fprintln(conn, "DETACH"); err != nil {
		return fmt.Errorf("send detach: %w", err)
	}

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return errors.New("no response from mount daemon")
	}
	resp := scanner.Text()
	if after, ok := strings.CutPrefix(resp, "ERR "); ok {
		return errors.New(after)
	}
	if !strings.HasPrefix(resp, "JOB ") {
		return fmt.Errorf("unexpected commit response: %s", resp)
	}
	if err := conn.Close(); err != nil {
		log.Error(err, "")
	}

	task.LogString("commit started, streaming progress")
	return streamCommitProgress(ctx, task, session)
}

// streamCommitProgress mirrors monitor lines into the task log; cancelling only abandons streaming, the mount-side commit is atomic.
func streamCommitProgress(ctx context.Context, task *tasklog.WorkerTask, session Session) error {
	monitorPath := session.SocketPath + ".monitor"

	watcherDone := make(chan error, 1)
	go func() {
		watcherDone <- watchMonitor(monitorPath, task)
	}()

	select {
	case err := <-watcherDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func watchMonitor(monitorPath string, task *tasklog.WorkerTask) error {
	for range 5 {
		conn, err := net.DialTimeout("unix", monitorPath, 5*time.Second)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		result := drainMonitor(conn, task)
		if err := conn.Close(); err != nil {
			log.Error(err, "")
		}
		if result != errMonitorIdle {
			return result
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.New("lost commit progress stream; check mount logs")
}

var errMonitorIdle = errors.New("monitor idle")

func drainMonitor(conn net.Conn, task *tasklog.WorkerTask) error {
	scanner := bufio.NewScanner(conn)
	first := true
	for scanner.Scan() {
		line := scanner.Text()
		if first {
			first = false
			if line == "IDLE" {
				return errMonitorIdle
			}
		}
		if after, ok := strings.CutPrefix(line, "OK "); ok {
			task.LogString(after)
			return nil
		}
		if after, ok := strings.CutPrefix(line, "ERR "); ok {
			task.LogString(after)
			return errors.New(after)
		}
		task.LogString(line)
	}
	return errMonitorIdle
}
