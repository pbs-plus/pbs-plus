//go:build linux

package rpc

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// unmountPath force-unmounts path via fusermount (falling back to umount) and
// removes the mount directory. It is a no-op when path is empty.
func unmountPath(path string) {
	if path == "" {
		return
	}
	umount := exec.Command("fusermount", "-uz", path)
	umount.Env = os.Environ()
	if err := umount.Run(); err != nil {
		umount = exec.Command("umount", "-lf", path)
		umount.Env = os.Environ()
		if err := umount.Run(); err != nil {
			return
		}
	}
	_ = os.RemoveAll(path)
}

// waitForAccessible polls path until it is readable or 30s elapse. It returns
// whether the mount became accessible and, if so, whether it is empty.
func waitForAccessible(ctx context.Context, path string) (isEmpty, ok bool) {
	checkTimeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false, false
		case <-checkTimeout:
			return false, false
		case <-ticker.C:
			if entries, err := os.ReadDir(path); err == nil {
				return len(entries) == 0, true
			}
		}
	}
}

// callMountRPC dials the mount unix socket and invokes serviceMethod. Dial
// failures are wrapped; call errors are returned unwrapped so callers can
// branch on them.
func callMountRPC(ctx context.Context, serviceMethod string, args, reply any) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", conf.MountSocketPath)
	if err != nil {
		return fmt.Errorf("failed to reach backup RPC: %w", err)
	}
	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()
	return rpcClient.Call(serviceMethod, args, reply)
}

// --- Agent (ARPC) mount client ---

// AgentMount is the client-side handle for an agent FS mount created via the
// mount RPC service. It owns the local mount path lifecycle.
type AgentMount struct {
	BackupID string
	Hostname string
	Drive    string
	Path     string
	isEmpty  bool
}

// AgentFSMount asks the mount RPC service to back up + mount the agent target,
// then waits for the mount to become accessible.
func AgentFSMount(ctx context.Context, storeInstance *store.Store, backup database.Backup, target database.Target) (*AgentMount, error) {
	agentMount := &AgentMount{
		BackupID: backup.ID,
		Hostname: target.GetHostname(),
		Drive:    target.VolumeID,
	}

	agentMount.Path = filepath.Join(conf.AgentMountBasePath, backup.ID)
	agentMount.Unmount() // Ensure clean mount point

	if err := os.MkdirAll(agentMount.Path, 0700); err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	errCleanup := func() {
		agentMount.CloseMount()
		agentMount.Unmount()
	}

	args := &BackupArgs{
		BackupID:       backup.ID,
		TargetHostname: target.GetHostname(),
		Drive:          target.VolumeID,
	}
	var reply BackupReply

	if err := callMountRPC(ctx, "MountRPCService.Backup", args, &reply); err != nil {
		errCleanup()
		return nil, err
	}
	if reply.Status != 200 {
		errCleanup()
		if reply.Message == "" || reply.Status == 0 {
			return nil, fmt.Errorf("server rpc did not respond on time")
		}
		return nil, fmt.Errorf("%s", reply.Message)
	}

	empty, ok := waitForAccessible(ctx, agentMount.Path)
	if !ok {
		errCleanup()
		return nil, fmt.Errorf("mounted directory not accessible after timeout")
	}
	agentMount.isEmpty = empty
	return agentMount, nil
}

func (a *AgentMount) IsEmpty() bool { return a.isEmpty }

func (a *AgentMount) IsConnected() bool {
	args := &StatusArgs{
		BackupID:       a.BackupID,
		TargetHostname: a.Hostname,
	}
	var reply StatusReply

	conn, err := net.DialTimeout("unix", conf.MountSocketPath, 5*time.Minute)
	if err != nil {
		return false
	}
	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()
	if err := rpcClient.Call("MountRPCService.Status", args, &reply); err != nil {
		return false
	}
	return reply.Connected
}

func (a *AgentMount) Unmount() { unmountPath(a.Path) }

func (a *AgentMount) CloseMount() {
	args := &CleanupArgs{
		BackupID:       a.BackupID,
		TargetHostname: a.Hostname,
		Drive:          a.Drive,
	}
	var reply CleanupReply

	conn, err := net.DialTimeout("unix", conf.MountSocketPath, 1*time.Minute)
	if err != nil {
		return
	}
	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()

	if err := rpcClient.Call("MountRPCService.ARPCCleanup", args, &reply); err != nil {
		syslog.L.Error(err).WithFields(map[string]any{"hostname": a.Hostname, "drive": a.Drive}).WithMessage(reply.Message).Write()
	}

	syslog.L.Info().WithFields(map[string]any{"hostname": a.Hostname, "drive": a.Drive}).WithMessage(reply.Message).Write()
}

// --- S3 mount client ---

// S3Mount is the client-side handle for an S3 FS mount created via the mount
// RPC service. It owns the local mount path lifecycle.
type S3Mount struct {
	BackupID  string
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
	Prefix    string
	UseSSL    bool
	Path      string
	isEmpty   bool
}

// S3FSMount asks the mount RPC service to mount the S3 target, then waits for
// the mount to become accessible.
func S3FSMount(ctx context.Context, storeInstance *store.Store, backup database.Backup, target database.Target) (*S3Mount, error) {
	parsedS3 := target.S3Info

	s3Mount := &S3Mount{
		BackupID:  backup.ID,
		Endpoint:  parsedS3.Endpoint,
		AccessKey: parsedS3.AccessKey,
		Bucket:    parsedS3.Bucket,
		Region:    parsedS3.Region,
		UseSSL:    parsedS3.UseSSL,
		Prefix:    backup.Subpath,
	}

	s3Mount.Path = filepath.Join(conf.AgentMountBasePath, backup.ID)
	s3Mount.Unmount() // Ensure clean mount point

	if err := os.MkdirAll(s3Mount.Path, 0700); err != nil {
		s3Mount.CloseMount()
		return nil, fmt.Errorf("error creating directory \"%s\" -> %w", s3Mount.Path, err)
	}

	errCleanup := func() {
		s3Mount.CloseMount()
		s3Mount.Unmount()
	}

	args := &S3BackupArgs{
		BackupID:     backup.ID,
		Endpoint:     parsedS3.Endpoint,
		AccessKey:    parsedS3.AccessKey,
		Bucket:       parsedS3.Bucket,
		Region:       parsedS3.Region,
		UseSSL:       parsedS3.UseSSL,
		UsePathStyle: parsedS3.IsPathStyle,
		Prefix:       backup.Subpath,
	}
	var reply BackupReply

	if err := callMountRPC(ctx, "MountRPCService.S3Backup", args, &reply); err != nil {
		errCleanup()
		return nil, err
	}
	if reply.Status != 200 {
		errCleanup()
		return nil, fmt.Errorf("%s", reply.Message)
	}

	empty, ok := waitForAccessible(ctx, s3Mount.Path)
	if !ok {
		errCleanup()
		return nil, fmt.Errorf("mounted directory not accessible after timeout")
	}
	s3Mount.isEmpty = empty
	return s3Mount, nil
}

func (a *S3Mount) Unmount() { unmountPath(a.Path) }

func (a *S3Mount) IsEmpty() bool { return a.isEmpty }

func (a *S3Mount) CloseMount() {
	sessions.DisconnectSession(a.Endpoint + "|" + a.BackupID)
}
