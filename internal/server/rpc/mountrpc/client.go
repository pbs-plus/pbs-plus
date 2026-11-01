//go:build linux

package mountrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func waitForAccessible(ctx context.Context, path string) (isEmpty, ok bool) {
	checkTimeout := time.NewTimer(30 * time.Second)
	defer checkTimeout.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, false
		case <-checkTimeout.C:
			return false, false
		case <-ticker.C:
			if entries, err := os.ReadDir(path); err == nil {
				return len(entries) == 0, true
			}
		}
	}
}

func callMountRPC(ctx context.Context, serviceMethod string, args, reply any) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", conf.MountSocketPath)
	if err != nil {
		return fmt.Errorf("failed to reach backup RPC: %w", err)
	}
	return callMountRPCConn(ctx, conn, serviceMethod, args, reply)
}

func callMountRPCConn(ctx context.Context, conn net.Conn, serviceMethod string, args, reply any) error {
	rpcClient := rpc.NewClient(conn)
	stop := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error(err, "failed to close canceled mount RPC")
		}
	})
	defer stop()
	defer func() {
		if err := rpcClient.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error(err, "failed to close mount RPC")
		}
	}()
	err := rpcClient.Call(serviceMethod, args, reply)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

type AgentMount struct {
	BackupID string
	Hostname string
	Drive    string
	Path     string
	MountID  string
	isEmpty  bool
}

func AgentFSMount(ctx context.Context, app *application.Runtime, backup coredb.Backup, target coredb.Target) (*AgentMount, error) {
	m := &AgentMount{
		BackupID: backup.ID, Hostname: target.GetHostname(), Drive: target.VolumeID,
		Path: filepath.Join(conf.AgentMountBasePath, backup.ID), MountID: uuid.NewString(),
	}
	args := &BackupArgs{BackupID: m.BackupID, TargetHostname: m.Hostname, Drive: m.Drive, MountID: m.MountID}
	var reply BackupReply
	if err := callMountRPC(ctx, ServiceName+".Backup", args, &reply); err != nil {
		m.CloseMount()
		return nil, err
	}
	if reply.Status != 200 {
		m.CloseMount()
		return nil, fmt.Errorf("mount rejected (%d): %s", reply.Status, reply.Message)
	}
	empty, ok := waitForAccessible(ctx, m.Path)
	if !ok {
		m.CloseMount()
		return nil, errors.New("mounted directory not accessible after timeout")
	}
	m.isEmpty = empty
	return m, nil
}

func (a *AgentMount) IsEmpty() bool { return a.isEmpty }

func (a *AgentMount) IsConnected() bool {
	connected, _ := a.Progress()
	return connected
}

// Progress pairs reachability with a served-operation count so a caller can spot a mount that is connected but no longer doing work.
func (a *AgentMount) Progress() (connected bool, activity int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	args := &StatusArgs{BackupID: a.BackupID, TargetHostname: a.Hostname, MountID: a.MountID}
	var reply StatusReply
	if err := callMountRPC(ctx, ServiceName+".Status", args, &reply); err != nil {
		return false, 0
	}
	return reply.Connected, reply.Activity
}

func (a *AgentMount) Unmount() { a.CloseMount() }

func closeMount(serviceMethod string, args *CleanupArgs) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	var reply CleanupReply
	if err := callMountRPC(ctx, serviceMethod, args, &reply); err != nil {
		log.Error(err, "mount cleanup failed, server retains ownership for retry")
	}
}

func (a *AgentMount) CloseMount() {
	closeMount(ServiceName+".ARPCCleanup", &CleanupArgs{
		BackupID: a.BackupID, TargetHostname: a.Hostname, Drive: a.Drive, MountID: a.MountID,
	})
}

type S3Mount struct {
	BackupID string
	Endpoint string
	Path     string
	MountID  string
	isEmpty  bool
}

func S3FSMount(ctx context.Context, app *application.Runtime, backup coredb.Backup, target coredb.Target) (*S3Mount, error) {
	info := target.S3Info
	m := &S3Mount{
		BackupID: backup.ID, Endpoint: info.Endpoint,
		Path: filepath.Join(conf.AgentMountBasePath, backup.ID), MountID: uuid.NewString(),
	}
	args := &S3BackupArgs{
		BackupID: backup.ID, Endpoint: info.Endpoint, AccessKey: info.AccessKey,
		Bucket: info.Bucket, Region: info.Region, UseSSL: info.UseSSL,
		UsePathStyle: info.IsPathStyle, Prefix: backup.Subpath, MountID: m.MountID,
	}
	var reply BackupReply
	if err := callMountRPC(ctx, ServiceName+".S3Backup", args, &reply); err != nil {
		m.CloseMount()
		return nil, err
	}
	if reply.Status != 200 {
		m.CloseMount()
		return nil, fmt.Errorf("mount rejected (%d): %s", reply.Status, reply.Message)
	}
	empty, ok := waitForAccessible(ctx, m.Path)
	if !ok {
		m.CloseMount()
		return nil, errors.New("mounted directory not accessible after timeout")
	}
	m.isEmpty = empty
	return m, nil
}

func (a *S3Mount) Unmount()      { a.CloseMount() }
func (a *S3Mount) IsEmpty() bool { return a.isEmpty }
func (a *S3Mount) CloseMount() {
	closeMount(ServiceName+".S3Cleanup", &CleanupArgs{
		BackupID: a.BackupID, TargetHostname: a.Endpoint, MountID: a.MountID,
	})
}
