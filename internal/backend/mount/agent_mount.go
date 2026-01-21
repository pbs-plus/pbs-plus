//go:build linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	rpcmount "github.com/pbs-plus/pbs-plus/internal/backend/rpc"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type AgentMount struct {
	BackupId string
	Hostname string
	Drive    string
	Path     string
	isEmpty  bool
}

func AgentFSMount(ctx context.Context, storeInstance *store.Store, backup database.Backup, target database.Target) (*AgentMount, error) {
	// Parse target information
	agentMount := &AgentMount{
		BackupId: backup.ID,
		Hostname: target.GetHostname(),
		Drive:    target.VolumeID,
	}

	// Setup mount path
	agentMount.Path = filepath.Join(constants.AgentMountBasePath, backup.ID)
	agentMount.Unmount() // Ensure clean mount point

	// Create mount directory if it doesn't exist
	err := os.MkdirAll(agentMount.Path, 0700)
	if err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	// Try mounting with retries
	const maxRetries = 3
	const retryDelay = 2 * time.Second

	errCleanup := func() {
		agentMount.CloseMount()
		agentMount.Unmount()
	}

	args := &rpcmount.BackupArgs{
		BackupId:       backup.ID,
		TargetHostname: target.GetHostname(),
		Drive:          target.VolumeID,
	}
	var reply rpcmount.BackupReply

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", constants.MountSocketPath)
	if err != nil {
		errCleanup()
		return nil, fmt.Errorf("failed to reach backup RPC: %w", err)
	} else {
		rpcClient := rpc.NewClient(conn)
		err = rpcClient.Call("MountRPCService.Backup", args, &reply)
		rpcClient.Close()
		if err != nil {
			errCleanup()
			return nil, err
		}
		if reply.Status != 200 {
			errCleanup()
			if reply.Message == "" || reply.Status == 0 {
				return nil, fmt.Errorf("server rpc did not respond on time")
			}
			return nil, errors.New(reply.Message)
		}
	}

	isAccessible := false
	checkTimeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

checkLoop:
	for {
		select {
		case <-ctx.Done():
			errCleanup()
			return nil, ctx.Err()
		case <-checkTimeout:
			break checkLoop
		case <-ticker.C:
			if entries, err := os.ReadDir(agentMount.Path); err == nil {
				isAccessible = true
				agentMount.isEmpty = len(entries) == 0
				break checkLoop
			}
		}
	}
	if !isAccessible {
		errCleanup()
		return nil, fmt.Errorf("mounted directory not accessible after timeout")
	}
	return agentMount, nil
}

func (a *AgentMount) IsEmpty() bool {
	return a.isEmpty
}

func (a *AgentMount) IsConnected() bool {
	args := &rpcmount.StatusArgs{
		BackupId:       a.BackupId,
		TargetHostname: a.Hostname,
	}
	var reply rpcmount.StatusReply

	conn, err := net.DialTimeout("unix", constants.MountSocketPath, 5*time.Minute)
	if err != nil {
		return false
	}
	rpcClient := rpc.NewClient(conn)
	err = rpcClient.Call("MountRPCService.Status", args, &reply)
	rpcClient.Close()
	if err != nil {
		return false
	}

	return reply.Connected
}

func (a *AgentMount) Unmount() {
	if a.Path == "" {
		return
	}

	umount := exec.Command("fusermount", "-uz", a.Path)
	umount.Env = os.Environ()
	err := umount.Run()
	if err != nil {
		umount = exec.Command("umount", "-lf", a.Path)
		umount.Env = os.Environ()
		err = umount.Run()
		if err != nil {
			return
		}
	}

	_ = os.RemoveAll(a.Path)
}

func (a *AgentMount) CloseMount() {
	args := &rpcmount.CleanupArgs{
		BackupId:       a.BackupId,
		TargetHostname: a.Hostname,
		Drive:          a.Drive,
	}
	var reply rpcmount.CleanupReply

	conn, err := net.DialTimeout("unix", constants.MountSocketPath, 1*time.Minute)
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
