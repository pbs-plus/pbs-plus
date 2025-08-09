//go:build linux

package mount

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	rpcmount "github.com/pbs-plus/pbs-plus/internal/proxy/rpc"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type AgentMount struct {
	JobId    string
	Hostname string
	Drive    string
	Path     string
}

func AgentFSMount(storeInstance *store.Store, job types.Job, target types.Target) (*AgentMount, error) {
	// Parse target information
	splittedTargetName := strings.Split(target.Name, " - ")
	targetHostname := splittedTargetName[0]
	agentPath := strings.TrimPrefix(target.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
	agentDrive := agentPathParts[1]

	agentMount := &AgentMount{
		JobId:    job.ID,
		Hostname: targetHostname,
		Drive:    agentDrive,
	}

	// Setup mount path
	agentMount.Path = filepath.Join(constants.AgentMountBasePath, job.ID)
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
		JobId:          job.ID,
		TargetHostname: targetHostname,
		Drive:          agentDrive,
	}
	var reply rpcmount.BackupReply

	conn, err := net.DialTimeout("unix", constants.MountSocketPath, 5*time.Minute)
	if err != nil {
		errCleanup()
		return nil, fmt.Errorf("failed to reach backup RPC: %w", err)
	} else {
		rpcClient := rpc.NewClient(conn)
		err = rpcClient.Call("MountRPCService.Backup", args, &reply)
		rpcClient.Close()
		if err != nil {
			errCleanup()
			return nil, fmt.Errorf("backup failed: %w", err)
		}
		if reply.Status != 200 {
			errCleanup()
			return nil, fmt.Errorf("backup returned an error %d: %s", reply.Status, reply.Message)
		}
	}

	isAccessible := false
	checkTimeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

checkLoop:
	for {
		select {
		case <-checkTimeout:
			break checkLoop
		case <-ticker.C:
			if _, err := os.ReadDir(agentMount.Path); err == nil {
				isAccessible = true
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

func (a *AgentMount) IsConnected() bool {
	args := &rpcmount.StatusArgs{
		JobId:          a.JobId,
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
		JobId:          a.JobId,
		TargetHostname: a.Hostname,
		Drive:          a.Drive,
	}
	var reply rpcmount.CleanupReply

	conn, err := net.DialTimeout("unix", constants.MountSocketPath, 5*time.Minute)
	if err != nil {
		return
	}
	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()

	if err := rpcClient.Call("MountRPCService.Cleanup", args, &reply); err != nil {
		syslog.L.Error(err).WithFields(map[string]interface{}{"hostname": a.Hostname, "drive": a.Drive}).Write()
	}
}
