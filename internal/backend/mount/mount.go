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

	rpcmount "github.com/sonroyaalmerol/pbs-plus/internal/proxy/rpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type AgentMount struct {
	JobId    string
	Hostname string
	Drive    string
	Path     string
}

func Mount(storeInstance *store.Store, job *types.Job, target *types.Target) (*AgentMount, error) {
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
	// Create mount directory if it doesn't exist
	err := os.MkdirAll(agentMount.Path, 0700)
	if err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	agentMount.Unmount() // Ensure clean mount point

	// Try mounting with retries
	const maxRetries = 3
	const retryDelay = 2 * time.Second

	var lastErr error

	args := &rpcmount.BackupArgs{
		JobId:          job.ID,
		TargetHostname: targetHostname,
		Drive:          agentDrive,
		SourceMode:     job.SourceMode,
	}
	var reply rpcmount.BackupReply

	for i := 0; i < maxRetries; i++ {
		conn, err := net.DialTimeout("unix", constants.SocketPath, 5*time.Second)
		if err != nil {
			lastErr = fmt.Errorf("failed to dial RPC server: %w", err)
		} else {
			rpcClient := rpc.NewClient(conn)
			err = rpcClient.Call("MountRPCService.Backup", args, &reply)
			rpcClient.Close()
			if err == nil && reply.Status == 200 {
				break
			}
			lastErr = fmt.Errorf("RPC Backup call failed: %w", err)
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	if lastErr != nil {
		agentMount.CloseMount()
		agentMount.Unmount()
		return nil, fmt.Errorf("Mount: error mounting FUSE mount after %d attempts -> %w", maxRetries, lastErr)
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
		agentMount.Unmount()
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: mounted directory not accessible after timeout")
	}
	return agentMount, nil
}

func (a *AgentMount) Unmount() {
	if a.Path == "" {
		return
	}

	// First try a clean unmount
	umount := exec.Command("umount", "-lf", a.Path)
	umount.Env = os.Environ()
	err := umount.Run()
	if err == nil {
		_ = os.RemoveAll(a.Path)
	}
}

func (a *AgentMount) CloseMount() {
	args := &rpcmount.CleanupArgs{
		JobId:          a.JobId,
		TargetHostname: utils.EncodePath(a.Hostname),
		Drive:          a.Drive,
	}
	var reply rpcmount.CleanupReply

	conn, err := net.DialTimeout("unix", constants.SocketPath, 5*time.Second)
	if err != nil {
		return
	}
	rpcClient := rpc.NewClient(conn)
	defer rpcClient.Close()

	if err := rpcClient.Call("MountRPCService.Cleanup", args, &reply); err != nil {
		syslog.L.Error(err).WithFields(map[string]interface{}{"hostname": a.Hostname, "drive": a.Drive}).Write()
	}
}
