//go:build linux

package mount

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

const (
	acknowledgementTimeout = 15 * time.Second
	defaultMountPerms    = 0700
)

type AgentMount struct {
	Hostname string
	Drive    string
	Path     string
	cmd      *exec.Cmd
	wsHub    *websockets.Server
	mutex    sync.Mutex
}

// Mount creates a new SFTP mount point for the specified target
func Mount(wsHub *websockets.Server, target *store.Target) (*AgentMount, error) {
	if err := validatePrerequisites(); err != nil {
		return nil, err
	}

	hostname, drive, err := parseTargetInfo(target)
	if err != nil {
		return nil, fmt.Errorf("failed to parse target info: %w", err)
	}

	agentMount := &AgentMount{
		wsHub:    wsHub,
		Hostname: hostname,
		Drive:    drive,
	}

	if err := agentMount.initializeConnection(target); err != nil {
		return nil, err
	}

	if err := agentMount.setupMountPoint(target); err != nil {
		agentMount.Cleanup()
		return nil, err
	}

	return agentMount, nil
}

func validatePrerequisites() error {
	if !utils.IsValid("/usr/bin/rclone") {
		return fmt.Errorf("rclone is missing! Please install rclone first before backing up from agent")
	}
	return nil
}

func parseTargetInfo(target *store.Target) (hostname string, drive string, err error) {
	splittedTargetName := strings.Split(target.Name, " - ")
	if len(splittedTargetName) == 0 {
		return "", "", fmt.Errorf("invalid target name format")
	}

	hostname = splittedTargetName[0]
	agentPath := strings.TrimPrefix(target.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
	
	if len(agentPathParts) < 2 {
		return "", "", fmt.Errorf("invalid agent path format")
	}
	
	drive = agentPathParts[1]
	return hostname, drive, nil
}

func (a *AgentMount) initializeConnection(target *store.Target) error {
	ctx, cancel := context.WithTimeout(context.Background(), acknowledgementTimeout)
	defer cancel()

	broadcast, err := a.wsHub.SendCommandWithBroadcast(a.Hostname, websockets.Message{
		Type:    "backup_start",
		Content: a.Drive,
	})
	if err != nil {
		return fmt.Errorf("failed to send backup request: %w", err)
	}

	listener := broadcast.Subscribe()
	defer broadcast.CancelSubscription(listener)

	return a.waitForAcknowledgement(ctx, listener)
}

func (a *AgentMount) waitForAcknowledgement(ctx context.Context, listener <-chan websockets.Message) error {
	expectedResponse := "Acknowledged: " + a.Drive
	
	for {
		select {
		case resp := <-listener:
			if resp.Type == "response-backup_start" && resp.Content == expectedResponse {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for backup acknowledgement")
		}
	}
}

func (a *AgentMount) setupMountPoint(target *store.Target) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	agentPathParts := strings.Split(strings.TrimPrefix(target.Path, "agent://"), "/")
	if len(agentPathParts) < 2 {
		return fmt.Errorf("invalid agent path")
	}

	agentHost := agentPathParts[0]
	agentPort, err := utils.DriveLetterPort(rune(a.Drive[0]))
	if err != nil {
		return fmt.Errorf("error mapping '%c' to network port: %w", a.Drive[0], err)
	}

	a.Path = filepath.Join(store.AgentMountBasePath, strings.ReplaceAll(target.Name, " ", "-"))
	a.Unmount() // Clean up any existing mounts

	if err := os.MkdirAll(a.Path, defaultMountPerms); err != nil {
		return fmt.Errorf("error creating directory '%s': %w", a.Path, err)
	}

	return a.startRcloneMount(target, agentHost, agentPort)
}

func (a *AgentMount) startRcloneMount(target *store.Target, agentHost, agentPort string) error {
	privKeyFile := filepath.Join(
		store.DbBasePath,
		"agent_keys",
		strings.ReplaceAll(fmt.Sprintf("%s.key", target.Name), " ", "-"),
	)

	mountArgs := []string{
		"mount",
		"--daemon",
		"--no-seek",
		"--read-only",
		"--uid", "0",
		"--gid", "0",
		"--sftp-disable-hashcheck",
		"--sftp-idle-timeout", "0",
		"--sftp-key-file", privKeyFile,
		"--sftp-port", agentPort,
		"--sftp-user", "proxmox",
		"--sftp-host", agentHost,
		"--allow-other",
		"--sftp-shell-type", "none",
		":sftp:/", a.Path,
	}

	a.cmd = exec.Command("rclone", mountArgs...)
	a.cmd.Env = os.Environ()
	a.cmd.Stdout = os.Stdout
	a.cmd.Stderr = os.Stderr

	return a.cmd.Start()
}

// Unmount safely unmounts the SFTP connection
func (a *AgentMount) Unmount() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.cmd != nil && a.cmd.Process != nil {
		_ = a.cmd.Process.Kill()
	}

	if a.Path != "" {
		umount := exec.Command("umount", a.Path)
		umount.Env = os.Environ()
		_ = umount.Run()
	}
}

// CloseSFTP closes the SFTP connection
func (a *AgentMount) CloseSFTP() {
	if a.wsHub != nil && a.Hostname != "" && a.Drive != "" {
		_ = a.wsHub.SendCommand(a.Hostname, websockets.Message{
			Type:    "backup_close",
			Content: a.Drive,
		})
	}
}

// Cleanup performs complete cleanup of resources
func (a *AgentMount) Cleanup() {
	a.Unmount()
	a.CloseSFTP()
}