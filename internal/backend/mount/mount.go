//go:build linux

package mount

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type AgentMount struct {
	Hostname      string
	Drive         string
	Path          string
	Cmd           *exec.Cmd
	StoreInstance *store.Store
}

func Mount(storeInstance *store.Store, target *store.Target) (*AgentMount, error) {
	if !utils.IsValid("/usr/bin/rclone") {
		return nil, fmt.Errorf("Mount: rclone is missing! Please install rclone first before backing up from agent.")
	}

	splittedTargetName := strings.Split(target.Name, " - ")
	targetHostname := splittedTargetName[0]
	agentPath := strings.TrimPrefix(target.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
	agentDrive := agentPathParts[1]

	err := storeInstance.ProxmoxHTTPRequest(http.MethodPost, fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", targetHostname, agentDrive), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: Failed to send backup request to target '%s' -> %w", target.Name, err)
	}

	agentMount := &AgentMount{StoreInstance: storeInstance, Hostname: targetHostname, Drive: agentDrive}

	agentHost := agentPathParts[0]
	agentDriveRune := []rune(agentDrive)[0]
	agentPort, err := utils.DriveLetterPort(agentDriveRune)
	if err != nil {
		agentMount.Unmount()
		agentMount.CloseSFTP()
		return nil, fmt.Errorf("Mount: error mapping \"%c\" to network port -> %w", agentDriveRune, err)
	}

	agentMount.Path = filepath.Join(store.AgentMountBasePath, strings.ReplaceAll(target.Name, " ", "-"))
	agentMount.Unmount()

	err = os.MkdirAll(agentMount.Path, 0700)
	if err != nil {
		return nil, fmt.Errorf("Mount: error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	privKeyDir := filepath.Join(store.DbBasePath, "agent_keys")
	privKeyFile := filepath.Join(privKeyDir, strings.ReplaceAll(fmt.Sprintf("%s.key", target.Name), " ", "-"))

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
		":sftp:/", agentMount.Path,
	}

	mnt := exec.Command("rclone", mountArgs...)
	mnt.Env = os.Environ()

	mnt.Stdout = os.Stdout
	mnt.Stderr = os.Stderr

	agentMount.Cmd = mnt

	err = mnt.Start()
	if err != nil {
		agentMount.Unmount()
		agentMount.CloseSFTP()
		return nil, fmt.Errorf("Mount: error starting rclone for sftp -> %w", err)
	}

	return agentMount, nil
}

func (a *AgentMount) Unmount() {
	if a.Cmd != nil && a.Cmd.Process != nil {
		_ = a.Cmd.Process.Kill()
	}

	umount := exec.Command("umount", a.Path)
	umount.Env = os.Environ()

	_ = umount.Run()

}

func (a *AgentMount) CloseSFTP() {
	_ = a.StoreInstance.ProxmoxHTTPRequest(http.MethodDelete, fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", a.Hostname, a.Drive), nil, nil)
}
