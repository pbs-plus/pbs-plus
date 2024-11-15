//go:build linux

package mount

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type AgentMount struct {
	Path string
	Cmd  *exec.Cmd
}

func Mount(target *store.Target) (*AgentMount, error) {
	if !utils.IsValid("/usr/bin/rclone") {
		return nil, fmt.Errorf("Mount: rclone is missing! Please install rclone first before backing up from agent.")
	}

	agentMount := &AgentMount{}

	agentPath := strings.TrimPrefix(target.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
	agentHost := agentPathParts[0]
	agentDrive := agentPathParts[1]
	agentDriveRune := []rune(agentDrive)[0]
	agentPort, err := utils.DriveLetterPort(agentDriveRune)
	if err != nil {
		agentMount.Unmount()
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
		"--vfs-cache-mode", "minimal",
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

	err = mnt.Run()
	if err != nil {
		agentMount.Unmount()
		return nil, fmt.Errorf("Mount: error starting rclone for sftp -> %w", err)
	}

	ls := exec.Command("bash", "-c", fmt.Sprintf("find %s -type f | xargs -P 8 -n 1 true", agentMount.Path))
	ls.Env = os.Environ()

	_ = ls.Start()

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
