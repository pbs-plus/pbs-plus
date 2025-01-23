//go:build linux

package mount

import (
	"encoding/base32"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type AgentMount struct {
	Hostname string
	Drive    string
	Path     string
	Cmd      *exec.Cmd
}

func Mount(storeInstance *store.Store, target *types.Target) (*AgentMount, error) {
	// Parse target information
	splittedTargetName := strings.Split(target.Name, " - ")
	targetHostname := splittedTargetName[0]
	agentPath := strings.TrimPrefix(target.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
	agentHost := agentPathParts[0]
	agentDrive := agentPathParts[1]

	// Encode hostname and drive for API call
	targetHostnameEnc := base32.StdEncoding.EncodeToString([]byte(targetHostname))
	agentDriveEnc := base32.StdEncoding.EncodeToString([]byte(agentDrive))

	// Request mount from agent
	err := proxmox.Session.ProxmoxHTTPRequest(
		http.MethodPost,
		fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", targetHostnameEnc, agentDriveEnc),
		nil,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("Mount: Failed to send mount request to target '%s' -> %w", target.Name, err)
	}

	agentMount := &AgentMount{
		Hostname: targetHostname,
		Drive:    agentDrive,
	}

	// Get port for NFS connection
	agentDriveRune := []rune(agentDrive)[0]
	agentPort, err := utils.DriveLetterPort(agentDriveRune)
	if err != nil {
		agentMount.Unmount()
		return nil, fmt.Errorf("Mount: error mapping \"%c\" to network port -> %w", agentDriveRune, err)
	}

	// Setup mount path
	agentMount.Path = filepath.Join(constants.AgentMountBasePath, strings.ReplaceAll(target.Name, " ", "-"))
	agentMount.Unmount() // Ensure clean mount point

	// Create mount directory if it doesn't exist
	err = os.MkdirAll(agentMount.Path, 0700)
	if err != nil {
		return nil, fmt.Errorf("Mount: error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	// Mount using NFS
	mountArgs := []string{
		"-t", "nfs",
		"-o", fmt.Sprintf("port=%s,mountport=%s,vers=3,ro,tcp,noacl", agentPort, agentPort),
		fmt.Sprintf("%s:/mount", agentHost),
		agentMount.Path,
	}

	// Mount the NFS share
	mnt := exec.Command("mount", mountArgs...)
	mnt.Env = os.Environ()
	mnt.Stdout = os.Stdout
	mnt.Stderr = os.Stderr
	agentMount.Cmd = mnt

	// Try mounting with retries
	const maxRetries = 3
	const retryDelay = 2 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err = mnt.Run()
		if err == nil {
			return agentMount, nil
		}
		lastErr = err
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}

	// If all retries failed, clean up and return error
	agentMount.Unmount()
	return nil, fmt.Errorf("Mount: error mounting NFS share after %d attempts -> %w", maxRetries, lastErr)
}

func (a *AgentMount) Unmount() {
	if a.Path == "" {
		return
	}

	// First try a clean unmount
	umount := exec.Command("umount", a.Path)
	umount.Env = os.Environ()
	err := umount.Run()

	// If clean unmount fails, try force unmount
	if err != nil {
		forceUmount := exec.Command("umount", "-f", a.Path)
		forceUmount.Env = os.Environ()
		_ = forceUmount.Run()
	}

	// Kill any lingering mount process
	if a.Cmd != nil && a.Cmd.Process != nil {
		_ = a.Cmd.Process.Kill()
	}
}

func (a *AgentMount) CloseMount() {
	targetHostnameEnc := base32.StdEncoding.EncodeToString([]byte(a.Hostname))
	agentDriveEnc := base32.StdEncoding.EncodeToString([]byte(a.Drive))

	_ = proxmox.Session.ProxmoxHTTPRequest(
		http.MethodDelete,
		fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", targetHostnameEnc, agentDriveEnc),
		nil,
		nil,
	)
}
