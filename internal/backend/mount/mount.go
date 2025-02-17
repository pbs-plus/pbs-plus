//go:build linux

package mount

import (
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
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

	agentMount := &AgentMount{
		Hostname: targetHostname,
		Drive:    agentDrive,
	}

	// Encode hostname and drive for API call
	targetHostnameEnc := utils.EncodePath(targetHostname)
	agentDriveEnc := utils.EncodePath(agentDrive)

	// Request mount from agent
	backupSession := &proxmox.ProxmoxSession{
		APIToken: proxmox.Session.APIToken,
		HTTPClient: &http.Client{
			Timeout:   time.Minute * 5,
			Transport: utils.MountTransport,
		},
	}

	err := backupSession.ProxmoxHTTPRequest(
		http.MethodPost,
		fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", targetHostnameEnc, agentDriveEnc),
		nil,
		nil,
	)
	if err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: Failed to send mount request to target '%s' -> %w", target.Name, err)
	}

	// Get port for NFS connection
	agentDriveRune := []rune(agentDrive)[0]
	agentPort, err := utils.DriveLetterPort(agentDriveRune)
	if err != nil {
		agentMount.Unmount()
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: error mapping \"%c\" to network port -> %w", agentDriveRune, err)
	}

	// Setup mount path
	agentMount.Path = filepath.Join(constants.AgentMountBasePath, strings.ReplaceAll(target.Name, " ", "-"))
	agentMount.Unmount() // Ensure clean mount point

	// Create mount directory if it doesn't exist
	err = os.MkdirAll(agentMount.Path, 0700)
	if err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	// Mount using NFS
	mountArgs := []string{
		"-t", "nfs",
		"-o", fmt.Sprintf("port=%s,mountport=%s,vers=3,ro,tcp,noacl,nocto,actimeo=3600,rsize=1048576,lookupcache=positive,noatime", agentPort, agentPort),
		fmt.Sprintf("%s:/", agentHost),
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
			isAccessible := false
			checkTimeout := time.After(10 * time.Second)
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

		checkLoop:
			for {
				select {
				case <-checkTimeout:
					break checkLoop
				case <-ticker.C:
					// Try to read directory contents
					_, err := os.ReadDir(agentMount.Path)
					if err == nil {
						isAccessible = true
						break checkLoop
					}
				}
			}

			if !isAccessible {
				// Clean up if mount point isn't accessible
				agentMount.Unmount()
				agentMount.CloseMount()
				return nil, fmt.Errorf("Mount: mounted directory not accessible after 10 seconds")
			}

			return agentMount, nil
		}
		lastErr = err
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}

	// If all retries failed, clean up and return error
	agentMount.Unmount()
	agentMount.CloseMount()
	return nil, fmt.Errorf("Mount: error mounting NFS share after %d attempts -> %w", maxRetries, lastErr)
}

func (a *AgentMount) Unmount() {
	if a.Path == "" {
		return
	}

	// First try a clean unmount
	umount := exec.Command("umount", "-lf", a.Path)
	umount.Env = os.Environ()
	_ = umount.Run()

	// Kill any lingering mount process
	if a.Cmd != nil && a.Cmd.Process != nil {
		_ = a.Cmd.Process.Kill()
	}
}

func (a *AgentMount) CloseMount() {
	targetHostnameEnc := utils.EncodePath(a.Hostname)
	agentDriveEnc := utils.EncodePath(a.Drive)

	syslog.L.Infof("CloseMount: Sending request for %s/%s", a.Hostname, a.Drive)
	err := proxmox.Session.ProxmoxHTTPRequest(
		http.MethodDelete,
		fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", targetHostnameEnc, agentDriveEnc),
		nil,
		nil,
	)
	if err != nil {
		syslog.L.Errorf("CloseMount: error sending unmount request -> %v", err)
	}
}
