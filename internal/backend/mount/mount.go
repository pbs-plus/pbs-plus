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
}

func Mount(storeInstance *store.Store, target *types.Target) (*AgentMount, error) {
	// Parse target information
	splittedTargetName := strings.Split(target.Name, " - ")
	targetHostname := splittedTargetName[0]
	agentPath := strings.TrimPrefix(target.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
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

	// Setup mount path
	agentMount.Path = filepath.Join(constants.AgentMountBasePath, strings.ReplaceAll(target.Name, " ", "-"))
	// Create mount directory if it doesn't exist
	err := os.MkdirAll(agentMount.Path, 0700)
	if err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: error creating directory \"%s\" -> %w", agentMount.Path, err)
	}

	agentMount.Unmount() // Ensure clean mount point

	if err != nil {
		agentMount.CloseMount()
		return nil, fmt.Errorf("Mount: Failed to send mount request to target '%s' -> %w", target.Name, err)
	}

	// Try mounting with retries
	const maxRetries = 3
	const retryDelay = 2 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err = backupSession.ProxmoxHTTPRequest(
			http.MethodPost,
			fmt.Sprintf("https://localhost:8008/plus/mount/%s/%s", targetHostnameEnc, agentDriveEnc),
			nil,
			nil,
		)
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
	err := umount.Run()
	if err == nil {
		_ = os.RemoveAll(a.Path)
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
