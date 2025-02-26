//go:build linux

package plus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/mount"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func MountHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		targetHostname := utils.DecodePath(r.PathValue("target"))
		agentDrive := utils.DecodePath(r.PathValue("drive"))
		targetName := targetHostname + " - " + agentDrive

		if r.Method == http.MethodPost {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			arpcSess := storeInstance.GetARPC(targetHostname)
			if arpcSess == nil {
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send backup request to target -> unable to reach target"), http.StatusInternalServerError)
				return
			}

			backupResp, err := arpcSess.CallContext(ctx, "backup", agentDrive)
			if err != nil || backupResp.Status != 200 {
				if err != nil {
					err = errors.New(backupResp.Message)
				}
				syslog.L.Errorf("MountHandler: Failed to send backup request to target -> %v", err)
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send backup request to target -> %v", err), http.StatusInternalServerError)
				return
			}

			arpcFS := storeInstance.GetARPCFS(targetName)
			if arpcFS == nil {
				arpcFS = arpcfs.NewARPCFS(context.Background(), storeInstance.GetARPC(targetHostname), targetHostname, agentDrive)
			}

			mntPath := filepath.Join(constants.AgentMountBasePath, strings.ReplaceAll(targetName, " ", "-"))

			err = mount.Mount(arpcFS, mntPath)
			if err != nil {
				syslog.L.Errorf("MountHandler: Failed to create fuse connection for target -> %v", err)
				http.Error(w, fmt.Sprintf("MountHandler: Failed to create fuse connection for target -> %v", err), http.StatusInternalServerError)
				return
			}

			storeInstance.AddARPCFS(targetName, arpcFS)

			// Handle successful response
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]string{"status": "true"}); err != nil {
				http.Error(w, fmt.Sprintf("MountHandler: Failed to encode response -> %v", err), http.StatusInternalServerError)
				return
			}
		}

		if r.Method == http.MethodDelete {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			arpcSess := storeInstance.GetARPC(targetHostname)
			if arpcSess == nil {
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send closure request to target -> unable to reach target"), http.StatusInternalServerError)
				return
			}

			arpcFS := storeInstance.GetARPCFS(targetName)
			if arpcFS == nil {
				arpcFS = arpcfs.NewARPCFS(context.Background(), storeInstance.GetARPC(targetHostname), targetHostname, agentDrive)
				arpcFS.Unmount()
			} else {
				storeInstance.RemoveARPCFS(targetName)
			}

			cleanupResp, err := arpcSess.CallContext(ctx, "cleanup", agentDrive)
			if err != nil || cleanupResp.Status != 200 {
				if err != nil {
					err = errors.New(cleanupResp.Message)
				}
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send closure request to target -> %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]string{"status": "true"}); err != nil {
				http.Error(w, fmt.Sprintf("MountHandler: Failed to encode response -> %v", err), http.StatusInternalServerError)
				return
			}

			return
		}
	}
}

func VersionHandler(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		toReturn := VersionResponse{
			Version: version,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

func DownloadBinary(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// Construct the passthrough URL
		baseURL := "https://github.com/sonroyaalmerol/pbs-plus/releases/download/"
		targetURL := fmt.Sprintf("%s%s/pbs-plus-agent-%s-windows-amd64.exe", baseURL, version, version)

		// Proxy the request
		req, err := http.NewRequest(http.MethodGet, targetURL, nil)
		if err != nil {
			http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
			return
		}

		// Copy headers from the original request to the proxy request
		copyHeaders(r.Header, req.Header)

		// Perform the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "failed to fetch binary", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Copy headers from the upstream response to the client response
		copyHeaders(resp.Header, w.Header())

		// Set the status code and copy the body
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			http.Error(w, "failed to write response body", http.StatusInternalServerError)
			return
		}
	}
}

func DownloadChecksum(storeInstance *store.Store, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// Construct the passthrough URL
		baseURL := "https://github.com/sonroyaalmerol/pbs-plus/releases/download/"
		targetURL := fmt.Sprintf("%s%s/pbs-plus-agent-%s-windows-amd64.exe.md5", baseURL, version, version)

		// Proxy the request
		req, err := http.NewRequest(http.MethodGet, targetURL, nil)
		if err != nil {
			http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
			return
		}

		// Copy headers from the original request to the proxy request
		copyHeaders(r.Header, req.Header)

		// Perform the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "failed to fetch checksum", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Copy headers from the upstream response to the client response
		copyHeaders(resp.Header, w.Header())

		// Set the status code and copy the body
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			http.Error(w, "failed to write response body", http.StatusInternalServerError)
			return
		}
	}
}

// copyHeaders is a helper function to copy headers from one Header map to another
func copyHeaders(src, dst http.Header) {
	for name, values := range src {
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}
