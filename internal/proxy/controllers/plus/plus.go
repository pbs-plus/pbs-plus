//go:build linux

package plus

import (
	"encoding/base32"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

func MountHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// TODO: add check for security

		targetHostnameEnc := utils.DecodePath(r.PathValue("target"))
		agentDriveEnc := utils.DecodePath(r.PathValue("drive"))

		targetHostnameBytes, err := base32.StdEncoding.DecodeString(targetHostnameEnc)
		if err != nil {
			http.Error(w, "invalid arguments", http.StatusBadRequest)
			return
		}

		agentDriveBytes, err := base32.StdEncoding.DecodeString(agentDriveEnc)
		if err != nil {
			http.Error(w, "invalid arguments", http.StatusBadRequest)
			return
		}

		targetHostname := string(targetHostnameBytes)
		agentDrive := string(agentDriveBytes)

		if r.Method == http.MethodPost {
			err := storeInstance.WSHub.SendToClient(targetHostname, websockets.Message{
				Type:    "backup_start",
				Content: agentDrive,
			})
			if err != nil {
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send backup request to target -> %v", err), http.StatusInternalServerError)
				return
			}

			listener, closeListener := storeInstance.WSHub.RegisterHandler()
			defer closeListener()
		respWait:
			for {
				select {
				case resp := <-listener:
					if resp.Type == "response-backup_start" && resp.Content == "Acknowledged: "+agentDrive {
						break respWait
					}
				case <-time.After(time.Second * 15):
					http.Error(w, fmt.Sprintf("MountHandler: Failed to receive backup acknowledgement from target -> %v", err), http.StatusInternalServerError)
					return
				}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "true"})

			return
		}

		if r.Method == http.MethodDelete {
			_ = storeInstance.WSHub.SendToClient(targetHostname, websockets.Message{
				Type:    "backup_close",
				Content: agentDrive,
			})

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "true"})

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
