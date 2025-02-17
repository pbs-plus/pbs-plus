//go:build linux

package plus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

		targetHostname := utils.DecodePath(r.PathValue("target"))
		agentDrive := utils.DecodePath(r.PathValue("drive"))

		if r.Method == http.MethodPost {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			// Create response channel and register handler
			respChan := make(chan *websockets.Message, 1)
			errChan := make(chan *websockets.Message, 1)
			cleanup := storeInstance.WSHub.RegisterHandler("response-backup_start", func(handlerCtx context.Context, msg *websockets.Message) error {
				if msg.Content == "Acknowledged: "+agentDrive && msg.ClientID == targetHostname {
					respChan <- msg
				}
				return nil
			})
			defer cleanup()
			cleanupErr := storeInstance.WSHub.RegisterHandler("error-backup_start", func(handlerCtx context.Context, msg *websockets.Message) error {
				if strings.Contains(msg.Content, agentDrive+": ") && msg.ClientID == targetHostname {
					errChan <- msg
				}
				return nil
			})
			defer cleanupErr()

			// Send initial message
			err := storeInstance.WSHub.SendToClient(targetHostname, websockets.Message{
				Type:    "backup_start",
				Content: agentDrive,
			})
			if err != nil {
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send backup request to target -> %v", err), http.StatusInternalServerError)
				return
			}

			// Wait for either response or timeout
			select {
			case <-respChan:
			case err := <-errChan:
				http.Error(w, fmt.Sprintf("MountHandler: %s", err.Content), http.StatusInternalServerError)
				return
			case <-ctx.Done():
				http.Error(w, "MountHandler: Timeout waiting for backup acknowledgement from target", http.StatusInternalServerError)
				return
			}

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

			// Create response channel and register handler
			respChan := make(chan *websockets.Message, 1)
			errChan := make(chan *websockets.Message, 1)
			cleanup := storeInstance.WSHub.RegisterHandler("response-backup_close", func(handlerCtx context.Context, msg *websockets.Message) error {
				if msg.Content == "Acknowledged: "+agentDrive && msg.ClientID == targetHostname {
					respChan <- msg
				}
				return nil
			})
			defer cleanup()
			cleanupErr := storeInstance.WSHub.RegisterHandler("error-backup_close", func(handlerCtx context.Context, msg *websockets.Message) error {
				if strings.Contains(msg.Content, agentDrive+": ") && msg.ClientID == targetHostname {
					errChan <- msg
				}
				return nil
			})
			defer cleanupErr()

			err := storeInstance.WSHub.SendToClient(targetHostname, websockets.Message{
				Type:    "backup_close",
				Content: agentDrive,
			})
			if err != nil {
				syslog.L.Errorf("MountHandler: Failed to send backup request to target -> %v", err)
				http.Error(w, fmt.Sprintf("MountHandler: Failed to send backup request to target -> %v", err), http.StatusInternalServerError)
				return
			}

			// Wait for either response or timeout
			select {
			case <-respChan:
			case err := <-errChan:
				syslog.L.Errorf("MountHandler: %s", err.Content)
				http.Error(w, fmt.Sprintf("MountHandler: %s", err.Content), http.StatusInternalServerError)
				return
			case <-ctx.Done():
				syslog.L.Error("MountHandler: Timeout waiting for backup acknowledgement from target")
				http.Error(w, "MountHandler: Timeout waiting for backup acknowledgement from target", http.StatusInternalServerError)
				return
			}

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
