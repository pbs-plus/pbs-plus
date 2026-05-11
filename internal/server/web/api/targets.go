//go:build linux

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	reqTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

func D2DTargetHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		all, err := storeInstance.TargetSvc.GetAllTargets()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		for i := range all {
			if all[i].IsS3() {
				all[i].ConnectionStatus = true
				all[i].AgentVersion = "N/A (S3 target)"
			} else if !all[i].IsAgent() {
				all[i].AgentVersion = "N/A (local target)"
				_, err := os.Stat(all[i].Path)
				all[i].ConnectionStatus = err == nil && validate.IsValid(all[i].Path)
			} else {
				// Instant: check if session exists (map lookup, no RPC)
				if qSess, ok := storeInstance.ARPCAgentsManager.GetQuicPipe(all[i].GetHostname()); ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = qSess.GetVersion()
				} else if tSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(all[i].GetHostname()); ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = tSess.GetVersion()
				}
			}
		}

		digest, err := calculateDigest(all)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := TargetsResponse{
			Data:   all,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

func D2DTargetStatusHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// Trigger async refresh if requested
		if strings.ToLower(r.FormValue("refresh")) == "true" {
			storeInstance.TargetSvc.RefreshStatuses()
		}

		cached := storeInstance.TargetSvc.GetCachedStatuses()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cached)
	}
}

type NewAgentHostnameRequest struct {
	Hostname        string               `json:"hostname"`
	Drives          []reqTypes.DriveInfo `json:"drives"`
	OperatingSystem string               `json:"os"`
}

func D2DTargetAgentHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		authHostname := r.Header.Get("X-PBS-Authenticated-Agent")

		var reqParsed NewAgentHostnameRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, fmt.Errorf("Failed to parse request body: %w", err))
			return
		}

		if authHostname != "" && authHostname != reqParsed.Hostname {
			w.WriteHeader(http.StatusForbidden)
			hostnameErr := fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, reqParsed.Hostname)
			WriteErrorResponse(w, hostnameErr)
			return
		}

		if reqParsed.Hostname == "" {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, fmt.Errorf("Hostname is required in request body"))
			return
		}

		clientIP := r.RemoteAddr
		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			ips := strings.Split(forwarded, ",")
			clientIP = strings.TrimSpace(ips[0])
		}

		if strings.Contains(clientIP, ":") {
			clientIP = strings.Split(clientIP, ":")[0]
		}

		tx, err := storeInstance.TargetSvc.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, fmt.Errorf("Failed to start transaction: %w", err))
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback()
			}
		}()

		for _, parsedDrive := range reqParsed.Drives {
			targetName := database.GetAgentTargetName(reqParsed.Hostname, parsedDrive.Letter, reqParsed.OperatingSystem)

			targetData := database.Target{
				Name:             targetName,
				AgentHost:        database.AgentHost{Name: reqParsed.Hostname},
				VolumeID:         parsedDrive.Letter,
				VolumeType:       parsedDrive.Type,
				VolumeName:       parsedDrive.VolumeName,
				VolumeFS:         parsedDrive.FileSystem,
				VolumeFreeBytes:  int(parsedDrive.FreeBytes),
				VolumeUsedBytes:  int(parsedDrive.UsedBytes),
				VolumeTotalBytes: int(parsedDrive.TotalBytes),
				VolumeFree:       parsedDrive.Free,
				VolumeUsed:       parsedDrive.Used,
				VolumeTotal:      parsedDrive.Total,
			}

			err = storeInstance.TargetSvc.UpsertTarget(tx, targetData)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, fmt.Errorf("Failed to upsert target %s: %w", targetName, err))
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, fmt.Errorf("Failed to commit transaction: %w", err))
			return
		}
		tx = nil

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]bool{
			"success": true,
		})

		if err != nil {
			fmt.Printf("Error encoding success response: %v\n", err)
		}
	}
}

func ExtJsTargetHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		newTarget := database.Target{
			Name:        r.FormValue("name"),
			Path:        r.FormValue("path"),
			MountScript: r.FormValue("mount_script"),
		}

		err = storeInstance.TargetSvc.CreateTarget(nil, newTarget)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsTargetSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			path := r.FormValue("path")
			if path != "" {
				_, s3Err := database.ParseS3Url(path)
				if !validate.IsValid(path) && s3Err != nil {
					WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", path))
					return
				}
			}

			target, err := storeInstance.TargetSvc.GetTarget(validate.DecodePath(r.PathValue("target")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("name") != "" {
				target.Name = r.FormValue("name")
			}
			if path != "" {
				target.Path = path
			}

			target.MountScript = r.FormValue("mount_script")

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						target.Name = ""
					case "path":
						target.Path = ""
					case "mount_script":
						target.MountScript = ""
					}
				}
			}

			err = storeInstance.TargetSvc.UpdateTarget(nil, target)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			target, err := storeInstance.TargetSvc.GetTarget(validate.DecodePath(r.PathValue("target")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if target.IsAgent() {
				arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(target.GetHostname())
				if ok {
					target.AgentVersion = arpcSess.GetVersion()
					target.ConnectionStatus = false

					if strings.ToLower(r.FormValue("status")) == "true" {
						respMsg, err := arpcSess.CallMessage(
							r.Context(),
							"target_status",
							&reqTypes.TargetStatusReq{Drive: target.VolumeID},
						)
						if err == nil && strings.HasPrefix(respMsg, "reachable") {
							target.ConnectionStatus = true
							splittedMsg := strings.Split(respMsg, "|")
							if len(splittedMsg) > 1 {
								target.AgentVersion = splittedMsg[1]
							}
						}
					}
				}
			} else if target.IsS3() {
				target.ConnectionStatus = true
				target.AgentVersion = "N/A (S3 target)"
			} else {
				target.AgentVersion = "N/A (local target)"

				_, err := os.Stat(target.Path)
				if err != nil {
					target.ConnectionStatus = false
				} else {
					target.ConnectionStatus = validate.IsValid(target.Path)
				}
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = target
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.TargetSvc.DeleteTarget(nil, validate.DecodePath(r.PathValue("target")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}

func ExtJsTargetS3SecretHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		target, err := storeInstance.TargetSvc.GetTarget(validate.DecodePath(r.PathValue("target")))
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		if r.FormValue("secret") == "" {
			WriteErrorResponse(w, errors.New("invalid empty secret"))
			return
		}

		err = storeInstance.TargetSvc.AddS3Secret(target.Name, r.FormValue("secret"))
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}
