//go:build linux

package targets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	reqTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	s3url "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/url"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

func CheckTargetStatusBatch(
	ctx context.Context,
	storeInstance *store.Store,
	targets []types.Target,
	checkStatus bool,
	timeout time.Duration,
) []TargetStatusResult {
	results := make([]TargetStatusResult, len(targets))
	var wg sync.WaitGroup

	sem := make(chan struct{}, 20) // Max concurrent requests

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, tgt types.Target) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			result := TargetStatusResult{Index: idx}

			if !tgt.Path.IsAgent() {
				results[idx] = result
				return
			}

			arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(tgt.Name.GetHostname())
			if !ok {
				results[idx] = result
				return
			}

			result.AgentVersion = arpcSess.GetVersion()
			result.ConnectionStatus = false

			if checkStatus {
				timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()

				respMsg, err := arpcSess.CallMessage(
					timeoutCtx,
					"target_status",
					&reqTypes.TargetStatusReq{Drive: tgt.Name.GetVolume()},
				)
				if err == nil && strings.HasPrefix(respMsg, "reachable") {
					result.ConnectionStatus = true
					splittedMsg := strings.Split(respMsg, "|")
					if len(splittedMsg) > 1 {
						result.AgentVersion = splittedMsg[1]
					}
				} else if err != nil {
					result.Error = err
				}
			}

			results[idx] = result
		}(i, target)
	}

	wg.Wait()
	return results
}

func D2DTargetHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		all, err := storeInstance.Database.GetAllTargets()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		checkStatus := strings.ToLower(r.FormValue("status")) == "true"

		// Separate agent targets from others for batch processing
		agentTargets := make([]types.Target, 0)
		agentIndices := make([]int, 0)

		for i := range all {
			if all[i].Path.IsAgent() {
				agentTargets = append(agentTargets, all[i])
				agentIndices = append(agentIndices, i)
			} else if all[i].Path.IsS3() {
				all[i].ConnectionStatus = true
				all[i].AgentVersion = "N/A (S3 target)"
			} else {
				all[i].AgentVersion = "N/A (local target)"
				_, err := os.Stat(all[i].Path.String())
				all[i].ConnectionStatus = err == nil && utils.IsValid(all[i].Path.String())
			}
		}

		if len(agentTargets) > 0 {
			timeout := 5 * time.Second
			results := CheckTargetStatusBatch(
				r.Context(),
				storeInstance,
				agentTargets,
				checkStatus,
				timeout,
			)

			for i, result := range results {
				originalIdx := agentIndices[i]
				all[originalIdx].AgentVersion = result.AgentVersion
				all[originalIdx].ConnectionStatus = result.ConnectionStatus
			}
		}

		digest, err := utils.CalculateDigest(all)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
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

type NewAgentHostnameRequest struct {
	Hostname string            `json:"hostname"`
	Drives   []utils.DriveInfo `json:"drives"`
}

func D2DTargetAgentHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		var reqParsed NewAgentHostnameRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, fmt.Errorf("Failed to parse request body: %w", err))
			return
		}

		if reqParsed.Hostname == "" {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, fmt.Errorf("Hostname is required in request body"))
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

		existingTargets, err := storeInstance.Database.GetAllTargetsByIP(clientIP)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, fmt.Errorf("Failed to get existing targets: %w", err))
			return
		}

		var targetTemplate types.Target
		if len(existingTargets) > 0 {
			targetTemplate = existingTargets[0]
		}

		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, fmt.Errorf("Failed to start transaction: %w", err))
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback()
			}
		}()

		existingTargetsMap := make(map[types.TargetName]types.Target)
		for _, target := range existingTargets {
			existingTargetsMap[target.Name] = target
		}

		processedTargetNames := make(map[types.TargetName]bool)

		for _, parsedDrive := range reqParsed.Drives {
			targetName := types.NewTargetName(types.TargetTypeAgent, reqParsed.Hostname, parsedDrive.Letter, parsedDrive.OperatingSystem)
			targetPath := types.NewTargetPath(types.TargetTypeAgent, clientIP, parsedDrive.Letter, parsedDrive.OperatingSystem)

			processedTargetNames[targetName] = true

			if existingTarget, found := existingTargetsMap[targetName]; found {
				updatedTarget := existingTarget
				updatedTarget.DriveType = parsedDrive.Type
				updatedTarget.DriveName = parsedDrive.VolumeName
				updatedTarget.DriveFS = parsedDrive.FileSystem
				updatedTarget.DriveFreeBytes = int(parsedDrive.FreeBytes)
				updatedTarget.DriveUsedBytes = int(parsedDrive.UsedBytes)
				updatedTarget.DriveTotalBytes = int(parsedDrive.TotalBytes)
				updatedTarget.DriveFree = parsedDrive.Free
				updatedTarget.DriveUsed = parsedDrive.Used
				updatedTarget.DriveTotal = parsedDrive.Total
				updatedTarget.OperatingSystem = parsedDrive.OperatingSystem

				err = storeInstance.Database.UpdateTarget(tx, updatedTarget)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					controllers.WriteErrorResponse(w, fmt.Errorf("Failed to update target %s: %w", targetName, err))
					return
				}
			} else {
				targetData := types.Target{
					Name:            targetName,
					Path:            targetPath,
					Auth:            targetTemplate.Auth,
					TokenUsed:       targetTemplate.TokenUsed,
					DriveType:       parsedDrive.Type,
					DriveName:       parsedDrive.VolumeName,
					DriveFS:         parsedDrive.FileSystem,
					DriveFreeBytes:  int(parsedDrive.FreeBytes),
					DriveUsedBytes:  int(parsedDrive.UsedBytes),
					DriveTotalBytes: int(parsedDrive.TotalBytes),
					DriveFree:       parsedDrive.Free,
					DriveUsed:       parsedDrive.Used,
					DriveTotal:      parsedDrive.Total,
					OperatingSystem: parsedDrive.OperatingSystem,
				}

				err = storeInstance.Database.CreateTarget(tx, targetData)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					controllers.WriteErrorResponse(w, fmt.Errorf("Failed to create target %s: %w", targetName, err))
					return
				}
			}
		}

		for _, existingTarget := range existingTargets {
			if _, processed := processedTargetNames[existingTarget.Name]; !processed {
				expectedPrefix := reqParsed.Hostname + " - "
				if strings.HasPrefix(existingTarget.Name.String(), expectedPrefix) {
					err = storeInstance.Database.DeleteTarget(tx, existingTarget.Name)
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
						controllers.WriteErrorResponse(w, fmt.Errorf("Failed to delete target %s: %w", existingTarget.Name, err))
						return
					}
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, fmt.Errorf("Failed to commit transaction: %w", err))
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
			controllers.WriteErrorResponse(w, err)
			return
		}

		path := r.FormValue("path")
		newTarget := types.Target{
			Name:        types.WrapTargetName(r.FormValue("name")),
			Path:        types.WrapTargetPath(path),
			MountScript: r.FormValue("mount_script"),
		}

		err = storeInstance.Database.CreateTarget(nil, newTarget)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
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
				controllers.WriteErrorResponse(w, err)
				return
			}

			path := r.FormValue("path")
			if path != "" {
				_, s3Err := s3url.Parse(path)
				if !utils.IsValid(path) && s3Err != nil {
					controllers.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", path))
					return
				}
			}

			target, err := storeInstance.Database.GetTarget(types.WrapTargetName(utils.DecodePath(r.PathValue("target"))))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("name") != "" {
				target.Name = types.WrapTargetName(r.FormValue("name"))
			}
			if path != "" {
				target.Path = types.WrapTargetPath(path)
			}

			target.MountScript = r.FormValue("mount_script")

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						target.Name.Raw = ""
					case "path":
						target.Path.Raw = ""
					case "mount_script":
						target.MountScript = ""
					}
				}
			}

			err = storeInstance.Database.UpdateTarget(nil, target)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			target, err := storeInstance.Database.GetTarget(types.WrapTargetName(utils.DecodePath(r.PathValue("target"))))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if target.Path.IsAgent() {
				arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(target.Name.GetHostname())
				if ok {
					target.AgentVersion = arpcSess.GetVersion()
					target.ConnectionStatus = false

					if strings.ToLower(r.FormValue("status")) == "true" {
						respMsg, err := arpcSess.CallMessage(
							r.Context(),
							"target_status",
							&reqTypes.TargetStatusReq{Drive: target.Name.GetVolume()},
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
			} else if target.Path.IsS3() {
				target.ConnectionStatus = true
				target.AgentVersion = "N/A (S3 target)"
			} else {
				target.AgentVersion = "N/A (local target)"

				_, err := os.Stat(target.Path.String())
				if err != nil {
					target.ConnectionStatus = false
				} else {
					target.ConnectionStatus = utils.IsValid(target.Path.String())
				}
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = target
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.Database.DeleteTarget(nil, types.WrapTargetName(utils.DecodePath(r.PathValue("target"))))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
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
			controllers.WriteErrorResponse(w, err)
			return
		}

		target, err := storeInstance.Database.GetTarget(types.WrapTargetName(utils.DecodePath(r.PathValue("target"))))
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		if r.FormValue("secret") == "" {
			controllers.WriteErrorResponse(w, errors.New("invalid empty secret"))
			return
		}

		err = storeInstance.Database.AddS3Secret(nil, target.Name, r.FormValue("secret"))
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}
