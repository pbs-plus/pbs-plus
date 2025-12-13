//go:build linux

package targets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
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
	Volumes          []utils.DriveInfo
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

			arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(tgt.Name)
			if !ok {
				results[idx] = result
				return
			}

			result.AgentVersion = arpcSess.GetVersion()
			result.ConnectionStatus = false

			if checkStatus {
				timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()

				var resp reqTypes.TargetStatusResp
				err := arpcSess.Call(
					timeoutCtx,
					"target_status",
					&reqTypes.TargetStatusReq{},
					&resp,
				)
				if err == nil {
					result.ConnectionStatus = true
					result.Volumes = resp.Volumes
				}

				result.Error = err
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

		withVolumes := strings.ToLower(r.FormValue("volumes")) == "true"
		all, err := storeInstance.Database.GetAllTargets(withVolumes)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		checkStatus := strings.ToLower(r.FormValue("status")) == "true"

		// Separate agent targets from others for batch processing
		agentTargets := make([]types.Target, 0)
		agentIndices := make([]int, 0)

		for i := range all {
			switch all[i].TargetType {
			case "agent":
				agentTargets = append(agentTargets, all[i])
				agentIndices = append(agentIndices, i)
			case "s3":
				all[i].ConnectionStatus = true
				all[i].AgentVersion = "N/A (S3 target)"
			default:
				all[i].AgentVersion = "N/A (local target)"
				_, err := os.Stat(all[i].LocalPath)
				all[i].ConnectionStatus = err == nil && utils.IsValid(all[i].LocalPath)
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
				accessibleVols := result.Volumes
				for volIdx, vol := range all[originalIdx].Volumes {
					if slices.ContainsFunc(accessibleVols, func(s utils.DriveInfo) bool {
						return s.Letter == vol.VolumeName
					}) {
						all[originalIdx].Volumes[volIdx].Accessible = true
					}
				}
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

func D2DVolumeHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		target := r.PathValue("target")
		all, err := storeInstance.Database.GetAllVolumes(target)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		digest, err := utils.CalculateDigest(all)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		toReturn := VolumesResponse{
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

		existingTarget, err := storeInstance.Database.GetTarget(reqParsed.Hostname)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			controllers.WriteErrorResponse(w, fmt.Errorf("target hostname not found: %w", err))
			return
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

		existingTarget.AgentHost = clientIP
		volumes := make([]types.Volume, len(reqParsed.Drives))
		for _, drive := range reqParsed.Drives {
			volumes = append(volumes, types.Volume{
				VolumeName:     drive.Letter,
				MetaType:       drive.Type,
				MetaFS:         drive.FileSystem,
				MetaFreeBytes:  int(drive.FreeBytes),
				MetaUsedBytes:  int(drive.UsedBytes),
				MetaTotalBytes: int(drive.TotalBytes),
				MetaFree:       drive.Free,
				MetaUsed:       drive.Used,
				MetaTotal:      drive.Total,
			})
		}

		existingTarget.Volumes = volumes

		err = storeInstance.Database.UpdateTarget(tx, existingTarget)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, fmt.Errorf("Failed to update target %s: %w", clientIP, err))
			return
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
			Name:        r.FormValue("name"),
			MountScript: r.FormValue("mount_script"),
		}

		s3, err := s3url.Parse(path)
		if err == nil {
			newTarget.S3AccessID = s3.AccessKey
			newTarget.S3Host = s3.Endpoint
			newTarget.S3Bucket = s3.Bucket
			newTarget.S3UsePathStyle = s3.IsPathStyle
			newTarget.S3Region = s3.Region
			newTarget.S3UseSSL = s3.UseSSL
			newTarget.TargetType = "s3"
		} else if utils.IsValid(path) {
			newTarget.LocalPath = path
			newTarget.TargetType = "local"
		} else {
			controllers.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", path))
			return
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

			target, err := storeInstance.Database.GetTarget(utils.DecodePath(r.PathValue("target")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("name") != "" {
				target.Name = r.FormValue("name")
			}
			if path != "" {
				s3, err := s3url.Parse(path)
				if err == nil {
					target.S3AccessID = s3.AccessKey
					target.S3Host = s3.Endpoint
					target.S3Bucket = s3.Bucket
					target.S3UsePathStyle = s3.IsPathStyle
					target.S3Region = s3.Region
					target.S3UseSSL = s3.UseSSL
					target.TargetType = "s3"
				} else if utils.IsValid(path) {
					target.LocalPath = path
					target.TargetType = "local"
				} else {
					controllers.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", path))
					return
				}
			}

			target.MountScript = r.FormValue("mount_script")

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						target.Name = ""
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
			target, err := storeInstance.Database.GetTarget(utils.DecodePath(r.PathValue("target")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			switch target.TargetType {
			case "agent":
				arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(target.Name)
				if ok {
					target.AgentVersion = arpcSess.GetVersion()
					target.ConnectionStatus = false

					if strings.ToLower(r.FormValue("status")) == "true" {
						respMsg, err := arpcSess.CallMessage(
							r.Context(),
							"target_status",
							&reqTypes.TargetStatusReq{},
						)
						if err == nil && strings.HasPrefix(respMsg, "reachable") {
							target.ConnectionStatus = true
						}
					}
				}
			case "s3":
				target.ConnectionStatus = true
				target.AgentVersion = "N/A (S3 target)"
			default:
				target.AgentVersion = "N/A (local target)"
				_, err := os.Stat(target.LocalPath)
				if err != nil {
					target.ConnectionStatus = false
				} else {
					target.ConnectionStatus = utils.IsValid(target.LocalPath)
				}
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = target
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.Database.DeleteTarget(nil, utils.DecodePath(r.PathValue("target")))
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

		target, err := storeInstance.Database.GetTarget(utils.DecodePath(r.PathValue("target")))
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		if target.TargetType != "s3" {
			controllers.WriteErrorResponse(w, errors.New("target is not a valid S3 path"))
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
