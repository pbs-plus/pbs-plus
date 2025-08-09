//go:build linux

package targets

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	s3url "github.com/pbs-plus/pbs-plus/internal/backend/s3/url"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func D2DTargetHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		all, err := storeInstance.Database.GetAllTargets()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		for i := range all {
			if all[i].IsAgent {
				targetSplit := strings.Split(all[i].Name, " - ")
				hostname := targetSplit[0]

				arpcSess, ok := storeInstance.ARPCSessionManager.GetSession(hostname)
				if ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = arpcSess.GetVersion()
				}
			} else if all[i].IsS3 {
				all[i].ConnectionStatus = true
				all[i].AgentVersion = "N/A (S3 target)"
			} else {
				all[i].AgentVersion = "N/A (local target)"

				_, err := os.Stat(all[i].Path)
				if err != nil {
					all[i].ConnectionStatus = false
				} else {
					all[i].ConnectionStatus = utils.IsValid(all[i].Path)
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
			_ = tx.Rollback()
		}()

		existingTargetsMap := make(map[string]types.Target)
		for _, target := range existingTargets {
			existingTargetsMap[target.Name] = target
		}

		processedTargetNames := make(map[string]bool)

		for _, parsedDrive := range reqParsed.Drives {
			targetName := reqParsed.Hostname
			targetData := types.Target{
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

			switch parsedDrive.OperatingSystem {
			case "windows":
				driveLetter := parsedDrive.Letter
				targetName = targetName + " - " + driveLetter
				targetPath := "agent://" + clientIP + "/" + driveLetter
				processedTargetNames[targetName] = true

				targetData.Name = targetName
				targetData.Path = targetPath
			default:
				targetName = targetName + " - Root"
				targetPath := "agent://" + clientIP + "/root"
				processedTargetNames[targetName] = true

				targetData.Name = targetName
				targetData.Path = targetPath
			}

			if _, found := existingTargetsMap[targetName]; found {
				err = storeInstance.Database.UpdateTarget(tx, targetData)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					controllers.WriteErrorResponse(w, fmt.Errorf("Failed to update target %s: %w", targetName, err))
					return
				}
			} else {
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
				// Only delete if the target name matches the hostname from the request
				// This prevents deleting targets from other hosts sharing the same IP
				expectedPrefix := reqParsed.Hostname + " - "
				if strings.HasPrefix(existingTarget.Name, expectedPrefix) {
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
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		_, s3Err := s3url.Parse(r.FormValue("path"))
		if !utils.IsValid(r.FormValue("path")) && s3Err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", r.FormValue("path")))
			return
		}

		newTarget := types.Target{
			Name:        r.FormValue("name"),
			Path:        r.FormValue("path"),
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
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			_, s3Err := s3url.Parse(r.FormValue("path"))
			if !utils.IsValid(r.FormValue("path")) && s3Err != nil {
				controllers.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", r.FormValue("path")))
				return
			}

			target, err := storeInstance.Database.GetTarget(utils.DecodePath(r.PathValue("target")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("name") != "" {
				target.Name = r.FormValue("name")
			}
			if r.FormValue("path") != "" {
				target.Path = r.FormValue("path")
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

			if target.IsAgent {
				targetSplit := strings.Split(target.Name, " - ")
				arpcSess, ok := storeInstance.ARPCSessionManager.GetSession(targetSplit[0])
				if ok {
					target.ConnectionStatus = true
					target.AgentVersion = arpcSess.GetVersion()
				}
			} else if target.IsS3 {
				target.ConnectionStatus = true
				target.AgentVersion = "N/A (S3 target)"
			} else {
				target.AgentVersion = "N/A (local target)"

				_, err := os.Stat(target.Path)
				if err != nil {
					target.ConnectionStatus = false
				} else {
					target.ConnectionStatus = utils.IsValid(target.Path)
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
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
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

		if r.FormValue("secret") != "" {
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
