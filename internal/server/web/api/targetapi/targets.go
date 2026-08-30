//go:build linux

package targetapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

func D2DTargetHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		all, err := app.Target.GetAllTargets()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		for i := range all {
			switch {
			case all[i].IsS3():
				all[i].ConnectionStatus = true
				all[i].AgentVersion = "N/A (S3 target)"
			case all[i].IsLocal():
				all[i].AgentVersion = "N/A (local target)"
				_, err := os.Stat(all[i].Path)
				all[i].ConnectionStatus = err == nil && validate.IsValid(all[i].Path)
			case all[i].IsAgent():
				if qSess, ok := app.Agents.GetQuicPipe(all[i].GetHostname()); ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = qSess.GetVersion()
				} else if tSess, ok := app.Agents.GetStreamPipe(all[i].GetHostname()); ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = tSess.GetVersion()
				}
			default:
				all[i].AgentVersion = "N/A"
			}
		}

		digest, err := digest.Calculate(all)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		data := make([]targetResponse, len(all))
		for i := range all {
			data[i] = newTargetResponse(all[i])
		}

		toReturn := TargetsResponse{
			Data:    data,
			Digest:  digest,
			Success: true,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			log.Error(err, "")
		}
	}
}

func D2DTargetStatusHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// Trigger async refresh if requested
		if strings.ToLower(r.FormValue("refresh")) == "true" {
			app.Target.RefreshStatuses()
		}

		cached := app.Target.GetCachedStatuses()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cached); err != nil {
			log.Error(err, "")
		}
	}
}

type NewAgentHostnameRequest struct {
	Hostname        string             `json:"hostname"`
	Drives          []fswire.DriveInfo `json:"drives"`
	OperatingSystem string             `json:"os"`
}

func D2DTargetAgentHandler(app *application.Runtime) http.HandlerFunc {
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
			respond.WriteErrorResponse(w, fmt.Errorf("Failed to parse request body: %w", err))
			return
		}

		if authHostname != "" && authHostname != reqParsed.Hostname {
			w.WriteHeader(http.StatusForbidden)
			hostnameErr := fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, reqParsed.Hostname)
			respond.WriteErrorResponse(w, hostnameErr)
			return
		}

		if reqParsed.Hostname == "" {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, fmt.Errorf("Hostname is required in request body"))
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

		tx, err := app.Target.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, fmt.Errorf("Failed to start transaction: %w", err))
			return
		}
		defer func() {
			if tx != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
			}
		}()

		for _, parsedDrive := range reqParsed.Drives {
			targetName := coredb.GetAgentTargetName(reqParsed.Hostname, parsedDrive.Letter, reqParsed.OperatingSystem)

			targetData := coredb.Target{
				Name:             targetName,
				Type:             coredb.TargetTypeFilesystem,
				Access:           coredb.FilesystemAccessAgent,
				AgentHost:        coredb.AgentHost{Name: reqParsed.Hostname},
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

			err = app.Target.UpsertTarget(tx, targetData)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				respond.WriteErrorResponse(w, fmt.Errorf("Failed to upsert target %s: %w", targetName, err))
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, fmt.Errorf("Failed to commit transaction: %w", err))
			return
		}
		tx = nil

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]bool{
			"success": true,
		})

		if err != nil {
			log.Error(err, "failed to encode success response")
		}
	}
}

func ExtJsTargetHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		newTarget := coredb.Target{
			Name:        r.FormValue("name"),
			Type:        targetTypeFromRequest(r),
			Access:      coredb.FilesystemAccess(r.FormValue("access")),
			Path:        r.FormValue("path"),
			MountScript: r.FormValue("mount_script"),
		}

		err = app.Target.CreateTarget(nil, newTarget)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsTargetSingleHandler(app *application.Runtime) http.HandlerFunc {
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
				respond.WriteErrorResponse(w, err)
				return
			}

			path := r.FormValue("path")
			if path != "" {
				_, s3Err := coredb.ParseS3Url(path)
				if !validate.IsValid(path) && s3Err != nil {
					respond.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", path))
					return
				}
			}

			target, err := app.Target.GetTarget(validate.DecodePath(r.PathValue("target")))
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("name") != "" {
				target.Name = r.FormValue("name")
			}
			if path != "" {
				target.Path = path
			}
			if targetType := targetTypeFromRequest(r); targetType != "" {
				target.Type = targetType
			}
			if access := r.FormValue("access"); access != "" {
				target.Access = coredb.FilesystemAccess(access)
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

			err = app.Target.UpdateTarget(nil, target)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodGet {
			target, err := app.Target.GetTarget(validate.DecodePath(r.PathValue("target")))
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			switch {
			case target.IsAgent():
				arpcSess, ok := app.Agents.GetStreamPipe(target.GetHostname())
				if ok {
					target.AgentVersion = arpcSess.GetVersion()
					target.ConnectionStatus = false

					if strings.ToLower(r.FormValue("status")) == "true" {
						respMsg, err := arpcSess.CallMessage(
							r.Context(),
							"target_status",
							&fswire.TargetStatusReq{Drive: target.VolumeID},
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
			case target.IsS3():
				target.ConnectionStatus = true
				target.AgentVersion = "N/A (S3 target)"
			case target.IsLocal():
				target.AgentVersion = "N/A (local target)"
				_, err := os.Stat(target.Path)
				target.ConnectionStatus = err == nil && validate.IsValid(target.Path)
			default:
				target.AgentVersion = "N/A"
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = newTargetResponse(target)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodDelete {
			err := app.Target.DeleteTarget(nil, validate.DecodePath(r.PathValue("target")))
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
		}
	}
}

func ExtJsTargetS3SecretHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		target, err := app.Target.GetTarget(validate.DecodePath(r.PathValue("target")))
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		if r.FormValue("secret") == "" {
			respond.WriteErrorResponse(w, errors.New("invalid empty secret"))
			return
		}

		err = app.Target.AddS3Secret(target.Name, r.FormValue("secret"))
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

type TargetsResponse struct {
	Data    []targetResponse `json:"data"`
	Digest  string           `json:"digest"`
	Success bool             `json:"success"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    targetResponse    `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type targetResponse struct {
	coredb.Target
	TargetType string `json:"target_type"`
	Kind       string `json:"kind"`
}

func newTargetResponse(target coredb.Target) targetResponse {
	return targetResponse{
		Target:     target,
		TargetType: target.LegacyType(),
		Kind:       string(target.Type),
	}
}

func targetTypeFromRequest(r *http.Request) coredb.TargetType {
	if kind := r.FormValue("kind"); kind != "" {
		return coredb.TargetType(kind)
	}
	return coredb.TargetType(r.FormValue("target_type"))
}
