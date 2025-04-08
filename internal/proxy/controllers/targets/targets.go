//go:build linux

package targets

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

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

		return
	}
}

type NewAgentHostnameRequest struct {
	Hostname string            `json:"hostname"`
	Drives   []utils.DriveInfo `json:"drives"`
}

func D2DTargetAgentHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var reqParsed NewAgentHostnameRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		clientIP := r.RemoteAddr

		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			clientIP = forwarded
		}

		clientIP = strings.Split(clientIP, ":")[0]

		existingTargets, err := storeInstance.Database.GetAllTargetsByIP(clientIP)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		if len(existingTargets) == 0 {
			w.WriteHeader(http.StatusNotFound)
			controllers.WriteErrorResponse(w, fmt.Errorf("No targets found."))
			return
		}

		targetTemplate := existingTargets[0]

		hostname := r.Header.Get("X-PBS-Agent")

		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
		}

		err = storeInstance.Database.DeleteTargetsByIP(tx, clientIP)
		if err != nil {
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
		}

		var driveLetters = make([]string, len(reqParsed.Drives))
		for i, parsedDrive := range reqParsed.Drives {
			driveLetters[i] = parsedDrive.Letter

			_ = storeInstance.Database.CreateTarget(tx, types.Target{
				Name:            hostname + " - " + parsedDrive.Letter,
				Path:            "agent://" + clientIP + "/" + parsedDrive.Letter,
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
			})
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]bool{
			"success": true,
		})

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
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

		if !utils.IsValid(r.FormValue("path")) {
			controllers.WriteErrorResponse(w, fmt.Errorf("invalid path '%s'", r.FormValue("path")))
			return
		}

		newTarget := types.Target{
			Name: r.FormValue("name"),
			Path: r.FormValue("path"),
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

			if !utils.IsValid(r.FormValue("path")) {
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

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						target.Name = ""
					case "path":
						target.Path = ""
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
