//go:build linux

package targets

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/target"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type NewAgentRequest struct {
	PublicKey string `json:"public_key"`
	BasePath  string `json:"base_path"`
	Hostname  string `json:"hostname"`
}

func D2DTargetHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		if r.Method == http.MethodGet {
			if err := storeInstance.CheckProxyAuth(r); err != nil {
				http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
				return
			}

			all, err := storeInstance.GetAllTargets()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
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

		if r.Method == http.MethodPost {
			var reqParsed NewAgentRequest
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

			agentPubKey, err := target.RegisterAgent(storeInstance, reqParsed.Hostname, clientIP, reqParsed.BasePath)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			err = json.NewEncoder(w).Encode(map[string]string{
				"public_key": string(agentPubKey),
			})

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				return
			}
		}
	}
}

type NewAgentHostnameRequest struct {
	Hostname     string   `json:"hostname"`
	DriveLetters []string `json:"drive_letters"`
}

func D2DTargetAgentHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
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

		existingTargets, err := storeInstance.GetAllTargetsByIP(clientIP)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		for _, target := range existingTargets {
			targetDrive := strings.Split(target.Path, "/")[3]
			if !slices.Contains(reqParsed.DriveLetters, targetDrive) && strings.Contains(target.Name, reqParsed.Hostname) {
				_ = storeInstance.DeleteTarget(target.Name)
			}
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
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
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

		newTarget := store.Target{
			Name: r.FormValue("name"),
			Path: r.FormValue("path"),
		}

		err = storeInstance.CreateTarget(newTarget)
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
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
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

			target, err := storeInstance.GetTarget(r.PathValue("target"))
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

			err = storeInstance.UpdateTarget(*target)
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
			target, err := storeInstance.GetTarget(r.PathValue("target"))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = target
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.DeleteTarget(r.PathValue("target"))
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
