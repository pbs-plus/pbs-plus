//go:build linux

package targets

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/target"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type NewAgentRequest struct {
	PublicKey string `json:"public_key"`
	BasePath  string `json:"base_path"`
	Hostname  string `json:"hostname"`
}

func D2DTargetHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if r.Method == http.MethodGet {
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

			proxy.ExtractTokenFromRequest(r, storeInstance)

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

func D2DTargetAgentHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if err := store.CheckAgentAuth(r); err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
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

func ExtJsTargetHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
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

		proxy.ExtractTokenFromRequest(r, storeInstance)

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsTargetSingleHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
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

			target, err := storeInstance.GetTarget(pathVar["target"])
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

			proxy.ExtractTokenFromRequest(r, storeInstance)

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			target, err := storeInstance.GetTarget(pathVar["target"])
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			proxy.ExtractTokenFromRequest(r, storeInstance)

			response.Status = http.StatusOK
			response.Success = true
			response.Data = target
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.DeleteTarget(pathVar["target"])
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			proxy.ExtractTokenFromRequest(r, storeInstance)

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}
