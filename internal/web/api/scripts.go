//go:build linux

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DScriptHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		all, err := storeInstance.Database.GetAllScripts()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		digest, err := calculateDigest(all)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := ScriptsResponse{
			Data:   all,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)

		return
	}
}

func ExtJsScriptHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := ScriptConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		scriptValue := r.FormValue("script")
		if !validate.IsValidShellScriptWithShebang(scriptValue) {
			WriteErrorResponse(w, errors.New("invalid script, no shebang detected"))
			return
		}

		path, err := validate.SaveScriptToFile(scriptValue)
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("failed to save script to file: %w", err))
			return
		}

		newScript := database.Script{
			Path:        path,
			Description: r.FormValue("description"),
		}

		err = storeInstance.Database.CreateScript(nil, newScript)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsScriptSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := ScriptConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		currentPath := validate.DecodePath(r.PathValue("path"))

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if currentPath == "" {
				WriteErrorResponse(w, errors.New("path is empty"))
			}

			scriptValue := r.FormValue("script")
			if !validate.IsValidShellScriptWithShebang(scriptValue) {
				WriteErrorResponse(w, errors.New("invalid script, no shebang detected"))
				return
			}

			script, err := storeInstance.Database.GetScript(currentPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			err = validate.UpdateScriptContentToFile(script.Path, scriptValue)
			if err != nil {
				WriteErrorResponse(w, fmt.Errorf("failed to save script to file: %w", err))
				return
			}

			script.Description = r.FormValue("description")

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "description":
						script.Description = ""
					}
				}
			}

			err = storeInstance.Database.UpdateScript(nil, script)
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
			script, err := storeInstance.Database.GetScript(currentPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			scriptContent, err := validate.ReadScriptContentFromFile(currentPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			script.Script = scriptContent

			response.Status = http.StatusOK
			response.Success = true
			response.Data = script
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.Database.DeleteScript(nil, currentPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			_ = os.Remove(currentPath)

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}
