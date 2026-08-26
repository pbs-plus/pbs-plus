//go:build linux

package scriptapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DScriptHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		all, err := app.Script.GetAllScripts()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		digest, err := digest.Calculate(all)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		toReturn := ScriptsResponse{
			Data:   all,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			log.Error(err, "")
		}

		return
	}
}

func ExtJsScriptHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := ScriptConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		scriptValue := r.FormValue("script")
		if !validate.IsValidShellScriptWithShebang(scriptValue) {
			respond.WriteErrorResponse(w, errors.New("invalid script, no shebang detected"))
			return
		}

		path, err := validate.SaveScriptToFile(scriptValue)
		if err != nil {
			respond.WriteErrorResponse(w, fmt.Errorf("failed to save script to file: %w", err))
			return
		}

		newScript := coredb.Script{
			Path:        path,
			Description: r.FormValue("description"),
		}

		err = app.Script.CreateScript(newScript)
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

func ExtJsScriptSingleHandler(app *application.Runtime) http.HandlerFunc {
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
				respond.WriteErrorResponse(w, err)
				return
			}

			if currentPath == "" {
				respond.WriteErrorResponse(w, errors.New("path is empty"))
			}

			scriptValue := r.FormValue("script")
			if !validate.IsValidShellScriptWithShebang(scriptValue) {
				respond.WriteErrorResponse(w, errors.New("invalid script, no shebang detected"))
				return
			}

			script, err := app.Script.GetScript(currentPath)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			err = validate.UpdateScriptContentToFile(script.Path, scriptValue)
			if err != nil {
				respond.WriteErrorResponse(w, fmt.Errorf("failed to save script to file: %w", err))
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

			err = app.Script.UpdateScript(script)
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
			script, err := app.Script.GetScript(currentPath)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			scriptContent, err := validate.ReadScriptContentFromFile(currentPath)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			script.Script = scriptContent

			response.Status = http.StatusOK
			response.Success = true
			response.Data = script
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodDelete {
			err := app.Script.DeleteScript(currentPath)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := os.Remove(currentPath); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
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

type ScriptsResponse struct {
	Data   []coredb.Script `json:"data"`
	Digest string          `json:"digest"`
}

type ScriptConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    coredb.Script     `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
