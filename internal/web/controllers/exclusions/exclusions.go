//go:build linux

package exclusions

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

func D2DExclusionHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		if r.Method == http.MethodGet {
			all, err := storeInstance.Database.GetAllGlobalExclusions()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			digest, err := utils.CalculateDigest(all)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			toReturn := ExclusionsResponse{
				Data:   all,
				Digest: digest,
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(toReturn)

			return
		}
	}
}

func ExtJsExclusionHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := ExclusionConfigResponse{}
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

		path := strings.TrimSpace(r.FormValue("path"))
		comment := strings.TrimSpace(r.FormValue("comment"))

		if err := utils.ValidateExclusionPath(path); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		if len(comment) > 1024 {
			controllers.WriteErrorResponse(w, fmt.Errorf("comment exceeds maximum length"))
			return
		}

		newExclusion := database.Exclusion{
			Path:    path,
			Comment: comment,
		}

		err = storeInstance.Database.CreateExclusion(nil, newExclusion)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsExclusionSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := ExclusionConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		pathDecoded, err := url.QueryUnescape(utils.DecodePath(r.PathValue("exclusion")))
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		if err := utils.ValidateExclusionPath(pathDecoded); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			exclusion, err := storeInstance.Database.GetExclusion(pathDecoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			newPath := strings.TrimSpace(r.FormValue("path"))
			newComment := strings.TrimSpace(r.FormValue("comment"))

			if newPath != "" {
				if err := utils.ValidateExclusionPath(newPath); err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}
				exclusion.Path = newPath
			}

			if newComment != "" {
				if len(newComment) > 1024 {
					controllers.WriteErrorResponse(w, fmt.Errorf("comment exceeds maximum length"))
					return
				}
				exclusion.Comment = newComment
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "path":
						exclusion.Path = ""
					case "comment":
						exclusion.Comment = ""
					}
				}
			}

			err = storeInstance.Database.UpdateExclusion(nil, *exclusion)
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
			exclusion, err := storeInstance.Database.GetExclusion(pathDecoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = exclusion
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err = storeInstance.Database.DeleteExclusion(nil, pathDecoded)
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
