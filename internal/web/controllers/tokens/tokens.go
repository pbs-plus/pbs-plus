//go:build linux

package tokens

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

func D2DTokenHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		all, err := storeInstance.Database.GetAllTokens(false)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		digest, err := utils.CalculateDigest(all)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		toReturn := TokensResponse{
			Data:   all,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)

		return
	}
}

func ExtJsTokenHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TokenConfigResponse{}
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

		newToken := database.AgentToken{
			Comment: r.FormValue("comment"),
		}

		duration := time.Hour * 24
		durationStr := strings.TrimSpace(r.FormValue("duration"))
		if durationStr != "" {
			duration, err = time.ParseDuration(durationStr)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}
			if duration < 0 {
				controllers.WriteErrorResponse(w, errors.New("duration value can only be > 0"))
			}
		}

		err = storeInstance.Database.CreateToken(duration, newToken.Comment)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsTokenSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TokenConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			token, err := storeInstance.Database.GetToken(utils.DecodePath(r.PathValue("token")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = token
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			token, err := storeInstance.Database.GetToken(utils.DecodePath(r.PathValue("token")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			err = storeInstance.Database.RevokeToken(token)
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
