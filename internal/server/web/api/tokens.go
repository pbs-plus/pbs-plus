//go:build linux

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DTokenHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		all, err := app.Token.GetAllTokens()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		digest, err := calculateDigest(all)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := TokensResponse{
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

func ExtJsTokenHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TokenConfigResponse{}
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

		newToken := coredb.AgentToken{
			Comment: r.FormValue("comment"),
		}

		duration := time.Hour * 24
		durationStr := strings.TrimSpace(r.FormValue("duration"))
		if durationStr != "" {
			duration, err = time.ParseDuration(durationStr)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if duration < 0 {
				WriteErrorResponse(w, errors.New("duration value can only be > 0"))
			}
		}

		err = app.Token.CreateToken(duration, newToken.Comment)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsTokenSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TokenConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			token, err := app.Token.GetToken(validate.DecodePath(r.PathValue("token")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = token
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodDelete {
			token, err := app.Token.GetToken(validate.DecodePath(r.PathValue("token")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			err = app.Token.RevokeToken(token)
			if err != nil {
				WriteErrorResponse(w, err)
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
