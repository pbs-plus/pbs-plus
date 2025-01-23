//go:build linux

package plus

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type TokenRequest struct {
	PBSAuthCookie string `json:"pbs_auth_cookie"`
}

func TokenHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		tokenReq := &TokenRequest{}
		err := json.NewDecoder(r.Body).Decode(tokenReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		decodedAuthCookie := strings.ReplaceAll(tokenReq.PBSAuthCookie, "%3A", ":")
		cookieSplit := strings.Split(decodedAuthCookie, ":")
		if len(cookieSplit) < 5 {
			http.Error(w, "ExtractTokenFromRequest: error invalid cookie, less than 5 split", http.StatusBadRequest)
			return
		}

		token := proxmox.Token{}

		token.CSRFToken = r.Header.Get("csrfpreventiontoken")
		token.Ticket = decodedAuthCookie
		token.Username = cookieSplit[1]

		proxmox.Session.LastToken = &token

		if !utils.IsValid(filepath.Join(constants.DbBasePath, "pbs-plus-token.json")) {
			apiToken, err := proxmox.Session.CreateAPIToken()
			if err != nil {
				http.Error(w, fmt.Sprintf("ExtractTokenFromRequest: error creating API token -> %v", err), http.StatusInternalServerError)
				return
			}

			proxmox.Session.APIToken = apiToken

			err = apiToken.SaveToFile()
			if err != nil {
				http.Error(w, fmt.Sprintf("ExtractTokenFromRequest: error saving API token to file -> %v", err), http.StatusInternalServerError)
				return
			}
		}
	}
}
