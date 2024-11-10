//go:build linux

package proxy

import (
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func ExtractTokenFromRequest(r *http.Request, storeInstance *store.Store) *store.Token {
	syslogger, err := syslog.InitializeLogger()
	if err != nil {
		log.Println(err)
		return nil
	}

	if r == nil {
		return nil
	}

	token := store.Token{}

	pbsAuthCookie, err := r.Cookie("PBSAuthCookie")
	if err != nil {
		return nil
	}
	decodedAuthCookie := strings.ReplaceAll(pbsAuthCookie.Value, "%3A", ":")
	cookieSplit := strings.Split(decodedAuthCookie, ":")
	if len(cookieSplit) < 5 {
		syslogger.Errorf("ExtractTokenFromRequest: error invalid cookie, less than 5 split")
		return nil
	}

	token.CSRFToken = r.Header.Get("csrfpreventiontoken")
	token.Ticket = decodedAuthCookie
	token.Username = cookieSplit[1]

	storeInstance.LastToken = &token

	if !utils.IsValid(filepath.Join(store.DbBasePath, "pbs-d2d-token.json")) {
		apiToken, err := storeInstance.CreateAPIToken()
		if err != nil {
			syslogger.Errorf("ExtractTokenFromRequest: error creating API token -> %v", err)
			return nil
		}

		storeInstance.APIToken = apiToken

		err = apiToken.SaveToFile()
		if err != nil {
			syslogger.Errorf("ExtractTokenFromRequest: error saving API token to file -> %v", err)
			return nil
		}
	}

	return &token
}
