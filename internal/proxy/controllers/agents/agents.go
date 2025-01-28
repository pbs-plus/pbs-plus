//go:build linux

package agents

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

type LogRequest struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
	Level    string `json:"level"`
}

func AgentLogHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		var reqParsed LogRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		switch reqParsed.Level {
		case "info":
			syslog.L.Infof("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		case "error":
			syslog.L.Errorf("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		case "warn":
			syslog.L.Warnf("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		default:
			syslog.L.Infof("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"success": "true"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}
	}
}

type BootstrapRequest struct {
	Hostname string   `json:"hostname"`
	CSR      string   `json:"csr"`
	Drives   []string `json:"drives"`
}

func AgentBootstrapHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		authHeader := r.Header.Get("Authorization")
		authHeaderSplit := strings.Split(authHeader, " ")
		if len(authHeaderSplit) != 2 || authHeaderSplit[0] != "Bearer" {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("unauthorized bearer access: %s", authHeader))
			return
		}

		tokenStr := authHeaderSplit[1]
		token, err := storeInstance.Database.GetToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("token not found"))
			return
		}

		if token.Revoked {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("token already revoked"))
			return
		}

		var reqParsed BootstrapRequest
		err = json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		cert, err := storeInstance.CertGenerator.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		encodedCert := base64.StdEncoding.EncodeToString(cert)
		encodedCA := base64.StdEncoding.EncodeToString(storeInstance.CertGenerator.GetCAPEM())

		clientIP := r.RemoteAddr

		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			clientIP = forwarded
		}

		clientIP = strings.Split(clientIP, ":")[0]

		for _, drive := range reqParsed.Drives {
			newTarget := types.Target{
				Name:      fmt.Sprintf("%s - %s", reqParsed.Hostname, drive),
				Path:      fmt.Sprintf("agent://%s/%s", clientIP, drive),
				Auth:      encodedCert,
				TokenUsed: tokenStr,
			}

			err := storeInstance.Database.CreateTarget(newTarget)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}
	}
}

func AgentRenewHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		var reqParsed BootstrapRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		existingTarget, err := storeInstance.Database.GetTarget(reqParsed.Hostname + " - C")
		if err != nil || existingTarget == nil {
			w.WriteHeader(http.StatusNotFound)
			controllers.WriteErrorResponse(w, err)
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		cert, err := storeInstance.CertGenerator.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		encodedCert := base64.StdEncoding.EncodeToString(cert)
		encodedCA := base64.StdEncoding.EncodeToString(storeInstance.CertGenerator.GetCAPEM())

		clientIP := r.RemoteAddr

		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			clientIP = forwarded
		}

		clientIP = strings.Split(clientIP, ":")[0]

		for _, drive := range reqParsed.Drives {
			newTarget := types.Target{
				Name:      fmt.Sprintf("%s - %s", reqParsed.Hostname, drive),
				Path:      fmt.Sprintf("agent://%s/%s", clientIP, drive),
				Auth:      encodedCert,
				TokenUsed: existingTarget.TokenUsed,
			}

			err := storeInstance.Database.CreateTarget(newTarget)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}
	}
}
