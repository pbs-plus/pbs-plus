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
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func AgentLogHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		err := syslog.ParseAndLogWindowsEntry(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
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
	Hostname string            `json:"hostname"`
	CSR      string            `json:"csr"`
	Drives   []utils.DriveInfo `json:"drives"`
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
			controllers.WriteErrorResponse(w, fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader))
			return
		}

		tokenStr := authHeaderSplit[1]
		token, err := storeInstance.Database.GetToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("[%s]: token not found", r.RemoteAddr))
			return
		}

		if token.Revoked {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("[%s]: token already revoked", r.RemoteAddr))
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

		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
		}

		for _, drive := range reqParsed.Drives {
			newTarget := types.Target{
				Name:            fmt.Sprintf("%s - %s", reqParsed.Hostname, drive.Letter),
				Path:            fmt.Sprintf("agent://%s/%s", clientIP, drive.Letter),
				Auth:            encodedCert,
				TokenUsed:       tokenStr,
				DriveType:       drive.Type,
				DriveFS:         drive.FileSystem,
				DriveFreeBytes:  int(drive.FreeBytes),
				DriveUsedBytes:  int(drive.UsedBytes),
				DriveTotalBytes: int(drive.TotalBytes),
				DriveFree:       drive.Free,
				DriveUsed:       drive.Used,
				DriveTotal:      drive.Total,
			}

			err := storeInstance.Database.CreateTarget(tx, newTarget)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
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
		if err != nil {
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

		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
		}

		for _, drive := range reqParsed.Drives {
			newTarget := types.Target{
				Name:            fmt.Sprintf("%s - %s", reqParsed.Hostname, drive.Letter),
				Path:            fmt.Sprintf("agent://%s/%s", clientIP, drive.Letter),
				Auth:            encodedCert,
				TokenUsed:       existingTarget.TokenUsed,
				DriveType:       drive.Type,
				DriveFS:         drive.FileSystem,
				DriveFreeBytes:  int(drive.FreeBytes),
				DriveUsedBytes:  int(drive.UsedBytes),
				DriveTotalBytes: int(drive.TotalBytes),
				DriveFree:       drive.Free,
				DriveUsed:       drive.Used,
				DriveTotal:      drive.Total,
			}

			err := storeInstance.Database.CreateTarget(tx, newTarget)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
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
