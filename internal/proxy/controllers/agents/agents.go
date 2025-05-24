//go:build linux

package agents

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
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
			return
		}

		authHeader := r.Header.Get("Authorization")
		authHeaderSplit := strings.Split(authHeader, " ")
		if len(authHeaderSplit) != 2 || authHeaderSplit[0] != "Bearer" {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader))
			syslog.L.Error(fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader)).Write()
			return
		}

		tokenStr := authHeaderSplit[1]
		token, err := storeInstance.Database.GetToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("[%s]: token not found", r.RemoteAddr))
			syslog.L.Error(fmt.Errorf("[%s]: token not found", r.RemoteAddr)).Write()
			return
		}

		if token.Revoked {
			w.WriteHeader(http.StatusUnauthorized)
			controllers.WriteErrorResponse(w, fmt.Errorf("[%s]: token already revoked", r.RemoteAddr))
			syslog.L.Error(fmt.Errorf("[%s]: token already revoked", r.RemoteAddr)).Write()
			return
		}

		var reqParsed BootstrapRequest
		err = json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		cert, err := storeInstance.CertGenerator.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
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

		syslog.L.Info().WithMessage("bootstrapping target").WithFields(map[string]interface{}{"target": reqParsed.Hostname}).Write()
		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		for _, drive := range reqParsed.Drives {
			syslog.L.Info().WithMessage("bootstrapping drive").WithFields(map[string]interface{}{"drive": drive}).Write()

			newTarget := types.Target{
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
				OperatingSystem: drive.OperatingSystem,
			}

			switch drive.OperatingSystem {
			case "windows":
				newTarget.Name = fmt.Sprintf("%s - %s", reqParsed.Hostname, drive.Letter)
				newTarget.Path = fmt.Sprintf("agent://%s/%s", clientIP, drive.Letter)
			default:
				newTarget.Name = fmt.Sprintf("%s", reqParsed.Hostname)
				newTarget.Path = fmt.Sprintf("agent://%s/", clientIP)
			}

			err := storeInstance.Database.CreateTarget(tx, newTarget)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}
	}
}

func AgentRenewHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var reqParsed BootstrapRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		existingTarget, err := storeInstance.Database.GetTarget(reqParsed.Hostname + " - C")
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		cert, err := storeInstance.CertGenerator.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
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
			syslog.L.Error(err).Write()
			return
		}

		for _, drive := range reqParsed.Drives {
			newTarget := types.Target{
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
				OperatingSystem: drive.OperatingSystem,
			}

			switch drive.OperatingSystem {
			case "windows":
				newTarget.Name = fmt.Sprintf("%s - %s", reqParsed.Hostname, drive.Letter)
				newTarget.Path = fmt.Sprintf("agent://%s/%s", clientIP, drive.Letter)
			default:
				newTarget.Name = fmt.Sprintf("%s", reqParsed.Hostname)
				newTarget.Path = fmt.Sprintf("agent://%s/", clientIP)
			}

			err := storeInstance.Database.CreateTarget(tx, newTarget)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}
	}
}
