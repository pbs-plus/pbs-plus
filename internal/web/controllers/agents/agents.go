//go:build linux

package agents

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

func AgentLogHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
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
	Hostname        string            `json:"hostname"`
	OperatingSystem string            `json:"os"`
	CSR             string            `json:"csr"`
	Drives          []utils.DriveInfo `json:"drives"`
}

func AgentBootstrapHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
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
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			syslog.L.Error(fmt.Errorf("no drives provided")).Write()
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		cert, ca, err := storeInstance.SignAgentCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		encodedCert := base64.StdEncoding.EncodeToString(cert)
		encodedCA := base64.StdEncoding.EncodeToString(ca)

		clientIP := r.RemoteAddr

		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			clientIP = forwarded
		}

		clientIP = strings.Split(clientIP, ":")[0]

		syslog.L.Info().WithMessage("bootstrapping target").WithFields(map[string]any{"target": reqParsed.Hostname}).Write()
		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		_, err = storeInstance.Database.GetTarget(reqParsed.Hostname)
		if err == nil {
			err := storeInstance.Database.DeleteTarget(tx, reqParsed.Hostname)
			if err != nil {
				tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				controllers.WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}
		}

		newTarget := types.Target{
			Name:            reqParsed.Hostname,
			AgentHost:       clientIP,
			Auth:            encodedCert,
			TokenUsed:       tokenStr,
			Volumes:         []types.Volume{},
			OperatingSystem: reqParsed.OperatingSystem,
			TargetType:      "agent",
		}

		for _, drive := range reqParsed.Drives {
			newTarget.Volumes = append(newTarget.Volumes, types.Volume{
				VolumeName:     drive.Letter,
				MetaType:       drive.Type,
				MetaFS:         drive.FileSystem,
				MetaFreeBytes:  int(drive.FreeBytes),
				MetaUsedBytes:  int(drive.UsedBytes),
				MetaTotalBytes: int(drive.TotalBytes),
				MetaFree:       drive.Free,
				MetaUsed:       drive.Used,
				MetaTotal:      drive.Total,
			})
		}

		err = storeInstance.Database.CreateTarget(tx, newTarget)
		if err != nil {
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
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
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		var reqParsed BootstrapRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			syslog.L.Error(fmt.Errorf("no drives provided")).Write()
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		cert, ca, err := storeInstance.SignAgentCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		encodedCert := base64.StdEncoding.EncodeToString(cert)
		encodedCA := base64.StdEncoding.EncodeToString(ca)

		clientIP := r.RemoteAddr
		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			clientIP = forwarded
		}
		clientIP = strings.Split(clientIP, ":")[0]

		syslog.L.Info().WithMessage("renewing target certificates").WithFields(map[string]any{"target": reqParsed.Hostname}).Write()

		tx, err := storeInstance.Database.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		existingTarget, err := storeInstance.Database.GetTarget(reqParsed.Hostname)
		if err != nil {
			syslog.L.Warn().WithMessage("target not found during renewal, skipping").WithFields(map[string]any{"target": reqParsed.Hostname}).Write()
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		newTarget := types.Target{
			Name:            reqParsed.Hostname,
			AgentHost:       clientIP,
			Auth:            encodedCert,
			TokenUsed:       existingTarget.Name,
			Volumes:         []types.Volume{},
			OperatingSystem: reqParsed.OperatingSystem,
			TargetType:      "agent",
		}

		for _, drive := range reqParsed.Drives {
			newTarget.Volumes = append(newTarget.Volumes, types.Volume{
				VolumeName:     drive.Letter,
				MetaType:       drive.Type,
				MetaFS:         drive.FileSystem,
				MetaFreeBytes:  int(drive.FreeBytes),
				MetaUsedBytes:  int(drive.UsedBytes),
				MetaTotalBytes: int(drive.TotalBytes),
				MetaFree:       drive.Free,
				MetaUsed:       drive.Used,
				MetaTotal:      drive.Total,
			})
		}

		err = storeInstance.Database.UpdateTarget(tx, newTarget)
		if err != nil {
			syslog.L.Warn().WithMessage("error updating target").WithFields(map[string]any{"target": reqParsed.Hostname}).Write()
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		syslog.L.Info().WithMessage("renewed target certificate").WithFields(map[string]any{"target": reqParsed.Hostname}).Write()

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
