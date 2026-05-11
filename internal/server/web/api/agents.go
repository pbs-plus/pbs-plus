//go:build linux

package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
			WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"success": "true"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			return
		}
	}
}

type BootstrapRequest struct {
	Hostname        string            `json:"hostname"`
	CSR             string            `json:"csr"`
	OperatingSystem string            `json:"os"`
	Drives          []types.DriveInfo `json:"drives"`
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
			WriteErrorResponse(w, fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader))
			syslog.L.Error(fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader)).Write()
			return
		}

		tokenStr := authHeaderSplit[1]
		token, err := storeInstance.TokenSvc.GetToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			WriteErrorResponse(w, fmt.Errorf("[%s]: token not found", r.RemoteAddr))
			syslog.L.Error(fmt.Errorf("[%s]: token not found", r.RemoteAddr)).Write()
			return
		}

		if token.Revoked {
			w.WriteHeader(http.StatusUnauthorized)
			WriteErrorResponse(w, fmt.Errorf("[%s]: token already revoked", r.RemoteAddr))
			syslog.L.Error(fmt.Errorf("[%s]: token already revoked", r.RemoteAddr)).Write()
			return
		}

		var reqParsed BootstrapRequest
		err = json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			syslog.L.Error(fmt.Errorf("no drives provided")).Write()
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		cert, ca, err := storeInstance.CertManager.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
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

		syslog.L.Info().WithMessage("bootstrapping target").WithFields(map[string]any{"target": reqParsed.Hostname, "clientIP": clientIP, "drives": reqParsed.Drives}).Write()
		tx, err := storeInstance.TargetSvc.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		host := database.AgentHost{
			Name:            reqParsed.Hostname,
			IP:              clientIP,
			Auth:            encodedCert,
			TokenUsed:       tokenStr,
			OperatingSystem: reqParsed.OperatingSystem,
		}

		_, err = storeInstance.AgentHostSvc.GetAgentHost(reqParsed.Hostname)
		if err == nil {
			syslog.L.Info().WithMessage("updating host target details").WithFields(map[string]any{"target": reqParsed.Hostname, "clientIP": clientIP, "drives": reqParsed.Drives}).Write()
			err = storeInstance.AgentHostSvc.UpdateAgentHost(tx, host)
			if err != nil {
				_ = tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}
		} else {
			syslog.L.Info().WithMessage("creating new host target").WithFields(map[string]any{"target": reqParsed.Hostname, "clientIP": clientIP, "drives": reqParsed.Drives, "error": err.Error()}).Write()
			err = storeInstance.AgentHostSvc.CreateAgentHost(tx, host)
			if err != nil {
				_ = tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}
		}

		for _, drive := range reqParsed.Drives {
			syslog.L.Info().WithMessage("bootstrapping drive").WithFields(map[string]any{"drive": drive}).Write()

			newTarget := database.Target{
				AgentHost:        database.AgentHost{Name: reqParsed.Hostname},
				VolumeID:         drive.Letter,
				VolumeType:       drive.Type,
				VolumeFS:         drive.FileSystem,
				VolumeFreeBytes:  int(drive.FreeBytes),
				VolumeUsedBytes:  int(drive.UsedBytes),
				VolumeTotalBytes: int(drive.TotalBytes),
				VolumeFree:       drive.Free,
				VolumeUsed:       drive.Used,
				VolumeTotal:      drive.Total,
				VolumeName:       drive.VolumeName,
			}

			newTarget.Name = database.GetAgentTargetName(reqParsed.Hostname, drive.Letter, reqParsed.OperatingSystem)

			existingTarget, err := storeInstance.TargetSvc.GetTarget(newTarget.Name)
			if err == nil {
				newTarget.JobCount = existingTarget.JobCount
				newTarget.AgentVersion = existingTarget.AgentVersion
				newTarget.ConnectionStatus = existingTarget.ConnectionStatus

				err := storeInstance.TargetSvc.DeleteTarget(tx, newTarget.Name)
				if err != nil {
					_ = tx.Rollback()
					w.WriteHeader(http.StatusInternalServerError)
					WriteErrorResponse(w, err)
					syslog.L.Error(err).Write()
					return
				}

				err = storeInstance.TargetSvc.CreateTarget(tx, newTarget)
				if err != nil {
					_ = tx.Rollback()
					w.WriteHeader(http.StatusInternalServerError)
					WriteErrorResponse(w, err)
					syslog.L.Error(err).Write()
					return
				}
				syslog.L.Info().WithMessage("updated existing target auth").WithFields(map[string]any{"target": newTarget.Name}).Write()
			} else {
				err := storeInstance.TargetSvc.CreateTarget(tx, newTarget)
				if err != nil {
					_ = tx.Rollback()
					w.WriteHeader(http.StatusInternalServerError)
					WriteErrorResponse(w, err)
					syslog.L.Error(err).Write()
					return
				}
				syslog.L.Info().WithMessage("created new target").WithFields(map[string]any{"target": newTarget.Name}).Write()
			}
		}

		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
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
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		authHostname := r.Header.Get("X-PBS-Authenticated-Agent")
		if authHostname != "" && authHostname != reqParsed.Hostname {
			w.WriteHeader(http.StatusForbidden)
			hostnameErr := fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, reqParsed.Hostname)
			WriteErrorResponse(w, hostnameErr)
			syslog.L.Error(hostnameErr).Write()
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			syslog.L.Error(fmt.Errorf("no drives provided")).Write()
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		cert, ca, err := storeInstance.CertManager.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
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

		tx, err := storeInstance.TargetSvc.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		currentHost, err := storeInstance.AgentHostSvc.GetAgentHost(reqParsed.Hostname)
		if err != nil {
			_ = tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		host := database.AgentHost{
			Name:            reqParsed.Hostname,
			IP:              clientIP,
			Auth:            encodedCert,
			TokenUsed:       currentHost.TokenUsed,
			OperatingSystem: reqParsed.OperatingSystem,
		}

		err = storeInstance.AgentHostSvc.UpdateAgentHost(tx, host)
		if err != nil {
			_ = tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		for _, drive := range reqParsed.Drives {
			targetName := database.GetAgentTargetName(reqParsed.Hostname, drive.Letter, reqParsed.OperatingSystem)

			existingTarget, err := storeInstance.TargetSvc.GetTarget(targetName)
			if err != nil {
				syslog.L.Warn().WithMessage("target not found during renewal, skipping").WithFields(map[string]any{"target": targetName}).Write()
				continue
			}

			updatedTarget := database.Target{
				Name:             targetName,
				AgentHost:        database.AgentHost{Name: reqParsed.Hostname},
				VolumeID:         drive.Letter,
				VolumeType:       drive.Type,
				VolumeFS:         drive.FileSystem,
				VolumeFreeBytes:  int(drive.FreeBytes),
				VolumeUsedBytes:  int(drive.UsedBytes),
				VolumeTotalBytes: int(drive.TotalBytes),
				VolumeFree:       drive.Free,
				VolumeUsed:       drive.Used,
				VolumeTotal:      drive.Total,
				VolumeName:       drive.VolumeName,
				// Preserve existing operational data
				JobCount:         existingTarget.JobCount,
				AgentVersion:     existingTarget.AgentVersion,
				ConnectionStatus: existingTarget.ConnectionStatus,
				MountScript:      existingTarget.MountScript,
			}

			err = storeInstance.TargetSvc.DeleteTarget(tx, targetName)
			if err != nil {
				_ = tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}

			err = storeInstance.TargetSvc.CreateTarget(tx, updatedTarget)
			if err != nil {
				_ = tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				syslog.L.Error(err).Write()
				return
			}

			syslog.L.Info().WithMessage("renewed target certificate").WithFields(map[string]any{"target": targetName}).Write()
		}

		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			syslog.L.Error(err).Write()
			return
		}
	}
}
