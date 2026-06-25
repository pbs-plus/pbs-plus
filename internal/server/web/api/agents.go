//go:build linux

package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

func AgentLogHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		err := log.ParseAndLogWindowsEntry(r.Body)
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
			log.Error(fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader), "")
			return
		}

		tokenStr := authHeaderSplit[1]
		token, err := storeInstance.TokenSvc.GetToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			WriteErrorResponse(w, fmt.Errorf("[%s]: token not found", r.RemoteAddr))
			log.Error(fmt.Errorf("[%s]: token not found", r.RemoteAddr), "")
			return
		}

		if token.Revoked {
			w.WriteHeader(http.StatusUnauthorized)
			WriteErrorResponse(w, fmt.Errorf("[%s]: token already revoked", r.RemoteAddr))
			log.Error(fmt.Errorf("[%s]: token already revoked", r.RemoteAddr), "")
			return
		}

		var reqParsed BootstrapRequest
		err = json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			log.Error(fmt.Errorf("no drives provided"), "")
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		cert, ca, err := storeInstance.CertManager.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
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
		log.Info("bootstrapping target")
		tx, err := storeInstance.TargetSvc.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
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
			log.Info("updating host target details")
			err = storeInstance.AgentHostSvc.UpdateAgentHost(tx, host)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}
		} else {
			log.Info("creating new host target")
			err = storeInstance.AgentHostSvc.CreateAgentHost(tx, host)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}
		}

		for _, drive := range reqParsed.Drives {
			log.Info("bootstrapping drive")

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
					if err := tx.Rollback(); err != nil {
						log.Error(err, "")
					}
					w.WriteHeader(http.StatusInternalServerError)
					WriteErrorResponse(w, err)
					log.Error(err, "")
					return
				}

				err = storeInstance.TargetSvc.CreateTarget(tx, newTarget)
				if err != nil {
					if err := tx.Rollback(); err != nil {
						log.Error(err, "")
					}
					w.WriteHeader(http.StatusInternalServerError)
					WriteErrorResponse(w, err)
					log.Error(err, "")
					return
				}
				log.Info("updated existing target auth")
			} else {
				err := storeInstance.TargetSvc.CreateTarget(tx, newTarget)
				if err != nil {
					if err := tx.Rollback(); err != nil {
						log.Error(err, "")
					}
					w.WriteHeader(http.StatusInternalServerError)
					WriteErrorResponse(w, err)
					log.Error(err, "")
					return
				}
				log.Info("created new target")
			}
		}

		err = tx.Commit()
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Error(err, "")
			}
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
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
			log.Error(err, "")
			return
		}

		authHostname := r.Header.Get("X-PBS-Authenticated-Agent")
		if authHostname != "" && authHostname != reqParsed.Hostname {
			w.WriteHeader(http.StatusForbidden)
			hostnameErr := fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, reqParsed.Hostname)
			WriteErrorResponse(w, hostnameErr)
			log.Error(hostnameErr, "")
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			log.Error(fmt.Errorf("no drives provided"), "")
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		cert, ca, err := storeInstance.CertManager.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
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
		log.Info("renewing target certificates")

		tx, err := storeInstance.TargetSvc.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		currentHost, err := storeInstance.AgentHostSvc.GetAgentHost(reqParsed.Hostname)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Error(err, "")
			}
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
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
			if err := tx.Rollback(); err != nil {
				log.Error(err, "")
			}
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		for _, drive := range reqParsed.Drives {
			targetName := database.GetAgentTargetName(reqParsed.Hostname, drive.Letter, reqParsed.OperatingSystem)

			existingTarget, err := storeInstance.TargetSvc.GetTarget(targetName)
			if err != nil {
				log.Warn("target not found during renewal, skipping")
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
				JobCount:         existingTarget.JobCount,
				AgentVersion:     existingTarget.AgentVersion,
				ConnectionStatus: existingTarget.ConnectionStatus,
				MountScript:      existingTarget.MountScript,
			}

			err = storeInstance.TargetSvc.DeleteTarget(tx, targetName)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}

			err = storeInstance.TargetSvc.CreateTarget(tx, updatedTarget)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}
			log.Info("renewed target certificate")
		}

		err = tx.Commit()
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Error(err, "")
			}
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}
	}
}
