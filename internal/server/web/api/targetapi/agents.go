//go:build linux

package targetapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func AgentLogHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		err := log.ParseAndLogWindowsEntry(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"success": "true"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			return
		}
	}
}

type BootstrapRequest struct {
	Hostname        string             `json:"hostname"`
	CSR             string             `json:"csr"`
	OperatingSystem string             `json:"os"`
	Drives          []fswire.DriveInfo `json:"drives"`
}

func AgentBootstrapHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		authHeader := r.Header.Get("Authorization")
		authHeaderSplit := strings.Split(authHeader, " ")
		if len(authHeaderSplit) != 2 || authHeaderSplit[0] != "Bearer" {
			w.WriteHeader(http.StatusUnauthorized)
			respond.WriteErrorResponse(w, fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader))
			log.Error(fmt.Errorf("[%s]: unauthorized bearer access: %s", r.RemoteAddr, authHeader), "")
			return
		}

		tokenStr := authHeaderSplit[1]
		token, err := app.Token.GetToken(tokenStr)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			respond.WriteErrorResponse(w, fmt.Errorf("[%s]: token not found", r.RemoteAddr))
			log.Error(fmt.Errorf("[%s]: token not found", r.RemoteAddr), "")
			return
		}

		if token.Revoked {
			w.WriteHeader(http.StatusUnauthorized)
			respond.WriteErrorResponse(w, fmt.Errorf("[%s]: token already revoked", r.RemoteAddr))
			log.Error(fmt.Errorf("[%s]: token already revoked", r.RemoteAddr), "")
			return
		}

		var reqParsed BootstrapRequest
		err = json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			log.Error(fmt.Errorf("no drives provided"), "")
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		cert, ca, err := app.CertManager.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
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
		tx, err := app.Target.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		host := coredb.AgentHost{
			Name:            reqParsed.Hostname,
			IP:              clientIP,
			Auth:            encodedCert,
			TokenUsed:       tokenStr,
			OperatingSystem: reqParsed.OperatingSystem,
		}

		_, err = app.AgentHost.GetAgentHost(reqParsed.Hostname)
		if err == nil {
			log.Info("updating host target details")
			err = app.AgentHost.UpdateAgentHost(tx, host)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				respond.WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}
		} else {
			log.Info("creating new host target")
			err = app.AgentHost.CreateAgentHost(tx, host)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				respond.WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}
		}

		for _, drive := range reqParsed.Drives {
			log.Info("bootstrapping drive")

			newTarget := coredb.Target{
				Type:             coredb.TargetTypeFilesystem,
				Access:           coredb.FilesystemAccessAgent,
				AgentHost:        coredb.AgentHost{Name: reqParsed.Hostname},
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
				Name:             coredb.GetAgentTargetName(reqParsed.Hostname, drive.Letter, reqParsed.OperatingSystem),
			}

			existingTarget, err := app.Target.GetTarget(newTarget.Name)
			if err == nil {
				newTarget.JobCount = existingTarget.JobCount
				newTarget.AgentVersion = existingTarget.AgentVersion
				newTarget.ConnectionStatus = existingTarget.ConnectionStatus

				err := app.Target.DeleteTarget(tx, newTarget.Name)
				if err != nil {
					if err := tx.Rollback(); err != nil {
						log.Error(err, "")
					}
					w.WriteHeader(http.StatusInternalServerError)
					respond.WriteErrorResponse(w, err)
					log.Error(err, "")
					return
				}

				err = app.Target.CreateTarget(tx, newTarget)
				if err != nil {
					if err := tx.Rollback(); err != nil {
						log.Error(err, "")
					}
					w.WriteHeader(http.StatusInternalServerError)
					respond.WriteErrorResponse(w, err)
					log.Error(err, "")
					return
				}
				log.Info("updated existing target auth")
			} else {
				err := app.Target.CreateTarget(tx, newTarget)
				if err != nil {
					if err := tx.Rollback(); err != nil {
						log.Error(err, "")
					}
					w.WriteHeader(http.StatusInternalServerError)
					respond.WriteErrorResponse(w, err)
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
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}
	}
}

func AgentRenewHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		var reqParsed BootstrapRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		authHostname := r.Header.Get("X-PBS-Authenticated-Agent")
		if authHostname != "" && authHostname != reqParsed.Hostname {
			w.WriteHeader(http.StatusForbidden)
			hostnameErr := fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, reqParsed.Hostname)
			respond.WriteErrorResponse(w, hostnameErr)
			log.Error(hostnameErr, "")
			return
		}

		if len(reqParsed.Drives) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, fmt.Errorf("no drives provided"))
			log.Error(fmt.Errorf("no drives provided"), "")
			return
		}

		decodedCSR, err := base64.StdEncoding.DecodeString(reqParsed.CSR)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		cert, ca, err := app.CertManager.SignCSR(decodedCSR)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
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

		tx, err := app.Target.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		currentHost, err := app.AgentHost.GetAgentHost(reqParsed.Hostname)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Error(err, "")
			}
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		host := coredb.AgentHost{
			Name:            reqParsed.Hostname,
			IP:              clientIP,
			Auth:            encodedCert,
			TokenUsed:       currentHost.TokenUsed,
			OperatingSystem: reqParsed.OperatingSystem,
		}

		err = app.AgentHost.UpdateAgentHost(tx, host)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Error(err, "")
			}
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		for _, drive := range reqParsed.Drives {
			targetName := coredb.GetAgentTargetName(reqParsed.Hostname, drive.Letter, reqParsed.OperatingSystem)

			existingTarget, err := app.Target.GetTarget(targetName)
			if err != nil {
				log.Warn("target not found during renewal, skipping")
				continue
			}

			updatedTarget := coredb.Target{
				Name:             targetName,
				Type:             coredb.TargetTypeFilesystem,
				Access:           coredb.FilesystemAccessAgent,
				AgentHost:        coredb.AgentHost{Name: reqParsed.Hostname},
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

			err = app.Target.DeleteTarget(tx, targetName)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				respond.WriteErrorResponse(w, err)
				log.Error(err, "")
				return
			}

			err = app.Target.CreateTarget(tx, updatedTarget)
			if err != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				w.WriteHeader(http.StatusInternalServerError)
				respond.WriteErrorResponse(w, err)
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
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"ca": encodedCA, "cert": encodedCert})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, err)
			log.Error(err, "")
			return
		}
	}
}
