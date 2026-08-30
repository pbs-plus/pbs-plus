//go:build linux

package targetapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type TargetStatusResult struct {
	Index            int
	AgentVersion     string
	ConnectionStatus bool
	Error            error
}

func D2DTargetHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		all, err := app.Target.GetAllTargets()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		for i := range all {
			switch {
			case all[i].IsS3():
				all[i].ConnectionStatus = true
				all[i].AgentVersion = "N/A (S3 target)"
			case all[i].IsLocal():
				all[i].AgentVersion = "N/A (local target)"
				_, err := os.Stat(all[i].Path)
				all[i].ConnectionStatus = err == nil && validate.IsValid(all[i].Path)
			case all[i].IsAgent():
				if qSess, ok := app.Agents.GetQuicPipe(all[i].GetHostname()); ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = qSess.GetVersion()
				} else if tSess, ok := app.Agents.GetStreamPipe(all[i].GetHostname()); ok {
					all[i].ConnectionStatus = true
					all[i].AgentVersion = tSess.GetVersion()
				}
			default:
				all[i].AgentVersion = "N/A"
			}
		}

		digest, err := digest.Calculate(all)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		data := make([]targetResponse, len(all))
		for i := range all {
			data[i] = newTargetResponse(all[i])
		}

		toReturn := TargetsResponse{
			Data:    data,
			Digest:  digest,
			Success: true,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			log.Error(err, "")
		}
	}
}

func D2DTargetStatusHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		// Trigger async refresh if requested
		if strings.ToLower(r.FormValue("refresh")) == "true" {
			app.Target.RefreshStatuses()
		}

		cached := app.Target.GetCachedStatuses()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cached); err != nil {
			log.Error(err, "")
		}
	}
}

type NewAgentHostnameRequest struct {
	Hostname        string             `json:"hostname"`
	Drives          []fswire.DriveInfo `json:"drives"`
	OperatingSystem string             `json:"os"`
}

func D2DTargetAgentHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		authHostname := r.Header.Get("X-PBS-Authenticated-Agent")

		var reqParsed NewAgentHostnameRequest
		err := json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, fmt.Errorf("Failed to parse request body: %w", err))
			return
		}

		if authHostname != "" && authHostname != reqParsed.Hostname {
			w.WriteHeader(http.StatusForbidden)
			hostnameErr := fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, reqParsed.Hostname)
			respond.WriteErrorResponse(w, hostnameErr)
			return
		}

		if reqParsed.Hostname == "" {
			w.WriteHeader(http.StatusBadRequest)
			respond.WriteErrorResponse(w, fmt.Errorf("Hostname is required in request body"))
			return
		}

		clientIP := r.RemoteAddr
		forwarded := r.Header.Get("X-FORWARDED-FOR")
		if forwarded != "" {
			ips := strings.Split(forwarded, ",")
			clientIP = strings.TrimSpace(ips[0])
		}

		if strings.Contains(clientIP, ":") {
			clientIP = strings.Split(clientIP, ":")[0]
		}

		tx, err := app.Target.NewTransaction()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, fmt.Errorf("Failed to start transaction: %w", err))
			return
		}
		defer func() {
			if tx != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
			}
		}()

		for _, parsedDrive := range reqParsed.Drives {
			targetName := coredb.GetAgentTargetName(reqParsed.Hostname, parsedDrive.Letter, reqParsed.OperatingSystem)

			targetData := coredb.Target{
				Name:             targetName,
				Type:             coredb.TargetTypeFilesystem,
				Access:           coredb.FilesystemAccessAgent,
				AgentHost:        coredb.AgentHost{Name: reqParsed.Hostname},
				VolumeID:         parsedDrive.Letter,
				VolumeType:       parsedDrive.Type,
				VolumeName:       parsedDrive.VolumeName,
				VolumeFS:         parsedDrive.FileSystem,
				VolumeFreeBytes:  int(parsedDrive.FreeBytes),
				VolumeUsedBytes:  int(parsedDrive.UsedBytes),
				VolumeTotalBytes: int(parsedDrive.TotalBytes),
				VolumeFree:       parsedDrive.Free,
				VolumeUsed:       parsedDrive.Used,
				VolumeTotal:      parsedDrive.Total,
			}

			err = app.Target.UpsertTarget(tx, targetData)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				respond.WriteErrorResponse(w, fmt.Errorf("Failed to upsert target %s: %w", targetName, err))
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			respond.WriteErrorResponse(w, fmt.Errorf("Failed to commit transaction: %w", err))
			return
		}
		tx = nil

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]bool{
			"success": true,
		})

		if err != nil {
			log.Error(err, "failed to encode success response")
		}
	}
}

func ExtJsTargetHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		newTarget := coredb.Target{}
		if err := applyTargetForm(&newTarget, r, true); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		password := r.FormValue("database_password")
		if newTarget.IsDatabase() && password == "" {
			respond.WriteErrorResponse(w, errors.New("database password is required"))
			return
		}
		s3Secret := r.FormValue("s3_secret_key")
		if newTarget.IsS3() && r.Form.Has("s3_endpoint") && s3Secret == "" {
			respond.WriteErrorResponse(w, errors.New("S3 secret key is required"))
			return
		}

		err = app.Target.CreateTarget(nil, newTarget)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if password != "" {
			if err := app.Target.AddDatabasePassword(newTarget.Name, password); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
		}
		if s3Secret != "" {
			if err := app.Target.AddS3Secret(newTarget.Name, s3Secret); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
		}

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsTargetSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			err := r.ParseForm()
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			target, err := app.Target.GetTarget(validate.DecodePath(r.PathValue("target")))
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := applyTargetForm(&target, r, false); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						target.Name = ""
					case "path":
						target.Path = ""
					case "mount_script":
						target.MountScript = ""
					}
				}
			}

			err = app.Target.UpdateTarget(nil, target)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if password := r.FormValue("database_password"); password != "" {
				if err := app.Target.AddDatabasePassword(target.Name, password); err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
			}
			if secret := r.FormValue("s3_secret_key"); secret != "" {
				if err := app.Target.AddS3Secret(target.Name, secret); err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
			}

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodGet {
			target, err := app.Target.GetTarget(validate.DecodePath(r.PathValue("target")))
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			switch {
			case target.IsAgent():
				arpcSess, ok := app.Agents.GetStreamPipe(target.GetHostname())
				if ok {
					target.AgentVersion = arpcSess.GetVersion()
					target.ConnectionStatus = false

					if strings.ToLower(r.FormValue("status")) == "true" {
						respMsg, err := arpcSess.CallMessage(
							r.Context(),
							"target_status",
							&fswire.TargetStatusReq{Drive: target.VolumeID},
						)
						if err == nil && strings.HasPrefix(respMsg, "reachable") {
							target.ConnectionStatus = true
							splittedMsg := strings.Split(respMsg, "|")
							if len(splittedMsg) > 1 {
								target.AgentVersion = splittedMsg[1]
							}
						}
					}
				}
			case target.IsS3():
				target.ConnectionStatus = true
				target.AgentVersion = "N/A (S3 target)"
			case target.IsLocal():
				target.AgentVersion = "N/A (local target)"
				_, err := os.Stat(target.Path)
				target.ConnectionStatus = err == nil && validate.IsValid(target.Path)
			default:
				target.AgentVersion = "N/A"
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = newTargetResponse(target)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodDelete {
			err := app.Target.DeleteTarget(nil, validate.DecodePath(r.PathValue("target")))
			if err != nil {
				respond.WriteErrorResponse(w, err)
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

func ExtJsTargetS3SecretHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := TargetConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		target, err := app.Target.GetTarget(validate.DecodePath(r.PathValue("target")))
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		if r.FormValue("secret") == "" {
			respond.WriteErrorResponse(w, errors.New("invalid empty secret"))
			return
		}

		err = app.Target.AddS3Secret(target.Name, r.FormValue("secret"))
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsTargetDatabasePasswordHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		password := r.FormValue("password")
		if password == "" {
			respond.WriteErrorResponse(w, errors.New("invalid empty password"))
			return
		}
		targetName := validate.DecodePath(r.PathValue("target"))
		if err := app.Target.AddDatabasePassword(targetName, password); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(TargetConfigResponse{Status: http.StatusOK, Success: true}); err != nil {
			log.Error(err, "")
		}
	}
}

type TargetsResponse struct {
	Data    []targetResponse `json:"data"`
	Digest  string           `json:"digest"`
	Success bool             `json:"success"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    targetResponse    `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type targetResponse struct {
	coredb.Target
	TargetType  string `json:"target_type"`
	Kind        string `json:"kind"`
	S3Endpoint  string `json:"s3_endpoint,omitempty"`
	S3Region    string `json:"s3_region,omitempty"`
	S3AccessKey string `json:"s3_access_key,omitempty"`
	S3Bucket    string `json:"s3_bucket,omitempty"`
	S3UseSSL    bool   `json:"s3_use_ssl"`
	S3PathStyle bool   `json:"s3_path_style"`
}

func newTargetResponse(target coredb.Target) targetResponse {
	response := targetResponse{
		Target:     target,
		TargetType: target.LegacyType(),
		Kind:       string(target.Type),
	}
	if target.S3Info != nil {
		response.S3Endpoint = target.S3Info.Endpoint
		response.S3Region = target.S3Info.Region
		response.S3AccessKey = target.S3Info.AccessKey
		response.S3Bucket = target.S3Info.Bucket
		response.S3UseSSL = target.S3Info.UseSSL
		response.S3PathStyle = target.S3Info.IsPathStyle
	}
	return response
}

func targetTypeFromRequest(r *http.Request) coredb.TargetType {
	if kind := r.FormValue("kind"); kind != "" {
		return coredb.TargetType(kind)
	}
	return coredb.TargetType(r.FormValue("target_type"))
}

func applyS3Form(target *coredb.Target, r *http.Request) error {
	if !r.Form.Has("s3_endpoint") {
		return nil
	}

	endpoint := strings.TrimSpace(r.FormValue("s3_endpoint"))
	if endpoint == "" {
		return errors.New("S3 endpoint is required")
	}
	parsedEndpoint, err := url.Parse("https://" + strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://"))
	if err != nil || parsedEndpoint.Host == "" || parsedEndpoint.Path != "" || parsedEndpoint.RawQuery != "" {
		return fmt.Errorf("invalid S3 endpoint %q", endpoint)
	}

	bucket := strings.TrimSpace(r.FormValue("s3_bucket"))
	if bucket == "" || strings.ContainsAny(bucket, "/?#") {
		return fmt.Errorf("invalid S3 bucket %q", bucket)
	}

	useSSL, err := strconv.ParseBool(r.FormValue("s3_use_ssl"))
	if err != nil {
		return fmt.Errorf("invalid S3 TLS setting %q", r.FormValue("s3_use_ssl"))
	}
	pathStyle, err := strconv.ParseBool(r.FormValue("s3_path_style"))
	if err != nil {
		return fmt.Errorf("invalid S3 addressing style %q", r.FormValue("s3_path_style"))
	}

	s3URL := url.URL{Scheme: "http", Host: parsedEndpoint.Host}
	if useSSL {
		s3URL.Scheme = "https"
	}
	if accessKey := strings.TrimSpace(r.FormValue("s3_access_key")); accessKey != "" {
		s3URL.User = url.User(accessKey)
	}
	if pathStyle {
		s3URL.Path = "/" + bucket
	} else {
		s3URL.Host = bucket + "." + parsedEndpoint.Host
	}
	query := s3URL.Query()
	query.Set("path-style", strconv.FormatBool(pathStyle))
	if region := strings.TrimSpace(r.FormValue("s3_region")); region != "" {
		query.Set("region", region)
	}
	s3URL.RawQuery = query.Encode()
	target.Path = s3URL.String()
	return nil
}

func applyTargetForm(target *coredb.Target, r *http.Request, create bool) error {
	setString := func(key string, dest *string) {
		if create || r.Form.Has(key) {
			*dest = r.FormValue(key)
		}
	}

	setString("name", &target.Name)
	if create || r.Form.Has("kind") || r.Form.Has("target_type") {
		target.Type = targetTypeFromRequest(r)
	}
	if create || r.Form.Has("access") {
		target.Access = coredb.FilesystemAccess(r.FormValue("access"))
	}
	setString("path", &target.Path)
	setString("mount_script", &target.MountScript)
	setString("database_host", &target.DatabaseHost)
	setString("database_username", &target.DatabaseUsername)
	setString("database_tls_mode", &target.DatabaseTLSMode)
	setString("database_ca_certificate", &target.DatabaseCACertificate)
	setString("database_default_client_dir", &target.DatabaseDefaultClientDir)
	setString("database_variant", &target.DatabaseVariant)
	setString("database_default_client_family", &target.DatabaseClientFamily)

	if create || r.Form.Has("database_port") {
		port := r.FormValue("database_port")
		if port == "" {
			target.DatabasePort = 0
		} else {
			parsed, err := strconv.Atoi(port)
			if err != nil {
				return fmt.Errorf("invalid database port %q", port)
			}
			target.DatabasePort = parsed
		}
	}
	if target.Type == coredb.TargetTypeS3 {
		return applyS3Form(target, r)
	}
	return nil
}
