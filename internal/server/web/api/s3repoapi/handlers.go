//go:build linux

// Package s3repoapi serves the S3 backup repository configuration API.
package s3repoapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
	"github.com/pbs-plus/pbs-plus/internal/server/s3repo"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
)

type repositoryView struct {
	Name       string             `json:"name"`
	ListenAddr string             `json:"listen-addr"`
	Config     objectstore.Config `json:"config"`
	Running    bool               `json:"running"`
	Error      string             `json:"error,omitempty"`
	Endpoints  []string           `json:"endpoints"`
}

func toView(s s3repo.Status) repositoryView {
	config := s.Config
	config.Credentials = append([]objectstore.Credential(nil), s.Config.Credentials...)
	for i := range config.Credentials {
		config.Credentials[i].SecretKey = ""
	}
	return repositoryView{
		Name:       s.Name,
		ListenAddr: s.ListenAddr,
		Config:     config,
		Running:    s.Running,
		Error:      s.Error,
		Endpoints:  s.Endpoints,
	}
}

func writeExtJS(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Status  int  `json:"status"`
		Success bool `json:"success"`
		Data    any  `json:"data"`
	}{Status: http.StatusOK, Success: true, Data: data}); err != nil {
		log.Error(err, "")
	}
}

func formValues(r *http.Request) (s3repo.Repository, error) {
	repo := s3repo.Repository{
		Name:       strings.TrimSpace(r.FormValue("name")),
		ListenAddr: strings.TrimSpace(r.FormValue("listen-addr")),
	}
	raw := strings.TrimSpace(r.FormValue("config"))
	if raw == "" {
		return s3repo.Repository{}, fmt.Errorf("config is required")
	}
	if err := json.Unmarshal([]byte(raw), &repo.Config); err != nil {
		return s3repo.Repository{}, fmt.Errorf("invalid config: %w", err)
	}
	return repo, nil
}

// preserveSecrets keeps stored secret keys for credentials the client resubmitted blank, since views redact them.
func preserveSecrets(next *objectstore.Config, current objectstore.Config) {
	secrets := make(map[string]string, len(current.Credentials))
	for _, credential := range current.Credentials {
		secrets[credential.AccessKey] = credential.SecretKey
	}
	for i := range next.Credentials {
		if next.Credentials[i].SecretKey == "" {
			next.Credentials[i].SecretKey = secrets[next.Credentials[i].AccessKey]
		}
	}
}

// ListHandler serves GET (list) and POST (create) on the repository collection.
func ListHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			statuses, err := s3repo.StatusAll()
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			views := make([]repositoryView, 0, len(statuses))
			for _, s := range statuses {
				views = append(views, toView(s))
			}
			writeExtJS(w, views)
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			repo, err := formValues(r)
			if err != nil {
				respond.Error(w, http.StatusBadRequest, err)
				return
			}
			if err := s3repo.Validate(repo); err != nil {
				respond.Error(w, http.StatusBadRequest, err)
				return
			}
			if _, exists, err := s3repo.Load(repo.Name); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			} else if exists {
				respond.BadRequest(w, "repository %s already exists", repo.Name)
				return
			}
			if err := s3repo.Apply(r.Context(), repo); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writeExtJS(w, toView(s3repo.StatusOf(repo)))
		default:
			respond.MethodNotAllowed(w, r)
		}
	}
}

// SingleHandler serves GET, PUT and DELETE on one repository; names are immutable.
func SingleHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			respond.MethodNotAllowed(w, r)
			return
		}
		name := r.PathValue("name")
		existing, ok, err := s3repo.Load(name)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !ok {
			respond.NotFound(w, "no such repository: %s", name)
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeExtJS(w, toView(s3repo.StatusOf(existing)))
		case http.MethodPut:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			repo, err := formValues(r)
			if err != nil {
				respond.Error(w, http.StatusBadRequest, err)
				return
			}
			if repo.Name != "" && repo.Name != existing.Name {
				respond.BadRequest(w, "repositories cannot be renamed")
				return
			}
			repo.Name = existing.Name
			repo.CreatedAt = existing.CreatedAt
			preserveSecrets(&repo.Config, existing.Config)
			if err := s3repo.Validate(repo); err != nil {
				respond.Error(w, http.StatusBadRequest, err)
				return
			}
			if err := s3repo.Apply(r.Context(), repo); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writeExtJS(w, toView(s3repo.StatusOf(repo)))
		case http.MethodDelete:
			s3repo.Stop(name)
			if err := s3repo.Delete(name); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writeExtJS(w, nil)
		}
	}
}
