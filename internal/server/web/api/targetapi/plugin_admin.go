//go:build linux

package targetapi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type pluginRepositoryResponse struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	URL             string `json:"url"`
	Fingerprint     string `json:"fingerprint"`
	Enabled         bool   `json:"enabled"`
	LastRefreshedAt string `json:"last_refreshed_at,omitempty"`
	LastError       string `json:"last_error,omitempty"`
}

type installedPluginResponse struct {
	PluginID      string                     `json:"plugin_id"`
	RepositoryID  string                     `json:"repository_id"`
	ActiveVersion string                     `json:"active_version"`
	Enabled       bool                       `json:"enabled"`
	Versions      []installedVersionResponse `json:"versions"`
}

type installedVersionResponse struct {
	Version         string `json:"version"`
	Platform        string `json:"platform"`
	HealthState     string `json:"health_state"`
	HealthMessage   string `json:"health_message,omitempty"`
	HealthCheckedAt string `json:"health_checked_at,omitempty"`
}

type repositoryReleaseResponse struct {
	PluginID    string   `json:"plugin_id"`
	Version     string   `json:"version"`
	Publisher   string   `json:"publisher"`
	Channel     string   `json:"channel"`
	TargetTypes []string `json:"target_types"`
}

func ExtJsPluginRepositoriesHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			repositories, err := app.CoreDB.ListPluginRepositories(r.Context())
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			data := make([]pluginRepositoryResponse, len(repositories))
			for index, repository := range repositories {
				digest := sha256.Sum256(repository.PublicKey)
				data[index] = pluginRepositoryResponse{
					ID:          repository.ID,
					Name:        repository.Name,
					URL:         repository.URL,
					Fingerprint: hex.EncodeToString(digest[:]),
					Enabled:     repository.Enabled,
					LastError:   repository.LastError,
				}
				if !repository.LastRefreshedAt.IsZero() {
					data[index].LastRefreshedAt = repository.LastRefreshedAt.Format("2006-01-02 15:04:05")
				}
			}
			writePluginTargetResponse(w, data)
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			fingerprint, err := plugins.AddRepository(r.Context(), app.CoreDB, plugins.AddRepositoryRequest{
				ID:           r.FormValue("id"),
				Name:         r.FormValue("name"),
				URL:          r.FormValue("url"),
				PublicKeyPEM: r.FormValue("public_key_pem"),
				Fingerprint:  r.FormValue("fingerprint"),
			})
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writePluginTargetResponse(w, map[string]string{"fingerprint": fingerprint})
		default:
			respond.MethodNotAllowed(w, r)
		}
	}
}

func ExtJsPluginRepositoryHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repositoryID := validate.DecodePath(r.PathValue("repository"))
		switch r.Method {
		case http.MethodPut:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			enabled, err := strconv.ParseBool(r.FormValue("enabled"))
			if err != nil {
				respond.WriteErrorResponse(w, errors.New("enabled must be true or false"))
				return
			}
			updated, err := app.CoreDB.SetPluginRepositoryEnabled(r.Context(), repositoryID, enabled)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if !updated {
				respond.WriteErrorResponse(w, errors.New("plugin repository was not found"))
				return
			}
			writePluginTargetResponse(w, nil)
		case http.MethodDelete:
			if err := plugins.RemoveRepository(r.Context(), app.CoreDB, repositoryID); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writePluginTargetResponse(w, nil)
		default:
			respond.MethodNotAllowed(w, r)
		}
	}
}

func ExtJsPluginRepositoryRefreshHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respond.MethodNotAllowed(w, r)
			return
		}
		index, changed, err := plugins.Refresh(r.Context(), app.CoreDB, targetplugin.Fetcher{}, validate.DecodePath(r.PathValue("repository")))
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		data := make([]repositoryReleaseResponse, len(index.Releases))
		for releaseIndex, release := range index.Releases {
			data[releaseIndex] = repositoryReleaseResponse{
				PluginID:    release.PluginID,
				Version:     release.Version,
				Publisher:   release.Publisher,
				Channel:     release.Channel,
				TargetTypes: release.TargetTypes,
			}
		}
		writePluginTargetResponse(w, map[string]any{"changed": changed, "releases": data})
	}
}

func ExtJsInstalledPluginsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respond.MethodNotAllowed(w, r)
			return
		}
		installed, err := app.CoreDB.ListInstalledPlugins(r.Context())
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		data := make([]installedPluginResponse, len(installed))
		for index, plugin := range installed {
			versions, err := app.CoreDB.ListInstalledPluginVersions(r.Context(), plugin.PluginID)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			data[index] = installedPluginResponse{
				PluginID:      plugin.PluginID,
				RepositoryID:  plugin.RepositoryID,
				ActiveVersion: plugin.ActiveVersion,
				Enabled:       plugin.Enabled,
				Versions:      make([]installedVersionResponse, len(versions)),
			}
			for versionIndex, version := range versions {
				data[index].Versions[versionIndex] = installedVersionResponse{
					Version:       version.Version,
					Platform:      version.Platform,
					HealthState:   string(version.HealthState),
					HealthMessage: version.HealthMessage,
				}
				if !version.HealthCheckedAt.IsZero() {
					data[index].Versions[versionIndex].HealthCheckedAt = version.HealthCheckedAt.Format("2006-01-02 15:04:05")
				}
			}
		}
		writePluginTargetResponse(w, data)
	}
}

func ExtJsPluginInstallHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respond.MethodNotAllowed(w, r)
			return
		}
		if app.Engine == nil {
			respond.WriteErrorResponse(w, errors.New("job engine is unavailable"))
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		activate, err := strconv.ParseBool(r.FormValue("activate"))
		if err != nil {
			respond.WriteErrorResponse(w, errors.New("activate must be true or false"))
			return
		}
		execution, existing, err := plugins.Submit(r.Context(), app.Engine, jobs.PluginInstallInput{
			RepositoryID: r.FormValue("repository_id"),
			PluginID:     r.FormValue("plugin_id"),
			Version:      r.FormValue("version"),
			Activate:     activate,
		})
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		writePluginTargetResponse(w, map[string]any{"execution_id": execution.ID, "existing": existing})
	}
}
