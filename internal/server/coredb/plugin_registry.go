//go:build linux

package coredb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

// PluginHealthState is the persisted result of an installed-version health check.
type PluginHealthState string

const (
	PluginHealthUnknown   PluginHealthState = "unknown"
	PluginHealthHealthy   PluginHealthState = "healthy"
	PluginHealthUnhealthy PluginHealthState = "unhealthy"
)

// PluginRepository stores one administrator-trusted repository.
type PluginRepository struct {
	ID              string
	Name            string
	URL             string
	PublicKey       []byte
	Enabled         bool
	ETag            string
	LastModified    string
	LastRefreshedAt time.Time
	LastError       string
}

// PluginRepositoryRefresh stores conditional-request and diagnostic metadata.
type PluginRepositoryRefresh struct {
	ETag         string
	LastModified string
	RefreshedAt  time.Time
	LastError    string
}

// InstalledPlugin stores activation and enablement state for one plugin ID.
type InstalledPlugin struct {
	PluginID      string
	RepositoryID  string
	ActiveVersion string
	Enabled       bool
}

// InstalledPluginVersion stores one immutable on-disk plugin version.
type InstalledPluginVersion struct {
	PluginID        string
	Version         string
	Platform        string
	InstallPath     string
	Manifest        []byte
	ArtifactSHA256  string
	InstalledAt     time.Time
	HealthState     PluginHealthState
	HealthMessage   string
	HealthCheckedAt time.Time
}

func (db *Store) CreatePluginRepository(ctx context.Context, repository PluginRepository) error {
	if repository.ID == "" || repository.Name == "" || repository.URL == "" || len(repository.PublicKey) == 0 {
		return errors.New("plugin repository identity, name, URL, and public key are required")
	}
	ctx = db.pluginContext(ctx)
	if err := db.queries.CreateTargetPluginRepository(ctx, corequery.CreateTargetPluginRepositoryParams{
		ID:        repository.ID,
		Name:      repository.Name,
		Url:       repository.URL,
		PublicKey: repository.PublicKey,
		Enabled:   boolInteger(repository.Enabled),
	}); err != nil {
		return fmt.Errorf("create plugin repository: %w", err)
	}
	return nil
}

func (db *Store) UpdatePluginRepository(ctx context.Context, id, name, url string) (bool, error) {
	if id == "" || name == "" || url == "" {
		return false, errors.New("plugin repository identity, name, and URL are required")
	}
	rows, err := db.queries.UpdateTargetPluginRepository(db.pluginContext(ctx), corequery.UpdateTargetPluginRepositoryParams{
		Name: name,
		Url:  url,
		ID:   id,
	})
	if err != nil {
		return false, fmt.Errorf("update plugin repository: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) GetPluginRepository(ctx context.Context, id string) (PluginRepository, error) {
	row, err := db.readQueries.GetTargetPluginRepository(db.pluginContext(ctx), id)
	if err != nil {
		return PluginRepository{}, fmt.Errorf("get plugin repository: %w", err)
	}
	return pluginRepositoryFromRow(row), nil
}

func (db *Store) ListPluginRepositories(ctx context.Context) ([]PluginRepository, error) {
	rows, err := db.readQueries.ListTargetPluginRepositories(db.pluginContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("list plugin repositories: %w", err)
	}
	repositories := make([]PluginRepository, len(rows))
	for index := range rows {
		repositories[index] = pluginRepositoryFromRow(rows[index])
	}
	return repositories, nil
}

func (db *Store) SetPluginRepositoryEnabled(ctx context.Context, id string, enabled bool) (bool, error) {
	rows, err := db.queries.SetTargetPluginRepositoryEnabled(db.pluginContext(ctx), corequery.SetTargetPluginRepositoryEnabledParams{
		Enabled: boolInteger(enabled),
		ID:      id,
	})
	if err != nil {
		return false, fmt.Errorf("set plugin repository enabled: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) RecordPluginRepositoryRefresh(ctx context.Context, id string, refresh PluginRepositoryRefresh) (bool, error) {
	if refresh.RefreshedAt.IsZero() {
		return false, errors.New("plugin repository refresh time is required")
	}
	rows, err := db.queries.UpdateTargetPluginRepositoryRefresh(db.pluginContext(ctx), corequery.UpdateTargetPluginRepositoryRefreshParams{
		Etag:            refresh.ETag,
		LastModified:    refresh.LastModified,
		LastRefreshedAt: sql.NullInt64{Int64: refresh.RefreshedAt.Unix(), Valid: true},
		LastError:       refresh.LastError,
		ID:              id,
	})
	if err != nil {
		return false, fmt.Errorf("record plugin repository refresh: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) DeletePluginRepository(ctx context.Context, id string) (bool, error) {
	rows, err := db.queries.DeleteTargetPluginRepository(db.pluginContext(ctx), id)
	if err != nil {
		return false, fmt.Errorf("delete plugin repository: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) RegisterPluginVersion(ctx context.Context, repositoryID string, version InstalledPluginVersion, activate bool) error {
	if err := validateInstalledPluginVersion(repositoryID, version); err != nil {
		return err
	}
	ctx = db.pluginContext(ctx)
	return db.RunInTransaction(ctx, func(_ *Transaction, queries *corequery.Queries) error {
		if err := queries.EnsureTargetPlugin(ctx, corequery.EnsureTargetPluginParams{
			PluginID:     version.PluginID,
			RepositoryID: repositoryID,
			Enabled:      1,
		}); err != nil {
			return fmt.Errorf("create installed plugin: %w", err)
		}
		plugin, err := queries.GetTargetPlugin(ctx, version.PluginID)
		if err != nil {
			return fmt.Errorf("get installed plugin: %w", err)
		}
		if plugin.RepositoryID != repositoryID {
			return fmt.Errorf("plugin %q belongs to repository %q", version.PluginID, plugin.RepositoryID)
		}
		if err := queries.CreateTargetPluginVersion(ctx, pluginVersionParams(version)); err != nil {
			return fmt.Errorf("create installed plugin version: %w", err)
		}
		if !activate {
			return nil
		}
		rows, err := queries.ActivateTargetPluginVersion(ctx, corequery.ActivateTargetPluginVersionParams{
			Version:  version.Version,
			PluginID: version.PluginID,
		})
		if err != nil {
			return fmt.Errorf("activate installed plugin version: %w", err)
		}
		if rows != 1 {
			return errors.New("installed plugin version was not activated")
		}
		return nil
	})
}

func (db *Store) GetInstalledPlugin(ctx context.Context, pluginID string) (InstalledPlugin, error) {
	row, err := db.readQueries.GetTargetPlugin(db.pluginContext(ctx), pluginID)
	if err != nil {
		return InstalledPlugin{}, fmt.Errorf("get installed plugin: %w", err)
	}
	return installedPluginFromRow(row), nil
}

func (db *Store) ListInstalledPlugins(ctx context.Context) ([]InstalledPlugin, error) {
	rows, err := db.readQueries.ListTargetPlugins(db.pluginContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("list installed plugins: %w", err)
	}
	plugins := make([]InstalledPlugin, len(rows))
	for index := range rows {
		plugins[index] = installedPluginFromRow(rows[index])
	}
	return plugins, nil
}

func (db *Store) GetInstalledPluginVersion(ctx context.Context, pluginID, version string) (InstalledPluginVersion, error) {
	row, err := db.readQueries.GetTargetPluginVersion(db.pluginContext(ctx), corequery.GetTargetPluginVersionParams{
		PluginID: pluginID,
		Version:  version,
	})
	if err != nil {
		return InstalledPluginVersion{}, fmt.Errorf("get installed plugin version: %w", err)
	}
	return installedPluginVersionFromRow(row), nil
}

func (db *Store) ListInstalledPluginVersions(ctx context.Context, pluginID string) ([]InstalledPluginVersion, error) {
	rows, err := db.readQueries.ListTargetPluginVersions(db.pluginContext(ctx), pluginID)
	if err != nil {
		return nil, fmt.Errorf("list installed plugin versions: %w", err)
	}
	versions := make([]InstalledPluginVersion, len(rows))
	for index := range rows {
		versions[index] = installedPluginVersionFromRow(rows[index])
	}
	return versions, nil
}

func (db *Store) SetInstalledPluginEnabled(ctx context.Context, pluginID string, enabled bool) (bool, error) {
	rows, err := db.queries.SetTargetPluginEnabled(db.pluginContext(ctx), corequery.SetTargetPluginEnabledParams{
		Enabled:  boolInteger(enabled),
		PluginID: pluginID,
	})
	if err != nil {
		return false, fmt.Errorf("set installed plugin enabled: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) ActivatePluginVersion(ctx context.Context, pluginID, version string) (bool, error) {
	rows, err := db.queries.ActivateTargetPluginVersion(db.pluginContext(ctx), corequery.ActivateTargetPluginVersionParams{
		Version:  version,
		PluginID: pluginID,
	})
	if err != nil {
		return false, fmt.Errorf("activate plugin version: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) ClearPluginActivation(ctx context.Context, pluginID string) (bool, error) {
	rows, err := db.queries.ClearTargetPluginActivation(db.pluginContext(ctx), pluginID)
	if err != nil {
		return false, fmt.Errorf("clear plugin activation: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) UpdatePluginVersionHealth(ctx context.Context, pluginID, version string, state PluginHealthState, message string, checkedAt time.Time) (bool, error) {
	if err := validatePluginHealth(state, checkedAt); err != nil {
		return false, err
	}
	rows, err := db.queries.UpdateTargetPluginVersionHealth(db.pluginContext(ctx), corequery.UpdateTargetPluginVersionHealthParams{
		HealthState:     string(state),
		HealthMessage:   message,
		HealthCheckedAt: sql.NullInt64{Int64: checkedAt.Unix(), Valid: !checkedAt.IsZero()},
		PluginID:        pluginID,
		Version:         version,
	})
	if err != nil {
		return false, fmt.Errorf("update plugin version health: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) DeleteInactivePluginVersion(ctx context.Context, pluginID, version string) (bool, error) {
	rows, err := db.queries.DeleteInactiveTargetPluginVersion(db.pluginContext(ctx), corequery.DeleteInactiveTargetPluginVersionParams{
		PluginID: pluginID,
		Version:  version,
	})
	if err != nil {
		return false, fmt.Errorf("delete inactive plugin version: %w", err)
	}
	return rows == 1, nil
}

func (db *Store) DeleteEmptyPlugin(ctx context.Context, pluginID string) (bool, error) {
	rows, err := db.queries.DeleteEmptyTargetPlugin(db.pluginContext(ctx), pluginID)
	if err != nil {
		return false, fmt.Errorf("delete empty plugin: %w", err)
	}
	return rows == 1, nil
}

func validateInstalledPluginVersion(repositoryID string, version InstalledPluginVersion) error {
	if repositoryID == "" || version.PluginID == "" || version.Version == "" || version.Platform == "" || version.InstallPath == "" {
		return errors.New("plugin repository, identity, version, platform, and install path are required")
	}
	if len(version.Manifest) == 0 || version.ArtifactSHA256 == "" || version.InstalledAt.IsZero() {
		return errors.New("plugin manifest, artifact digest, and install time are required")
	}
	return validatePluginHealth(version.HealthState, version.HealthCheckedAt)
}

func validatePluginHealth(state PluginHealthState, checkedAt time.Time) error {
	switch state {
	case PluginHealthUnknown:
		if !checkedAt.IsZero() {
			return errors.New("unknown plugin health cannot have a check time")
		}
	case PluginHealthHealthy, PluginHealthUnhealthy:
		if checkedAt.IsZero() {
			return errors.New("known plugin health requires a check time")
		}
	default:
		return fmt.Errorf("invalid plugin health state %q", state)
	}
	return nil
}

func pluginVersionParams(version InstalledPluginVersion) corequery.CreateTargetPluginVersionParams {
	return corequery.CreateTargetPluginVersionParams{
		PluginID:        version.PluginID,
		Version:         version.Version,
		Platform:        version.Platform,
		InstallPath:     version.InstallPath,
		Manifest:        bytes.Clone(version.Manifest),
		ArtifactSha256:  version.ArtifactSHA256,
		InstalledAt:     version.InstalledAt.Unix(),
		HealthState:     string(version.HealthState),
		HealthMessage:   version.HealthMessage,
		HealthCheckedAt: sql.NullInt64{Int64: version.HealthCheckedAt.Unix(), Valid: !version.HealthCheckedAt.IsZero()},
	}
}

func pluginRepositoryFromRow(row corequery.TargetPluginRepository) PluginRepository {
	return PluginRepository{
		ID:              row.ID,
		Name:            row.Name,
		URL:             row.Url,
		PublicKey:       bytes.Clone(row.PublicKey),
		Enabled:         row.Enabled != 0,
		ETag:            row.Etag,
		LastModified:    row.LastModified,
		LastRefreshedAt: timeFromNullUnix(row.LastRefreshedAt),
		LastError:       row.LastError,
	}
}

func installedPluginFromRow(row corequery.TargetPlugin) InstalledPlugin {
	return InstalledPlugin{
		PluginID:      row.PluginID,
		RepositoryID:  row.RepositoryID,
		ActiveVersion: row.ActiveVersion,
		Enabled:       row.Enabled != 0,
	}
}

func installedPluginVersionFromRow(row corequery.TargetPluginVersion) InstalledPluginVersion {
	return InstalledPluginVersion{
		PluginID:        row.PluginID,
		Version:         row.Version,
		Platform:        row.Platform,
		InstallPath:     row.InstallPath,
		Manifest:        bytes.Clone(row.Manifest),
		ArtifactSHA256:  row.ArtifactSha256,
		InstalledAt:     time.Unix(row.InstalledAt, 0),
		HealthState:     PluginHealthState(row.HealthState),
		HealthMessage:   row.HealthMessage,
		HealthCheckedAt: timeFromNullUnix(row.HealthCheckedAt),
	}
}

func (db *Store) pluginContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return db.ctx
}

func boolInteger(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func timeFromNullUnix(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.Unix(value.Int64, 0)
}
