-- name: CreateTargetPluginRepository :exec
INSERT INTO target_plugin_repositories (id, name, url, public_key, enabled)
VALUES (?, ?, ?, ?, ?);

-- name: GetTargetPluginRepository :one
SELECT id, name, url, public_key, enabled, etag, last_modified, last_refreshed_at, last_error
FROM target_plugin_repositories
WHERE id = ?;

-- name: ListTargetPluginRepositories :many
SELECT id, name, url, public_key, enabled, etag, last_modified, last_refreshed_at, last_error
FROM target_plugin_repositories
ORDER BY name, id;

-- name: UpdateTargetPluginRepository :execrows
UPDATE target_plugin_repositories
SET name = sqlc.arg(name), url = sqlc.arg(url)
WHERE id = sqlc.arg(id);

-- name: SetTargetPluginRepositoryEnabled :execrows
UPDATE target_plugin_repositories
SET enabled = sqlc.arg(enabled)
WHERE id = sqlc.arg(id);

-- name: UpdateTargetPluginRepositoryRefresh :execrows
UPDATE target_plugin_repositories
SET etag = sqlc.arg(etag),
    last_modified = sqlc.arg(last_modified),
    last_refreshed_at = sqlc.arg(last_refreshed_at),
    last_error = sqlc.arg(last_error)
WHERE id = sqlc.arg(id);

-- name: DeleteTargetPluginRepository :execrows
DELETE FROM target_plugin_repositories WHERE id = ?;

-- name: EnsureTargetPlugin :exec
INSERT INTO target_plugins (plugin_id, repository_id, active_version, enabled)
VALUES (?, ?, '', ?)
ON CONFLICT(plugin_id) DO NOTHING;

-- name: GetTargetPlugin :one
SELECT plugin_id, repository_id, active_version, enabled
FROM target_plugins
WHERE plugin_id = ?;

-- name: ListTargetPlugins :many
SELECT plugin_id, repository_id, active_version, enabled
FROM target_plugins
ORDER BY plugin_id;

-- name: SetTargetPluginEnabled :execrows
UPDATE target_plugins
SET enabled = sqlc.arg(enabled)
WHERE plugin_id = sqlc.arg(plugin_id);

-- name: ClearTargetPluginActivation :execrows
UPDATE target_plugins
SET active_version = ''
WHERE plugin_id = ?;

-- name: CreateTargetPluginVersion :exec
INSERT INTO target_plugin_versions (
    plugin_id, version, platform, install_path, manifest, artifact_sha256,
    installed_at, health_state, health_message, health_checked_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetTargetPluginVersion :one
SELECT plugin_id, version, platform, install_path, manifest, artifact_sha256,
       installed_at, health_state, health_message, health_checked_at
FROM target_plugin_versions
WHERE plugin_id = sqlc.arg(plugin_id) AND version = sqlc.arg(version);

-- name: ListTargetPluginVersions :many
SELECT plugin_id, version, platform, install_path, manifest, artifact_sha256,
       installed_at, health_state, health_message, health_checked_at
FROM target_plugin_versions
WHERE plugin_id = ?
ORDER BY installed_at DESC, version DESC;

-- name: ActivateTargetPluginVersion :execrows
UPDATE target_plugins
SET active_version = sqlc.arg(version)
WHERE target_plugins.plugin_id = sqlc.arg(plugin_id)
  AND EXISTS (
    SELECT 1
    FROM target_plugin_versions
    WHERE target_plugin_versions.plugin_id = target_plugins.plugin_id
      AND target_plugin_versions.version = sqlc.arg(version)
  );

-- name: UpdateTargetPluginVersionHealth :execrows
UPDATE target_plugin_versions
SET health_state = sqlc.arg(health_state),
    health_message = sqlc.arg(health_message),
    health_checked_at = sqlc.arg(health_checked_at)
WHERE plugin_id = sqlc.arg(plugin_id) AND version = sqlc.arg(version);

-- name: DeleteInactiveTargetPluginVersion :execrows
DELETE FROM target_plugin_versions
WHERE target_plugin_versions.plugin_id = sqlc.arg(plugin_id)
  AND target_plugin_versions.version = sqlc.arg(version)
  AND NOT EXISTS (
    SELECT 1
    FROM target_plugins
    WHERE target_plugins.plugin_id = target_plugin_versions.plugin_id
      AND target_plugins.active_version = target_plugin_versions.version
  );

-- name: DeleteEmptyTargetPlugin :execrows
DELETE FROM target_plugins
WHERE plugin_id = ?
  AND NOT EXISTS (
    SELECT 1
    FROM target_plugin_versions
    WHERE target_plugin_versions.plugin_id = target_plugins.plugin_id
  );
