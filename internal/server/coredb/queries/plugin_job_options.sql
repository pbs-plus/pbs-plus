-- name: UpsertBackupPluginOptions :exec
INSERT INTO backup_plugin_options (
    backup_id, plugin_id, plugin_version, schema_version, options, updated_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(backup_id) DO UPDATE SET
    plugin_id = excluded.plugin_id,
    plugin_version = excluded.plugin_version,
    schema_version = excluded.schema_version,
    options = excluded.options,
    updated_at = excluded.updated_at;

-- name: GetBackupPluginOptions :one
SELECT backup_id, plugin_id, plugin_version, schema_version, options, updated_at
FROM backup_plugin_options
WHERE backup_id = ?;

-- name: ListBackupPluginOptionsByPluginVersion :many
SELECT backup_id, plugin_id, plugin_version, schema_version, options, updated_at
FROM backup_plugin_options
WHERE plugin_id = sqlc.arg(plugin_id) AND plugin_version = sqlc.arg(plugin_version)
ORDER BY backup_id;

-- name: ArchiveBackupPluginOptions :exec
INSERT INTO backup_plugin_option_history (
    backup_id, plugin_id, plugin_version, schema_version, options, migrated_at
)
SELECT backup_id, plugin_id, plugin_version, schema_version, options, sqlc.arg(migrated_at)
FROM backup_plugin_options AS option
WHERE option.backup_id = sqlc.arg(backup_id);

-- name: ListBackupPluginOptionHistory :many
SELECT id, backup_id, plugin_id, plugin_version, schema_version, options, migrated_at
FROM backup_plugin_option_history
WHERE backup_id = ?
ORDER BY migrated_at DESC, id DESC;

-- name: UpsertRestorePluginOptions :exec
INSERT INTO restore_plugin_options (
    restore_id, plugin_id, plugin_version, schema_version, options, updated_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(restore_id) DO UPDATE SET
    plugin_id = excluded.plugin_id,
    plugin_version = excluded.plugin_version,
    schema_version = excluded.schema_version,
    options = excluded.options,
    updated_at = excluded.updated_at;

-- name: GetRestorePluginOptions :one
SELECT restore_id, plugin_id, plugin_version, schema_version, options, updated_at
FROM restore_plugin_options
WHERE restore_id = ?;

-- name: ListRestorePluginOptionsByPluginVersion :many
SELECT restore_id, plugin_id, plugin_version, schema_version, options, updated_at
FROM restore_plugin_options
WHERE plugin_id = sqlc.arg(plugin_id) AND plugin_version = sqlc.arg(plugin_version)
ORDER BY restore_id;

-- name: ArchiveRestorePluginOptions :exec
INSERT INTO restore_plugin_option_history (
    restore_id, plugin_id, plugin_version, schema_version, options, migrated_at
)
SELECT restore_id, plugin_id, plugin_version, schema_version, options, sqlc.arg(migrated_at)
FROM restore_plugin_options AS option
WHERE option.restore_id = sqlc.arg(restore_id);

-- name: ListRestorePluginOptionHistory :many
SELECT id, restore_id, plugin_id, plugin_version, schema_version, options, migrated_at
FROM restore_plugin_option_history
WHERE restore_id = ?
ORDER BY migrated_at DESC, id DESC;
