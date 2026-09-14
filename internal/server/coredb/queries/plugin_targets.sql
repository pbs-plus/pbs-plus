-- name: CreatePluginTargetConfig :exec
INSERT INTO plugin_target_configs (
    target_name, plugin_id, plugin_version, target_type, schema_version, config, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: UpdatePluginTargetConfig :execrows
UPDATE plugin_target_configs
SET plugin_version = sqlc.arg(plugin_version),
    target_type = sqlc.arg(target_type),
    schema_version = sqlc.arg(schema_version),
    config = sqlc.arg(config),
    updated_at = sqlc.arg(updated_at)
WHERE target_name = sqlc.arg(target_name)
  AND plugin_id = sqlc.arg(plugin_id);

-- name: GetPluginTargetConfig :one
SELECT target_name, plugin_id, plugin_version, target_type, schema_version, config, updated_at
FROM plugin_target_configs
WHERE target_name = ?;

-- name: ListPluginTargetConfigsByPlugin :many
SELECT target_name, plugin_id, plugin_version, target_type, schema_version, config, updated_at
FROM plugin_target_configs
WHERE plugin_id = ?
ORDER BY target_name;

-- name: UpsertPluginTargetSecret :exec
INSERT INTO plugin_target_secrets (target_name, field_key, encrypted_value)
VALUES (?, ?, ?)
ON CONFLICT(target_name, field_key) DO UPDATE SET encrypted_value = excluded.encrypted_value;

-- name: DeletePluginTargetSecret :execrows
DELETE FROM plugin_target_secrets
WHERE target_name = sqlc.arg(target_name) AND field_key = sqlc.arg(field_key);

-- name: ListPluginTargetSecrets :many
SELECT field_key, encrypted_value
FROM plugin_target_secrets
WHERE target_name = ?
ORDER BY field_key;

-- name: ArchivePluginTargetConfig :exec
INSERT INTO plugin_target_config_history (
    target_name, plugin_id, plugin_version, schema_version, config, migrated_at
)
SELECT target_name, plugin_id, plugin_version, schema_version, config, sqlc.arg(migrated_at)
FROM plugin_target_configs AS config
WHERE config.target_name = sqlc.arg(target_name);

-- name: ListPluginTargetConfigHistory :many
SELECT id, target_name, plugin_id, plugin_version, schema_version, config, migrated_at
FROM plugin_target_config_history
WHERE target_name = ?
ORDER BY migrated_at DESC, id DESC;
