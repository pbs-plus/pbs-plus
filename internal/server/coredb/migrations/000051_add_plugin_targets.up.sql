CREATE TABLE plugin_target_configs (
  target_name TEXT PRIMARY KEY,
  plugin_id TEXT NOT NULL,
  plugin_version TEXT NOT NULL,
  target_type TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
  config BLOB NOT NULL CHECK (length(config) <= 4194304),
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE,
  FOREIGN KEY (plugin_id, plugin_version) REFERENCES target_plugin_versions(plugin_id, version) ON DELETE RESTRICT
);

CREATE TABLE plugin_target_secrets (
  target_name TEXT NOT NULL,
  field_key TEXT NOT NULL CHECK (field_key <> '' AND length(field_key) <= 128),
  encrypted_value TEXT NOT NULL,
  PRIMARY KEY (target_name, field_key),
  FOREIGN KEY (target_name) REFERENCES plugin_target_configs(target_name) ON DELETE CASCADE
);

CREATE TABLE plugin_target_config_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_name TEXT NOT NULL,
  plugin_id TEXT NOT NULL,
  plugin_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
  config BLOB NOT NULL CHECK (length(config) <= 4194304),
  migrated_at INTEGER NOT NULL,
  FOREIGN KEY (target_name) REFERENCES plugin_target_configs(target_name) ON DELETE CASCADE,
  FOREIGN KEY (plugin_id, plugin_version) REFERENCES target_plugin_versions(plugin_id, version) ON DELETE RESTRICT
);

CREATE INDEX plugin_target_configs_plugin_version_idx
ON plugin_target_configs(plugin_id, plugin_version);

CREATE INDEX plugin_target_config_history_target_idx
ON plugin_target_config_history(target_name, migrated_at DESC);
