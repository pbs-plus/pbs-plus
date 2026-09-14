CREATE TABLE backup_plugin_options (
  backup_id TEXT PRIMARY KEY,
  plugin_id TEXT NOT NULL,
  plugin_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
  options BLOB NOT NULL CHECK (length(options) <= 4194304),
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (backup_id) REFERENCES backups(id) ON DELETE CASCADE,
  FOREIGN KEY (plugin_id, plugin_version) REFERENCES target_plugin_versions(plugin_id, version) ON DELETE RESTRICT
);

CREATE TABLE restore_plugin_options (
  restore_id TEXT PRIMARY KEY,
  plugin_id TEXT NOT NULL,
  plugin_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
  options BLOB NOT NULL CHECK (length(options) <= 4194304),
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (restore_id) REFERENCES restores(id) ON DELETE CASCADE,
  FOREIGN KEY (plugin_id, plugin_version) REFERENCES target_plugin_versions(plugin_id, version) ON DELETE RESTRICT
);

CREATE TABLE backup_plugin_option_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  backup_id TEXT NOT NULL,
  plugin_id TEXT NOT NULL,
  plugin_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
  options BLOB NOT NULL CHECK (length(options) <= 4194304),
  migrated_at INTEGER NOT NULL,
  FOREIGN KEY (backup_id) REFERENCES backup_plugin_options(backup_id) ON DELETE CASCADE,
  FOREIGN KEY (plugin_id, plugin_version) REFERENCES target_plugin_versions(plugin_id, version) ON DELETE RESTRICT
);

CREATE TABLE restore_plugin_option_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  restore_id TEXT NOT NULL,
  plugin_id TEXT NOT NULL,
  plugin_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 4294967295),
  options BLOB NOT NULL CHECK (length(options) <= 4194304),
  migrated_at INTEGER NOT NULL,
  FOREIGN KEY (restore_id) REFERENCES restore_plugin_options(restore_id) ON DELETE CASCADE,
  FOREIGN KEY (plugin_id, plugin_version) REFERENCES target_plugin_versions(plugin_id, version) ON DELETE RESTRICT
);

CREATE INDEX backup_plugin_options_plugin_version_idx
ON backup_plugin_options(plugin_id, plugin_version);

CREATE INDEX restore_plugin_options_plugin_version_idx
ON restore_plugin_options(plugin_id, plugin_version);
