CREATE TABLE target_plugin_repositories (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL UNIQUE,
  public_key BLOB NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
  etag TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL DEFAULT '',
  last_refreshed_at INTEGER,
  last_error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE target_plugins (
  plugin_id TEXT PRIMARY KEY,
  repository_id TEXT NOT NULL,
  active_version TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
  FOREIGN KEY (repository_id) REFERENCES target_plugin_repositories(id) ON DELETE RESTRICT
);

CREATE TABLE target_plugin_versions (
  plugin_id TEXT NOT NULL,
  version TEXT NOT NULL,
  platform TEXT NOT NULL,
  install_path TEXT NOT NULL UNIQUE,
  manifest BLOB NOT NULL,
  artifact_sha256 TEXT NOT NULL,
  installed_at INTEGER NOT NULL,
  health_state TEXT NOT NULL DEFAULT 'unknown' CHECK (health_state IN ('unknown', 'healthy', 'unhealthy')),
  health_message TEXT NOT NULL DEFAULT '',
  health_checked_at INTEGER,
  PRIMARY KEY (plugin_id, version),
  FOREIGN KEY (plugin_id) REFERENCES target_plugins(plugin_id) ON DELETE CASCADE
);
