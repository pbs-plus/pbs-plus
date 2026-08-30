CREATE TABLE target_postgresql (
  target_name TEXT PRIMARY KEY,
  host TEXT NOT NULL,
  port INTEGER NOT NULL DEFAULT 5432 CHECK (port BETWEEN 1 AND 65535),
  username TEXT NOT NULL,
  password TEXT NOT NULL DEFAULT '',
  ssl_mode TEXT NOT NULL DEFAULT 'prefer' CHECK (ssl_mode IN ('disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full')),
  ca_certificate TEXT NOT NULL DEFAULT '',
  default_client_dir TEXT NOT NULL,
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE
);

CREATE TABLE target_mysql (
  target_name TEXT PRIMARY KEY,
  variant TEXT NOT NULL CHECK (variant IN ('mysql', 'mariadb')),
  host TEXT NOT NULL,
  port INTEGER NOT NULL DEFAULT 3306 CHECK (port BETWEEN 1 AND 65535),
  username TEXT NOT NULL,
  password TEXT NOT NULL DEFAULT '',
  tls_mode TEXT NOT NULL DEFAULT 'preferred' CHECK (tls_mode IN ('disabled', 'preferred', 'required', 'verify-ca', 'verify-identity')),
  ca_certificate TEXT NOT NULL DEFAULT '',
  default_client_family TEXT NOT NULL CHECK (default_client_family IN ('mysql', 'mariadb')),
  default_client_dir TEXT NOT NULL,
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE
);

CREATE TABLE backup_database_options (
  backup_id TEXT PRIMARY KEY,
  scope TEXT NOT NULL CHECK (scope IN ('database', 'server')),
  database_name TEXT NOT NULL DEFAULT '',
  client_family TEXT NOT NULL DEFAULT '',
  client_dir TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (backup_id) REFERENCES backups(id) ON DELETE CASCADE,
  CHECK ((scope = 'database' AND database_name <> '') OR (scope = 'server' AND database_name = ''))
);

CREATE TABLE restore_database_options (
  restore_id TEXT PRIMARY KEY,
  destination_database TEXT NOT NULL DEFAULT '',
  replace_existing INTEGER NOT NULL DEFAULT 0 CHECK (replace_existing IN (0, 1)),
  client_family TEXT NOT NULL DEFAULT '',
  client_dir TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (restore_id) REFERENCES restores(id) ON DELETE CASCADE
);
