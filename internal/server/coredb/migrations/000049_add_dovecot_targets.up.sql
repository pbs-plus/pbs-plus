CREATE TABLE target_dovecot (
  target_name TEXT PRIMARY KEY,
  host TEXT NOT NULL,
  port INTEGER NOT NULL DEFAULT 24245 CHECK (port BETWEEN 1 AND 65535),
  password TEXT NOT NULL DEFAULT '',
  ca_certificate TEXT NOT NULL,
  default_client_dir TEXT NOT NULL,
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE
);

CREATE TABLE backup_dovecot_options (
  backup_id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  mailbox TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (backup_id) REFERENCES backups(id) ON DELETE CASCADE
);

CREATE TABLE restore_dovecot_options (
  restore_id TEXT PRIMARY KEY,
  source_username TEXT NOT NULL DEFAULT '',
  destination_username TEXT NOT NULL DEFAULT '',
  mailbox TEXT NOT NULL DEFAULT '',
  replace_existing INTEGER NOT NULL DEFAULT 0 CHECK (replace_existing IN (0, 1)),
  FOREIGN KEY (restore_id) REFERENCES restores(id) ON DELETE CASCADE
);
