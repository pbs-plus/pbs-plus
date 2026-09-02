CREATE TABLE target_ldap (
  target_name TEXT PRIMARY KEY,
  host TEXT NOT NULL,
  port INTEGER NOT NULL DEFAULT 389 CHECK (port BETWEEN 1 AND 65535),
  username TEXT NOT NULL,
  password TEXT NOT NULL DEFAULT '',
  tls_mode TEXT NOT NULL DEFAULT 'starttls' CHECK (tls_mode IN ('disabled', 'starttls', 'ldaps')),
  ca_certificate TEXT NOT NULL DEFAULT '',
  base_dn TEXT NOT NULL,
  default_client_dir TEXT NOT NULL,
  FOREIGN KEY (target_name) REFERENCES targets(name) ON DELETE CASCADE
);
