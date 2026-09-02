-- name: CreateRestore :exec
INSERT INTO restores (
    id, store, namespace, snapshot, src_path, dest_target, dest_subpath,
    comment, current_pid, last_run_upid, last_successful_upid, retry,
    retry_interval, pre_script, post_script, restore_mode, last_run_status, retry_count,
    notification_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetRestore :one
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target,
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid,
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode, j.last_run_status, j.retry_count, j.notification_mode,
    COALESCE(dro.source_database, '') AS source_database,
    COALESCE(dro.destination_database, '') AS destination_database,
    COALESCE(dro.replace_existing, 0) AS replace_existing,
    COALESCE(dro.client_family, '') AS database_client_family,
    COALESCE(dro.client_dir, '') AS database_client_dir,
    COALESCE(dvo.source_username, '') AS dovecot_source_username,
    COALESCE(dvo.destination_username, '') AS dovecot_destination_username,
    COALESCE(dvo.mailbox, '') AS dovecot_mailbox,
    COALESCE(dvo.replace_existing, 0) AS dovecot_replace_existing,
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    COALESCE(p.host, m.host, l.host, d.host, '') AS database_host,
    COALESCE(p.port, m.port, l.port, d.port, 0) AS database_port,
    COALESCE(p.username, m.username, l.username, '') AS database_username,
    COALESCE(p.ssl_mode, m.tls_mode, l.tls_mode, '') AS database_tls_mode,
    COALESCE(p.ca_certificate, m.ca_certificate, l.ca_certificate, d.ca_certificate, '') AS database_ca_certificate,
    COALESCE(p.default_client_dir, m.default_client_dir, l.default_client_dir, d.default_client_dir, '') AS database_default_client_dir,
    COALESCE(m.variant, '') AS database_variant,
    COALESCE(m.default_client_family, '') AS database_default_client_family,
    COALESCE(l.base_dn, '') AS ldap_base_dn,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth,
    ah.token_used as agent_token_used, ah.os as agent_os
FROM restores j
LEFT JOIN restore_database_options dro ON dro.restore_id = j.id
LEFT JOIN restore_dovecot_options dvo ON dvo.restore_id = j.id
LEFT JOIN targets t ON j.dest_target = t.name
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN target_postgresql p ON p.target_name = t.name
LEFT JOIN target_mysql m ON m.target_name = t.name
LEFT JOIN target_ldap l ON l.target_name = t.name
LEFT JOIN target_dovecot d ON d.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
WHERE j.id = ?
LIMIT 1;

-- name: ListAllRestores :many
SELECT
    j.id, j.store, j.namespace, j.snapshot, j.src_path, j.dest_target,
    j.dest_subpath, j.comment, j.current_pid, j.last_run_upid,
    j.last_successful_upid, j.retry, j.retry_interval, j.pre_script, j.post_script,
    j.restore_mode, j.last_run_status, j.retry_count, j.notification_mode,
    COALESCE(dro.source_database, '') AS source_database,
    COALESCE(dro.destination_database, '') AS destination_database,
    COALESCE(dro.replace_existing, 0) AS replace_existing,
    COALESCE(dro.client_family, '') AS database_client_family,
    COALESCE(dro.client_dir, '') AS database_client_dir,
    COALESCE(dvo.source_username, '') AS dovecot_source_username,
    COALESCE(dvo.destination_username, '') AS dovecot_destination_username,
    COALESCE(dvo.mailbox, '') AS dovecot_mailbox,
    COALESCE(dvo.replace_existing, 0) AS dovecot_replace_existing,
    t.name, t.target_type, t.mount_script,
    COALESCE(f.access, '') AS filesystem_access,
    COALESCE(f.path, s.url, '') AS path,
    f.agent_host, f.volume_id, f.volume_type, f.volume_name,
    f.volume_fs, f.volume_total_bytes, f.volume_used_bytes, f.volume_free_bytes,
    f.volume_total, f.volume_used, f.volume_free,
    COALESCE(p.host, m.host, l.host, d.host, '') AS database_host,
    COALESCE(p.port, m.port, l.port, d.port, 0) AS database_port,
    COALESCE(p.username, m.username, l.username, '') AS database_username,
    COALESCE(p.ssl_mode, m.tls_mode, l.tls_mode, '') AS database_tls_mode,
    COALESCE(p.ca_certificate, m.ca_certificate, l.ca_certificate, d.ca_certificate, '') AS database_ca_certificate,
    COALESCE(p.default_client_dir, m.default_client_dir, l.default_client_dir, d.default_client_dir, '') AS database_default_client_dir,
    COALESCE(m.variant, '') AS database_variant,
    COALESCE(m.default_client_family, '') AS database_default_client_family,
    COALESCE(l.base_dn, '') AS ldap_base_dn,
    ah.name as agent_name, ah.ip as agent_ip, ah.auth as agent_auth,
    ah.token_used as agent_token_used, ah.os as agent_os
FROM restores j
LEFT JOIN restore_database_options dro ON dro.restore_id = j.id
LEFT JOIN restore_dovecot_options dvo ON dvo.restore_id = j.id
LEFT JOIN targets t ON j.dest_target = t.name
LEFT JOIN target_filesystems f ON f.target_name = t.name
LEFT JOIN target_s3 s ON s.target_name = t.name
LEFT JOIN target_postgresql p ON p.target_name = t.name
LEFT JOIN target_mysql m ON m.target_name = t.name
LEFT JOIN target_ldap l ON l.target_name = t.name
LEFT JOIN target_dovecot d ON d.target_name = t.name
LEFT JOIN agent_hosts ah ON f.agent_host = ah.name
ORDER BY j.id;

-- name: UpdateRestore :exec
UPDATE restores
SET store = ?, namespace = ?, snapshot = ?, src_path = ?, dest_target = ?,
    dest_subpath = ?, comment = ?, current_pid = ?, last_run_upid = ?,
    retry = ?, retry_interval = ?, last_successful_upid = ?,
    pre_script = ?, post_script = ?, restore_mode = ?, last_run_status = ?, retry_count = ?,
    notification_mode = ?
WHERE id = ?;

-- name: UpsertRestoreDatabaseOptions :exec
INSERT INTO restore_database_options (
    restore_id, source_database, destination_database, replace_existing, client_family, client_dir
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(restore_id) DO UPDATE SET
    source_database = excluded.source_database,
    destination_database = excluded.destination_database,
    replace_existing = excluded.replace_existing,
    client_family = excluded.client_family,
    client_dir = excluded.client_dir;

-- name: DeleteRestoreDatabaseOptions :exec
DELETE FROM restore_database_options WHERE restore_id = ?;

-- name: UpsertRestoreDovecotOptions :exec
INSERT INTO restore_dovecot_options (
    restore_id, source_username, destination_username, mailbox, replace_existing
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(restore_id) DO UPDATE SET
    source_username = excluded.source_username,
    destination_username = excluded.destination_username,
    mailbox = excluded.mailbox,
    replace_existing = excluded.replace_existing;

-- name: DeleteRestoreDovecotOptions :exec
DELETE FROM restore_dovecot_options WHERE restore_id = ?;

-- name: DeleteRestore :execrows
DELETE FROM restores WHERE id = ?;

-- name: RestoreExists :one
SELECT 1 FROM restores WHERE id = ? LIMIT 1;

-- name: CountRestores :one
SELECT COUNT(*) FROM restores;
