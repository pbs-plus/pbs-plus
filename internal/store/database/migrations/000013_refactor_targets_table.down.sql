-- Step 1: Drop indexes created in the up migration
DROP INDEX IF EXISTS idx_targets_host_hostname;
DROP INDEX IF EXISTS idx_targets_path;
DROP INDEX IF EXISTS idx_targets_host_hostname_mount_point;

-- Step 2: Rename the new 'targets' table temporarily to facilitate reconstruction
ALTER TABLE targets RENAME TO new_targets_to_revert;

-- Step 3: Recreate the original 'targets' table schema (what 'old_targets' was before drop)
CREATE TABLE targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,           -- Original schema had UNIQUE constraint
    path TEXT NOT NULL,
    os TEXT NOT NULL,                    -- 'os' was directly on targets
    auth TEXT,                           -- 'auth' was directly on targets
    token_used TEXT,                     -- 'token_used' was directly on targets
    mount_script TEXT,
    drive_type TEXT,
    drive_name TEXT,
    drive_fs TEXT,
    drive_total_bytes INTEGER,
    drive_used_bytes INTEGER,
    drive_free_bytes INTEGER,
    drive_total TEXT,
    drive_used TEXT,
    drive_free TEXT,
    secret_s3 TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 4: Populate the original 'targets' table from 'new_targets_to_revert' and 'hosts'
-- This step reconstructs the 'name', 'path', 'os', 'auth', and 'token_used' columns.
INSERT INTO targets (
    id,
    name,
    path,
    os,
    auth,
    token_used,
    mount_script,
    drive_type,
    drive_name,
    drive_fs,
    drive_total_bytes,
    drive_used_bytes,
    drive_free_bytes,
    drive_total,
    drive_used,
    drive_free,
    secret_s3,
    created_at,
    updated_at
)
SELECT
    ntr.id,
    -- Reconstruct the original 'name' column (e.g., "Hostname - C:", "S3 Bucket")
    CASE
        WHEN ntr.target_type = 's3' THEN
            -- For S3, the original 'name' was often the path itself.
            ntr.mount_point
        WHEN ntr.target_type IN ('agent_drive', 'agent_root') THEN
            h.hostname || ' - ' ||
            CASE
                WHEN ntr.mount_point = '/' THEN 'Root'
                WHEN ntr.mount_point IS NOT NULL THEN REPLACE(ntr.mount_point, ':', '')
                ELSE '' -- Should ideally not happen for agent targets
            END
        ELSE
            -- Fallback for local or unknown types, use the simplified name or mount_point
            COALESCE(ntr.name, ntr.mount_point, '')
    END AS original_name,
    -- Reconstruct the original 'path' column
    CASE
        WHEN ntr.target_type = 's3' THEN ntr.path -- S3 path was kept in ntr.path
        WHEN ntr.target_type IN ('agent_drive', 'agent_root') THEN
             -- Reconstruct agent path using hostname/IP and mount_point
             'agent://' || COALESCE(h.ip_address, h.hostname) ||
             CASE
                 WHEN ntr.mount_point = '/' OR ntr.mount_point IS NULL OR ntr.mount_point = '' THEN '/Root'
                 ELSE '/' || REPLACE(ntr.mount_point, ':', '')
             END
        ELSE ntr.path -- Use path directly for other types if available
    END AS original_path,
    h.operating_system AS os,   -- Get OS from the hosts table
    h.auth AS auth,             -- Get Auth from the hosts table
    h.token_used AS token_used, -- Get Token_used from the hosts table
    ntr.mount_script,
    ntr.drive_type,
    ntr.drive_name,
    ntr.drive_fs,
    ntr.drive_total_bytes,
    ntr.drive_used_bytes,
    ntr.drive_free_bytes,
    ntr.drive_total,
    ntr.drive_used,
    ntr.drive_free,
    ntr.secret_s3,
    ntr.created_at,
    ntr.updated_at
FROM
    new_targets_to_revert AS ntr
LEFT JOIN
    hosts AS h ON ntr.host_hostname = h.hostname;


-- Step 5: Drop the new 'hosts' table
DROP TABLE IF EXISTS hosts;

-- Step 6: Drop the temporary 'new_targets_to_revert' table
DROP TABLE IF EXISTS new_targets_to_revert;
