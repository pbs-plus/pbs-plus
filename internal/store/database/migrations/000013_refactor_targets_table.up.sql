-- Step 1: Create the new 'hosts' table with hostname as PRIMARY KEY and add ip_address
CREATE TABLE hosts (
    hostname TEXT NOT NULL PRIMARY KEY, -- hostname is now the PRIMARY KEY
    ip_address TEXT,                    -- New column for IP address
    operating_system TEXT NOT NULL,
    auth TEXT,
    token_used TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Rename the existing 'targets' table to 'old_targets'
ALTER TABLE targets RENAME TO old_targets;

-- Step 3: Create the new 'targets' table with the updated schema
-- IMPORTANT: The 'name' column no longer has the UNIQUE constraint
CREATE TABLE targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,           -- NO LONGER UNIQUE! This will be the simplified label (e.g., "C", "Root").
    host_hostname TEXT,           -- Foreign key to hosts table (NULL for S3 or non-agent targets), references hostname
    mount_point TEXT,             -- e.g., "C:", "/", "/mnt/data" (NULL for S3). This is the full path. (Renamed from 'path' in previous iteration)
    path TEXT,                    -- The original path for non-agent targets, NULL for agent targets
    target_type TEXT NOT NULL,    -- e.g., 'agent_drive', 'agent_root', 's3'
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
    secret_s3 TEXT,               -- Made nullable, only for 's3' target_type
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (host_hostname) REFERENCES hosts(hostname) ON DELETE CASCADE
);

-- Step 4: Create a temporary table to store parsed old target data using a subquery
CREATE TEMPORARY TABLE temp_parsed_old_targets AS
SELECT
    inner_tpt.*, -- Select all columns from the inner subquery

    -- Derived Display Name (for targets.name column - simplified label without colon)
    CASE
        WHEN inner_tpt.old_path LIKE 's3://%' THEN SUBSTR(inner_tpt.old_path, INSTR(inner_tpt.old_path, '//') + 2) -- Full S3 path/bucket name as name
        ELSE inner_tpt.mount_suffix_base -- Use the simplified suffix without the colon
    END AS derived_display_name,

    -- Derived Mount Point Path (for targets.mount_point column - full path, includes colon for Windows)
    CASE
        WHEN inner_tpt.old_path LIKE 's3://%' THEN inner_tpt.old_path -- Full S3 path
        WHEN inner_tpt.old_os LIKE 'windows' AND inner_tpt.mount_suffix_base IS NOT NULL AND inner_tpt.mount_suffix_base != '' THEN inner_tpt.mount_suffix_base || ':' -- Add colon for Windows drives (e.g., "C:")
        WHEN inner_tpt.mount_suffix_base = 'Root' THEN '/' -- Map 'Root' to '/'
        WHEN inner_tpt.mount_suffix_base IS NOT NULL AND inner_tpt.mount_suffix_base != '' THEN inner_tpt.mount_suffix_base -- Other custom mount names or Linux paths
        ELSE NULL -- No specific mount point path for other generic types
    END AS derived_mount_point_path,

    -- Determine the target_type early for conditional path handling
    CASE
        WHEN inner_tpt.old_path LIKE 's3://%' THEN 's3'
        WHEN inner_tpt.old_path LIKE 'agent://%' AND (inner_tpt.mount_suffix_base = 'Root' OR inner_tpt.mount_suffix_base = '') THEN 'agent_root'
        WHEN inner_tpt.old_path LIKE 'agent://%' THEN 'agent_drive'
        ELSE 'local'
    END AS derived_target_type_temp,

    -- Conditional path for the new 'targets' table (NULL for agents)
    CASE
        WHEN inner_tpt.old_path LIKE 'agent://%' THEN NULL
        ELSE inner_tpt.old_path
    END AS derived_target_path_for_new_table
FROM (
    SELECT
        ot.name AS old_full_name, -- The original full name (e.g., "ServerA - C:")
        ot.path AS old_path,
        ot.os AS old_os,
        ot.auth AS old_auth,
        ot.token_used AS old_token,
        ot.mount_script,
        ot.drive_type,
        ot.drive_name,
        ot.drive_fs,
        ot.drive_total_bytes,
        ot.drive_used_bytes,
        ot.drive_free_bytes,
        ot.drive_total,
        ot.drive_used,
        ot.drive_free,
        ot.secret_s3,

        -- Derived Hostname logic
        CASE
            WHEN INSTR(ot.name, ' - ') > 0 THEN SUBSTR(ot.name, 1, INSTR(ot.name, ' - ') - 1)
            WHEN ot.path LIKE 'agent://%' THEN -- Handles Windows/Linux agent paths like agent://IP/C or agent://hostname/root
                 SUBSTR(ot.path, INSTR(ot.path, '//') + 2,
                        CASE WHEN INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '/') > 0
                             THEN INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '/') - 1
                             ELSE LENGTH(SUBSTR(ot.path, INSTR(ot.path, '//') + 2)) END)
            ELSE NULL
        END AS derived_hostname,

        -- Derived IP Address logic (from agent path)
        CASE
            WHEN ot.path LIKE 'agent://%' AND INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '.') > 0 THEN
                SUBSTR(ot.path, INSTR(ot.path, '//') + 2,
                       CASE WHEN INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '/') > 0
                            THEN INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '/') - 1
                            ELSE LENGTH(SUBSTR(ot.path, INSTR(ot.path, '//') + 2)) END)
            ELSE NULL
        END AS derived_ip_address,

        -- Intermediate extraction for mount suffix (e.g., "C", "Root") - NO COLON HERE
        CASE
            WHEN INSTR(ot.name, ' - ') > 0 THEN SUBSTR(ot.name, INSTR(ot.name, ' - ') + 3)
            WHEN ot.path LIKE 'agent://%' THEN
                -- Get the segment after the hostname/IP in the path (e.g., "root" from agent://10.1.99.5/root, or "C" from agent://IP/C)
                SUBSTR(ot.path, INSTR(ot.path, '//') + 2 +
                                 CASE WHEN INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '/') > 0
                                      THEN INSTR(SUBSTR(ot.path, INSTR(ot.path, '//') + 2), '/')
                                      ELSE LENGTH(SUBSTR(ot.path, INSTR(ot.path, '//') + 2)) + 1 END)
            ELSE NULL
        END AS mount_suffix_base
    FROM old_targets AS ot
) AS inner_tpt;


-- Step 5: Populate the 'hosts' table from the temporary parsed data
INSERT OR IGNORE INTO hosts (hostname, ip_address, operating_system, auth, token_used)
SELECT DISTINCT
    derived_hostname,
    derived_ip_address,
    old_os,
    old_auth,
    old_token
FROM temp_parsed_old_targets
WHERE derived_hostname IS NOT NULL AND derived_hostname != '';


-- Step 6: Migrate data from 'old_targets' (via temp_parsed_old_targets) to 'targets'
INSERT INTO targets (
    name, host_hostname, mount_point, path, target_type,
    mount_script,
    drive_type, drive_name, drive_fs, drive_total_bytes,
    drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free,
    secret_s3
)
SELECT
    tpt.derived_display_name,               -- 'name' is now the simplified label (e.g., "C", "Root")
    tpt.derived_hostname AS host_hostname,  -- Use the derived hostname directly
    tpt.derived_mount_point_path,           -- 'mount_point' is the actual path (e.g., "C:", "/", "/mnt/data")
    tpt.derived_target_path_for_new_table,  -- Conditional path (NULL for agents)
    tpt.derived_target_type_temp,           -- Derived target type
    tpt.mount_script,
    tpt.drive_type,
    tpt.drive_name,
    tpt.drive_fs,
    tpt.drive_total_bytes,
    tpt.drive_used_bytes,
    tpt.drive_free_bytes,
    tpt.drive_total,
    tpt.drive_used,
    tpt.drive_free,
    NULLIF(tpt.secret_s3, '') -- Insert secret_s3 only if not empty string
FROM temp_parsed_old_targets AS tpt;

-- Step 7: Drop the 'old_targets' table
DROP TABLE old_targets;

-- Step 8: Drop the temporary table
DROP TABLE temp_parsed_old_targets;

-- Step 9: Create indexes for the new tables
-- hostname is now the primary key, so no additional index needed on hosts(hostname)
-- No index needed for targets.name if it's not unique and not frequently queried by itself.
CREATE INDEX idx_targets_host_hostname ON targets(host_hostname);
CREATE INDEX idx_targets_path ON targets(path);
CREATE UNIQUE INDEX idx_targets_host_hostname_mount_point ON targets(host_hostname, name) WHERE host_hostname IS NOT NULL;
