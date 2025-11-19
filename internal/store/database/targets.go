//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net" // For IP address validation
	"strings"

	s3url "github.com/pbs-plus/pbs-plus/internal/backend/s3/url"
	simplebox "github.com/pbs-plus/pbs-plus/internal/store/database/secrets"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils" // Assuming this has ValidateTargetPath
	_ "modernc.org/sqlite"
)

// withTx executes a function within a transaction. If the provided tx is nil,
// it starts a new one, otherwise it uses the provided one.
// It handles commit/rollback based on the returned error.
func (database *Database) withTx(tx *sql.Tx, fn func(*sql.Tx) error) (err error) {
	var newTx bool
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		newTx = true
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				syslog.L.Error(fmt.Errorf("failed to rollback transaction: %w", rbErr)).Write()
			}
		} else if newTx { // Only commit if we started the transaction
			if cErr := tx.Commit(); cErr != nil {
				err = fmt.Errorf("failed to commit transaction: %w", cErr)
				syslog.L.Error(err).Write()
			}
		}
	}()

	err = fn(tx)
	return err
}

// resolveHost ensures the host exists in the database and returns its hostname.
func (database *Database) resolveHost(tx *sql.Tx, host *types.Host) (hostname string, err error) {
	if host.Hostname == "" {
		return "", errors.New("hostname cannot be empty")
	}

	// Try to find an existing host by hostname (now primary key)
	var existingHostname sql.NullString
	err = tx.QueryRow("SELECT hostname FROM hosts WHERE hostname = ?", host.Hostname).Scan(&existingHostname)

	if err == nil {
		// Host exists, update its details
		_, err = tx.Exec(`
            UPDATE hosts SET
                ip_address = ?,
                operating_system = ?,
                auth = ?,
                token_used = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE hostname = ?
        `,
			host.IPAddress, host.OperatingSystem, host.Auth, host.TokenUsed, host.Hostname)
		if err != nil {
			return "", fmt.Errorf("resolveHost: failed to update existing host: %w", err)
		}
		return host.Hostname, nil
	} else if errors.Is(err, sql.ErrNoRows) {
		// Host does not exist, insert new host
		_, err := tx.Exec(`
            INSERT INTO hosts (hostname, ip_address, operating_system, auth, token_used)
            VALUES (?, ?, ?, ?, ?)
        `,
			host.Hostname, host.IPAddress, host.OperatingSystem, host.Auth, host.TokenUsed)
		if err != nil {
			return "", fmt.Errorf("resolveHost: failed to insert new host: %w", err)
		}
		return host.Hostname, nil
	}
	return "", fmt.Errorf("resolveHost: error querying host: %w", err)
}

// prepareTargetForDB processes a target struct before insertion or update into the database.
// It determines target type, resolves host for agent targets, and sets the DB 'path' correctly.
func (database *Database) prepareTargetForDB(tx *sql.Tx, target *types.Target) error {
	originalTargetPath := ""
	if target.Path != nil {
		originalTargetPath = *target.Path
	}

	_, s3Err := s3url.Parse(originalTargetPath)
	isS3Path := s3Err == nil
	isAgentPath := strings.HasPrefix(originalTargetPath, "agent://")

	// Validate path only if it's not an S3 path
	if !isS3Path && originalTargetPath != "" && !utils.ValidateTargetPath(originalTargetPath) {
		return fmt.Errorf("invalid target path: %s", originalTargetPath)
	}

	// Determine TargetType
	switch {
	case isS3Path:
		target.TargetType = "s3"
	case isAgentPath:
		target.TargetType = "agent_drive" // Default, refined below
		if target.MountPoint != nil && *target.MountPoint == "/" {
			target.TargetType = "agent_root"
		}
		// Resolve Host and set host_hostname
		if target.Host == nil || target.Host.Hostname == "" {
			return fmt.Errorf("agent target must have a host with hostname")
		}

		// Extract IP from agent path for Host.IPAddress if available and not already set
		if target.Host.IPAddress == nil || *target.Host.IPAddress == "" {
			ip := extractIPFromAgentPath(originalTargetPath)
			if ip != "" {
				target.Host.IPAddress = &ip
			}
		}

		hname, hostErr := database.resolveHost(tx, target.Host)
		if hostErr != nil {
			return fmt.Errorf("failed to resolve host for agent target: %w", hostErr)
		}
		target.HostHostname = &hname
		// For agent targets, the Path in the 'targets' table is NULL
		target.Path = nil
	default:
		// Assume "local" for other valid paths (if any) or "unknown" for invalid.
		// If path is empty, and not an agent or S3, it's likely invalid unless a new type is added.
		target.TargetType = "local"
		// Ensure path is not empty for local targets
		if originalTargetPath == "" {
			return fmt.Errorf("local target path cannot be empty")
		}
	}
	return nil
}

// extractIPFromAgentPath extracts an IP address from an agent path like "agent://192.168.1.10/C"
func extractIPFromAgentPath(path string) string {
	if !strings.HasPrefix(path, "agent://") {
		return ""
	}
	hostAndPort := strings.TrimPrefix(path, "agent://")
	if idx := strings.Index(hostAndPort, "/"); idx != -1 {
		hostAndPort = hostAndPort[:idx]
	}

	// Check if it's an IPv4 or IPv6 address
	ip := net.ParseIP(hostAndPort)
	if ip != nil {
		return ip.String()
	}
	return ""
}

// CreateTarget inserts a new target or updates if it exists.
func (database *Database) CreateTarget(tx *sql.Tx, target types.Target) (err error) {
	return database.withTx(tx, func(innerTx *sql.Tx) error {
		if err := database.prepareTargetForDB(innerTx, &target); err != nil {
			return fmt.Errorf("CreateTarget: %w", err)
		}

		// Attempt INSERT
		_, err = innerTx.Exec(`
            INSERT INTO targets (name, host_hostname, mount_point, path, target_type,
                mount_script, drive_type, drive_name, drive_fs, drive_total_bytes,
                drive_used_bytes, drive_free_bytes, drive_total, drive_used, drive_free, secret_s3)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
			target.Name, target.HostHostname, target.MountPoint, target.Path, target.TargetType,
			target.MountScript, target.DriveType, target.DriveName, target.DriveFS,
			target.DriveTotalBytes, target.DriveUsedBytes, target.DriveFreeBytes,
			target.DriveTotal, target.DriveUsed, target.DriveFree, target.SecretS3,
		)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				syslog.L.Error(fmt.Errorf("CreateTarget: unique constraint failed, attempting update for target %s: %w", target.Name, err)).Write()
				return database.UpdateTarget(innerTx, target) // Use the existing transaction
			}
			return fmt.Errorf("CreateTarget: error inserting target: %w", err)
		}
		return nil
	})
}

// UpdateTarget updates an existing target.
func (database *Database) UpdateTarget(tx *sql.Tx, target types.Target) (err error) {
	return database.withTx(tx, func(innerTx *sql.Tx) error {
		if err := database.prepareTargetForDB(innerTx, &target); err != nil {
			return fmt.Errorf("UpdateTarget: %w", err)
		}

		var res sql.Result
		if target.ID != 0 {
			res, err = innerTx.Exec(`
                UPDATE targets SET
                    name = ?, host_hostname = ?, mount_point = ?, path = ?, target_type = ?,
                    mount_script = ?, drive_type = ?, drive_name = ?, drive_fs = ?,
                    drive_total_bytes = ?, drive_used_bytes = ?, drive_free_bytes = ?,
                    drive_total = ?, drive_used = ?, drive_free = ?, secret_s3 = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            `,
				target.Name, target.HostHostname, target.MountPoint, target.Path, target.TargetType,
				target.MountScript, target.DriveType, target.DriveName, target.DriveFS,
				target.DriveTotalBytes, target.DriveUsedBytes, target.DriveFreeBytes,
				target.DriveTotal, target.DriveUsed, target.DriveFree, target.SecretS3,
				target.ID,
			)
		} else if target.HostHostname != nil && target.Name != "" && (target.TargetType == "agent_drive" || target.TargetType == "agent_root") {
			// For agent targets, use the unique combination of host_hostname and name (part of unique index)
			res, err = innerTx.Exec(`
                UPDATE targets SET
                    mount_point = ?, path = ?, target_type = ?,
                    mount_script = ?, drive_type = ?, drive_name = ?, drive_fs = ?,
                    drive_total_bytes = ?, drive_used_bytes = ?, drive_free_bytes = ?,
                    drive_total = ?, drive_used = ?, drive_free = ?, secret_s3 = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE host_hostname = ? AND name = ?
            `,
				target.MountPoint, target.Path, target.TargetType,
				target.MountScript, target.DriveType, target.DriveName, target.DriveFS,
				target.DriveTotalBytes, target.DriveUsedBytes, target.DriveFreeBytes,
				target.DriveTotal, target.DriveUsed, target.DriveFree, target.SecretS3,
				target.HostHostname, target.Name,
			)
		} else if target.Path != nil && target.TargetType == "s3" {
			// For S3 targets, use the path
			res, err = innerTx.Exec(`
                UPDATE targets SET
                    name = ?, target_type = ?,
                    mount_script = ?, drive_type = ?, drive_name = ?, drive_fs = ?,
                    drive_total_bytes = ?, drive_used_bytes = ?, drive_free_bytes = ?,
                    drive_total = ?, drive_used = ?, drive_free = ?, secret_s3 = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE path = ? AND target_type = 's3'
            `,
				target.Name, target.TargetType,
				target.MountScript, target.DriveType, target.DriveName, target.DriveFS,
				target.DriveTotalBytes, target.DriveUsedBytes, target.DriveFreeBytes,
				target.DriveTotal, target.DriveUsed, target.DriveFree, target.SecretS3,
				target.Path,
			)
		} else {
			return fmt.Errorf("insufficient information to identify target for update. ID, or (host_hostname + name for agent), or path (for s3/local) required")
		}

		if err != nil {
			return fmt.Errorf("UpdateTarget: error updating target: %w", err)
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return sql.ErrNoRows // Target not found by the specified criteria
		}
		return nil
	})
}

func (database *Database) AddS3Secret(tx *sql.Tx, targetPath, secret string) (err error) {
	return database.withTx(tx, func(innerTx *sql.Tx) error {
		encrypted, err := simplebox.Encrypt(secret)
		if err != nil {
			return fmt.Errorf("AddS3Secret: error encrypting secret: %w", err)
		}

		res, err := innerTx.Exec(`
            UPDATE targets SET secret_s3 = ?, updated_at = CURRENT_TIMESTAMP
            WHERE path = ? AND target_type = 's3'
        `,
			encrypted,
			targetPath,
		)
		if err != nil {
			return fmt.Errorf("AddS3Secret: error adding secret to target: %w", err)
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return sql.ErrNoRows // S3 target with the given path not found
		}
		return nil
	})
}

// DeleteTarget removes a target.
func (database *Database) DeleteTarget(tx *sql.Tx, targetID int) (err error) {
	return database.withTx(tx, func(innerTx *sql.Tx) error {
		res, err := innerTx.Exec("DELETE FROM targets WHERE id = ?", targetID)
		if err != nil {
			return fmt.Errorf("DeleteTarget: error deleting target: %w", err)
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			return sql.ErrNoRows // Return sql.ErrNoRows if the target wasn't found
		}
		return nil
	})
}

// scanTargetRow scans a sql.Row or sql.Rows into a types.Target struct.
func (database *Database) scanTargetRow(scanner interface{ Scan(...interface{}) error }, withJobCount bool) (types.Target, error) {
	var target types.Target
	var hostHostname sql.NullString
	var mountPoint sql.NullString
	var targetPath sql.NullString // target.Path is now nullable
	var mountScript sql.NullString
	var driveType sql.NullString
	var driveName sql.NullString
	var driveFS sql.NullString
	var driveTotalBytes sql.NullInt64
	var driveUsedBytes sql.NullInt64
	var driveFreeBytes sql.NullInt64
	var driveTotal sql.NullString
	var driveUsed sql.NullString
	var driveFree sql.NullString
	var secretS3 sql.NullString

	var hostnameFromHostTable sql.NullString
	var ipAddressFromHostTable sql.NullString
	var operatingSystem sql.NullString
	var auth sql.NullString
	var tokenUsed sql.NullString
	var hostCreatedAt sql.NullTime
	var hostUpdatedAt sql.NullTime

	scanArgs := []interface{}{
		&target.ID, &target.Name, &hostHostname, &mountPoint, &targetPath, &target.TargetType,
		&mountScript, &driveType, &driveName, &driveFS,
		&driveTotalBytes, &driveUsedBytes, &driveFreeBytes,
		&driveTotal, &driveUsed, &driveFree, &secretS3,
		&target.CreatedAt, &target.UpdatedAt,
		&hostnameFromHostTable, &ipAddressFromHostTable, &operatingSystem, &auth, &tokenUsed,
		&hostCreatedAt, &hostUpdatedAt,
	}

	var jobCount int
	if withJobCount {
		scanArgs = append(scanArgs, &jobCount)
	}

	err := scanner.Scan(scanArgs...)
	if err != nil {
		return types.Target{}, err
	}

	// Assign nullable fields
	if hostHostname.Valid {
		target.HostHostname = &hostHostname.String
	}
	if mountPoint.Valid {
		target.MountPoint = &mountPoint.String
	}
	if targetPath.Valid {
		target.Path = &targetPath.String
	}
	if mountScript.Valid {
		target.MountScript = &mountScript.String
	}
	if driveType.Valid {
		target.DriveType = &driveType.String
	}
	if driveName.Valid {
		target.DriveName = &driveName.String
	}
	if driveFS.Valid {
		target.DriveFS = &driveFS.String
	}
	if driveTotalBytes.Valid {
		val := driveTotalBytes.Int64
		target.DriveTotalBytes = &val
	}
	if driveUsedBytes.Valid {
		val := driveUsedBytes.Int64
		target.DriveUsedBytes = &val
	}
	if driveFreeBytes.Valid {
		val := driveFreeBytes.Int64
		target.DriveFreeBytes = &val
	}
	if driveTotal.Valid {
		target.DriveTotal = &driveTotal.String
	}
	if driveUsed.Valid {
		target.DriveUsed = &driveUsed.String
	}
	if driveFree.Valid {
		target.DriveFree = &driveFree.String
	}
	if secretS3.Valid {
		target.SecretS3 = &secretS3.String
	}

	// Assign embedded Host fields
	if hostnameFromHostTable.Valid { // If a host was found via LEFT JOIN
		target.Host = &types.Host{}
		target.Host.Hostname = hostnameFromHostTable.String
		if ipAddressFromHostTable.Valid {
			target.Host.IPAddress = &ipAddressFromHostTable.String
		}
		if operatingSystem.Valid {
			target.Host.OperatingSystem = operatingSystem.String
		}
		if auth.Valid {
			target.Host.Auth = &auth.String
		}
		if tokenUsed.Valid {
			target.Host.TokenUsed = &tokenUsed.String
		}
		if hostCreatedAt.Valid {
			target.Host.CreatedAt = hostCreatedAt.Time
		}
		if hostUpdatedAt.Valid {
			target.Host.UpdatedAt = hostUpdatedAt.Time
		}
	}

	if withJobCount {
		target.JobCount = jobCount
	}

	// Reconstruct Path for agent targets
	reconstructAgentTargetPath(&target)

	return target, nil
}

// reconstructAgentTargetPath sets the Path field for agent targets based on Host details.
func reconstructAgentTargetPath(target *types.Target) {
	if target == nil {
		return
	}
	if strings.HasPrefix(target.TargetType, "agent") && target.Host != nil {
		hostIdentifier := ""
		if target.Host.IPAddress != nil && *target.Host.IPAddress != "" {
			hostIdentifier = *target.Host.IPAddress
		} else if target.Host.Hostname != "" {
			hostIdentifier = target.Host.Hostname
		}

		if hostIdentifier != "" {
			// For agent_root, MountPoint is usually "/", for agent_drive it's like "C"
			mountPart := ""
			if target.MountPoint != nil && *target.MountPoint != "" && *target.MountPoint != "/" {
				mountPart = "/" + strings.TrimSuffix(*target.MountPoint, ":")
			} else if target.TargetType == "agent_root" {
				mountPart = "/Root" // Consistent representation for root
			} else if target.Name != "" {
				// Fallback to name if mount_point is not explicitly set for drive, might be "C", "D"
				mountPart = "/" + target.Name
			}

			pathVal := fmt.Sprintf("agent://%s%s", hostIdentifier, mountPart)
			target.Path = &pathVal
		}
	}
}

// getTargetInternal is a private helper to retrieve a single target.
func (database *Database) getTargetInternal(query string, args ...interface{}) (types.Target, error) {
	row := database.readDb.QueryRow(query, args...)
	target, err := database.scanTargetRow(row, true)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Target{}, sql.ErrNoRows
		}
		return types.Target{}, fmt.Errorf("GetTarget: error fetching target: %w", err)
	}
	return target, nil
}

// GetTargetByID retrieves a target by its unique ID.
func (database *Database) GetTargetByID(targetID int) (types.Target, error) {
	query := `
        SELECT
            t.id, t.name, t.host_hostname, t.mount_point, t.path, t.target_type,
            t.mount_script, t.drive_type, t.drive_name, t.drive_fs,
            t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free, t.secret_s3,
            t.created_at, t.updated_at,
            h.hostname, h.ip_address, h.operating_system, h.auth, h.token_used,
            h.created_at, h.updated_at,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN hosts h ON t.host_hostname = h.hostname
        LEFT JOIN jobs j ON t.id = j.target_id
        WHERE t.id = ?
        GROUP BY t.id, h.hostname
    `
	return database.getTargetInternal(query, targetID)
}

// GetTargetByHostAndName retrieves an agent target by host hostname and target name.
func (database *Database) GetTargetByHostAndName(hostHostname, targetName string) (types.Target, error) {
	query := `
        SELECT
            t.id, t.name, t.host_hostname, t.mount_point, t.path, t.target_type,
            t.mount_script, t.drive_type, t.drive_name, t.drive_fs,
            t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free, t.secret_s3,
            t.created_at, t.updated_at,
            h.hostname, h.ip_address, h.operating_system, h.auth, h.token_used,
            h.created_at, h.updated_at,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN hosts h ON t.host_hostname = h.hostname
        LEFT JOIN jobs j ON t.id = j.target_id
        WHERE t.host_hostname = ? AND t.name = ? AND t.target_type LIKE 'agent%'
        GROUP BY t.id, h.hostname
    `
	return database.getTargetInternal(query, hostHostname, targetName)
}

// GetTargetByPath retrieves a non-agent target by its path.
func (database *Database) GetTargetByPath(targetPath string) (types.Target, error) {
	query := `
        SELECT
            t.id, t.name, t.host_hostname, t.mount_point, t.path, t.target_type,
            t.mount_script, t.drive_type, t.drive_name, t.drive_fs,
            t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free, t.secret_s3,
            t.created_at, t.updated_at,
            h.hostname, h.ip_address, h.operating_system, h.auth, h.token_used,
            h.created_at, h.updated_at,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN hosts h ON t.host_hostname = h.hostname
        LEFT JOIN jobs j ON t.id = j.target_id
        WHERE t.path = ? AND t.target_type NOT LIKE 'agent%'
        GROUP BY t.id, h.hostname
    `
	return database.getTargetInternal(query, targetPath)
}

func (database *Database) GetUniqueAuthByHostname(hostname string) ([]string, error) {
	rows, err := database.readDb.Query(`
        SELECT DISTINCT auth
        FROM hosts
        WHERE hostname = ?
        AND auth IS NOT NULL AND auth != ''
        ORDER BY auth
    `, hostname)
	if err != nil {
		return nil, fmt.Errorf("GetUniqueAuthByHostname: error querying auth values: %w", err)
	}
	defer rows.Close()

	var authList []string
	for rows.Next() {
		var auth string
		err := rows.Scan(&auth)
		if err != nil {
			return nil, fmt.Errorf("GetUniqueAuthByHostname: error scanning auth value: %w", err)
		}
		authList = append(authList, auth)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetUniqueAuthByHostname: error iterating rows: %w", err)
	}

	return authList, nil
}

func (database *Database) GetS3Secret(targetPath string) (string, error) {
	row := database.readDb.QueryRow(`
        SELECT
            t.secret_s3
        FROM targets t
        WHERE t.path = ? AND t.target_type = 's3'
    `, targetPath)

	var secretS3 sql.NullString
	err := row.Scan(
		&secretS3,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", sql.ErrNoRows
		}
		return "", fmt.Errorf("GetS3Secret: error fetching secret for target: %w", err)
	}

	if !secretS3.Valid || secretS3.String == "" {
		return "", sql.ErrNoRows // Or a more specific "secret not set" error
	}

	decrypted, err := simplebox.Decrypt(secretS3.String)
	if err != nil {
		return "", fmt.Errorf("GetS3Secret: failed to decrypt secret: %w", err)
	}

	return decrypted, nil
}

// GetAllTargets returns all targets along with their associated job counts.
func (database *Database) GetAllTargets() ([]types.Target, error) {
	rows, err := database.readDb.Query(`
        SELECT
            t.id, t.name, t.host_hostname, t.mount_point, t.path, t.target_type,
            t.mount_script, t.drive_type, t.drive_name, t.drive_fs,
            t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free, t.secret_s3,
            t.created_at, t.updated_at,
            h.hostname, h.ip_address, h.operating_system, h.auth, h.token_used,
            h.created_at, h.updated_at,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN hosts h ON t.host_hostname = h.hostname
        LEFT JOIN jobs j ON t.id = j.target_id
        GROUP BY t.id, h.hostname
        ORDER BY t.name
    `)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		target, err := database.scanTargetRow(rows, true)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllTargets: error scanning target row: %w", err)).Write()
			continue
		}
		targets = append(targets, target)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllTargets: error iterating target rows: %w", err)
	}

	return targets, nil
}

// GetAllTargetsByIP returns all agent targets matching the given client IP.
func (database *Database) GetAllTargetsByIP(clientIP string) ([]types.Target, error) {
	rows, err := database.readDb.Query(`
		SELECT
            t.id, t.name, t.host_hostname, t.mount_point, t.path, t.target_type,
            t.mount_script, t.drive_type, t.drive_name, t.drive_fs,
            t.drive_total_bytes, t.drive_used_bytes, t.drive_free_bytes,
            t.drive_total, t.drive_used, t.drive_free, t.secret_s3,
            t.created_at, t.updated_at,
            h.hostname, h.ip_address, h.operating_system, h.auth, h.token_used,
            h.created_at, h.updated_at
		FROM targets t
        JOIN hosts h ON t.host_hostname = h.hostname
		WHERE h.ip_address = ? AND t.target_type LIKE 'agent%'
		ORDER BY t.name
		`, clientIP)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		target, err := database.scanTargetRow(rows, false) // No job count for this query
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllTargetsByIP: error scanning target row: %w", err)).Write()
			continue
		}
		targets = append(targets, target)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error iterating target rows: %w", err)
	}

	return targets, nil
}
