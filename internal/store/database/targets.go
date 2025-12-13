//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	simplebox "github.com/pbs-plus/pbs-plus/internal/store/database/secrets"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	_ "modernc.org/sqlite"
)

func (database *Database) CreateTarget(tx *sql.Tx, target types.Target) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("CreateTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateTarget: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if err := target.Validate(); err != nil {
		return err
	}

	_, err = tx.Exec(`
    INSERT INTO targets (
			name, target_type, agent_host, s3_access_id, s3_host, s3_bucket, s3_secret,
			s3_region, s3_ssl, s3_path_style, local_path, auth, token_used, os, mount_script
		)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
		target.Name, target.TargetType, target.AgentHost,
		target.S3AccessID, target.S3Host, target.S3Bucket, target.S3Secret,
		target.S3Region, target.S3UseSSL, target.S3UsePathStyle,
		target.LocalPath, target.Auth, target.TokenUsed, target.OperatingSystem, target.MountScript,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("CreateTarget: error inserting target: %w", err)
		}
		_, err = tx.Exec(`
			UPDATE targets SET
				target_type = ?, agent_host = ?, s3_access_id = ?, s3_host = ?, s3_bucket = ?, s3_secret = ?,
				s3_region = ?, s3_ssl = ?, s3_path_style = ?, local_path = ?, auth = ?, token_used = ?, os = ?, mount_script = ?
			WHERE name = ?
		`,
			target.TargetType, target.AgentHost,
			target.S3AccessID, target.S3Host, target.S3Bucket, target.S3Secret,
			target.S3Region, target.S3UseSSL, target.S3UsePathStyle,
			target.LocalPath, target.Auth, target.TokenUsed, target.OperatingSystem, target.MountScript,
			target.Name,
		)
		if err != nil {
			return fmt.Errorf("CreateTarget: error upserting target: %w", err)
		}
	}

	if len(target.Volumes) > 0 {
		for _, v := range target.Volumes {
			_, err = tx.Exec(`
				INSERT INTO volumes (
					target_name, volume_name, meta_type, meta_name, meta_fs,
					meta_total_bytes, meta_used_bytes, meta_free_bytes, meta_total, meta_used, meta_free
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(target_name, volume_name) DO UPDATE SET
					meta_type=excluded.meta_type,
					meta_name=excluded.meta_name,
					meta_fs=excluded.meta_fs,
					meta_total_bytes=excluded.meta_total_bytes,
					meta_used_bytes=excluded.meta_used_bytes,
					meta_free_bytes=excluded.meta_free_bytes,
					meta_total=excluded.meta_total,
					meta_used=excluded.meta_used,
					meta_free=excluded.meta_free
			`,
				target.Name, v.VolumeName, v.MetaType, v.MetaName, v.MetaFS,
				v.MetaTotalBytes, v.MetaUsedBytes, v.MetaFreeBytes, v.MetaTotal, v.MetaUsed, v.MetaFree,
			)
			if err != nil {
				return fmt.Errorf("CreateTarget: error upserting volume %q: %w", v.VolumeName, err)
			}
		}
	}

	commitNeeded = true
	return nil
}

func (database *Database) UpdateTarget(tx *sql.Tx, target types.Target) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateTarget: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	if err := target.Validate(); err != nil {
		return err
	}

	_, err = tx.Exec(`
      UPDATE targets SET
			target_type = ?, agent_host = ?, s3_access_id = ?, s3_host = ?, s3_bucket = ?, s3_secret = ?,
			s3_region = ?, s3_ssl = ?, s3_path_style = ?, local_path = ?, auth = ?, token_used = ?, os = ?, mount_script = ?
        WHERE name = ?
    `,
		target.TargetType, target.AgentHost,
		target.S3AccessID, target.S3Host, target.S3Bucket, target.S3Secret,
		target.S3Region, target.S3UseSSL, target.S3UsePathStyle,
		target.LocalPath, target.Auth, target.TokenUsed, target.OperatingSystem, target.MountScript,
		target.Name,
	)
	if err != nil {
		return fmt.Errorf("UpdateTarget: error updating target: %w", err)
	}

	if len(target.Volumes) > 0 {
		err = database.UpdateTargetVolumes(tx, target.Name, target.Volumes)
		if err != nil {
			return fmt.Errorf("UpdateTarget: error updating target volume list %q: %w", target.Name, err)
		}
	}

	commitNeeded = true
	return nil
}

func (database *Database) AddS3Secret(tx *sql.Tx, targetName, secret string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("AddSecret: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("AddSecret: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("AddSecret: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("AddSecret: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	encrypted, err := simplebox.Encrypt(secret)
	if err != nil {
		return fmt.Errorf("AddSecret: error encrypting secret: %w", err)
	}

	_, err = tx.Exec(`
        UPDATE targets SET s3_secret = ? WHERE name = ?
    `,
		encrypted,
		targetName,
	)
	if err != nil {
		return fmt.Errorf("AddSecret: error adding secret to target: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) DeleteTarget(tx *sql.Tx, name string) (err error) {
	var commitNeeded bool = false
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("DeleteTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteTarget: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteTarget: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}

	res, err := tx.Exec("DELETE FROM targets WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("DeleteTarget: error deleting target: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return ErrTargetNotFound
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetTarget(name string) (types.Target, error) {
	row := database.readDb.QueryRow(`
        SELECT
            t.name, t.target_type, t.agent_host, t.s3_access_id, t.s3_host, t.s3_bucket, t.s3_secret,
						t.s3_region, t.s3_ssl, t.s3_path_style,
            t.local_path, t.auth, t.token_used, t.mount_script, t.os,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN jobs j ON t.name = j.target
        WHERE t.name = ?
        GROUP BY t.name
    `, name)

	var target types.Target
	var jobCount int
	err := row.Scan(
		&target.Name, &target.TargetType, &target.AgentHost, &target.S3AccessID, &target.S3Host, &target.S3Bucket, &target.S3Secret,
		&target.S3Region, &target.S3UseSSL, &target.S3UsePathStyle,
		&target.LocalPath, &target.Auth, &target.TokenUsed, &target.MountScript, &target.OperatingSystem,
		&jobCount,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Target{}, ErrTargetNotFound
		}
		return types.Target{}, fmt.Errorf("GetTarget: error fetching target: %w", err)
	}

	target.JobCount = jobCount

	volRows, err := database.readDb.Query(`
		SELECT
			volume_name, meta_type, meta_name, meta_fs,
			meta_total_bytes, meta_used_bytes, meta_free_bytes,
			meta_total, meta_used, meta_free
		FROM volumes
		WHERE target_name = ?
		ORDER BY volume_name
	`, name)
	if err != nil {
		return types.Target{}, fmt.Errorf("GetTarget: error querying volumes: %w", err)
	}
	defer volRows.Close()

	var volumes []types.Volume
	for volRows.Next() {
		var v types.Volume
		err := volRows.Scan(
			&v.VolumeName, &v.MetaType, &v.MetaName, &v.MetaFS,
			&v.MetaTotalBytes, &v.MetaUsedBytes, &v.MetaFreeBytes,
			&v.MetaTotal, &v.MetaUsed, &v.MetaFree,
		)
		if err != nil {
			return types.Target{}, fmt.Errorf("GetTarget: error scanning volume: %w", err)
		}
		v.TargetName = target.Name
		volumes = append(volumes, v)
	}
	if err := volRows.Err(); err != nil {
		return types.Target{}, fmt.Errorf("GetTarget: error iterating volumes: %w", err)
	}
	target.Volumes = volumes

	return target, nil
}

func (database *Database) GetTargetAndVolume(targetName, volumeName string) (types.Target, types.Volume, error) {
	row := database.readDb.QueryRow(`
        SELECT
            t.name, t.target_type, t.agent_host, t.s3_access_id, t.s3_host, t.s3_bucket, t.s3_secret,
						t.s3_region, t.s3_ssl, t.s3_path_style,
            t.local_path, t.auth, t.token_used, t.mount_script, t.os,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN jobs j ON t.name = j.target
        WHERE t.name = ?
        GROUP BY t.name
    `, targetName)

	var tgt types.Target
	var jobCount int
	if err := row.Scan(
		&tgt.Name, &tgt.TargetType, &tgt.AgentHost, &tgt.S3AccessID, &tgt.S3Host, &tgt.S3Bucket, &tgt.S3Secret,
		&tgt.S3Region, &tgt.S3UseSSL, &tgt.S3UsePathStyle,
		&tgt.LocalPath, &tgt.Auth, &tgt.TokenUsed, &tgt.MountScript, &tgt.OperatingSystem,
		&jobCount,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Target{}, types.Volume{}, ErrTargetNotFound
		}
		return types.Target{}, types.Volume{}, fmt.Errorf("GetTargetAndVolume: fetch target: %w", err)
	}

	tgt.JobCount = jobCount

	vrow := database.readDb.QueryRow(`
		SELECT
			volume_name, meta_type, meta_name, meta_fs,
			meta_total_bytes, meta_used_bytes, meta_free_bytes,
			meta_total, meta_used, meta_free
		FROM volumes
		WHERE target_name = ? AND volume_name = ?
	`, targetName, volumeName)

	var vol types.Volume
	vol.TargetName = tgt.Name
	if err := vrow.Scan(
		&vol.VolumeName, &vol.MetaType, &vol.MetaName, &vol.MetaFS,
		&vol.MetaTotalBytes, &vol.MetaUsedBytes, &vol.MetaFreeBytes,
		&vol.MetaTotal, &vol.MetaUsed, &vol.MetaFree,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Target{}, types.Volume{}, fmt.Errorf("Volume not found")
		}
		return types.Target{}, types.Volume{}, fmt.Errorf("GetTargetAndVolume: fetch volume: %w", err)
	}

	return tgt, vol, nil
}

func (database *Database) GetVolume(targetName, volumeName string) (types.Volume, error) {
	row := database.readDb.QueryRow(`
		SELECT
			volume_name, meta_type, meta_name, meta_fs,
			meta_total_bytes, meta_used_bytes, meta_free_bytes,
			meta_total, meta_used, meta_free
		FROM volumes
		WHERE target_name = ? AND volume_name = ?
	`, targetName, volumeName)

	var vol types.Volume
	if err := row.Scan(
		&vol.VolumeName, &vol.MetaType, &vol.MetaName, &vol.MetaFS,
		&vol.MetaTotalBytes, &vol.MetaUsedBytes, &vol.MetaFreeBytes,
		&vol.MetaTotal, &vol.MetaUsed, &vol.MetaFree,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return types.Volume{}, fmt.Errorf("Volume not found")
		}
		return types.Volume{}, fmt.Errorf("GetVolume: fetch volume: %w", err)
	}
	vol.TargetName = targetName

	return vol, nil
}

func (database *Database) GetAuthByHostname(hostname string) (string, error) {
	row := database.readDb.QueryRow(`
        SELECT t.auth
        FROM targets t
        WHERE t.name = ?
          AND t.auth IS NOT NULL
          AND t.auth != ''
        LIMIT 1
    `, hostname)

	var auth string
	if err := row.Scan(&auth); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("GetAuthByHostname: no auth found for %q", hostname)
		}
		return "", fmt.Errorf("GetAuthByHostname: query error: %w", err)
	}

	return auth, nil
}

func (database *Database) GetS3Secret(name string) (string, error) {
	row := database.readDb.QueryRow(`
        SELECT
            t.s3_secret
        FROM targets t
        WHERE t.name = ?
    `, name)

	var encrypted string
	err := row.Scan(&encrypted)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrSecretNotFound
		}
		return "", fmt.Errorf("GetS3Secret: error fetching target: %w", err)
	}

	decrypted, err := simplebox.Decrypt(encrypted)
	if err != nil {
		return "", fmt.Errorf("GetS3Secret: failed to decrypt secret: %w", err)
	}

	return decrypted, nil
}

func (database *Database) GetAllTargets(withVolumes bool) ([]types.Target, error) {
	rows, err := database.readDb.Query(`
        SELECT
            t.name, t.target_type, t.agent_host, t.s3_access_id, t.s3_host, t.s3_bucket, t.s3_secret,
						t.s3_region, t.s3_ssl, t.s3_path_style,
            t.local_path, t.auth, t.token_used, t.mount_script, t.os,
            COUNT(j.id) as job_count
        FROM targets t
        LEFT JOIN jobs j ON t.name = j.target
        GROUP BY t.name
        ORDER BY t.name
    `)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
	}
	defer rows.Close()

	var targets []types.Target
	for rows.Next() {
		var target types.Target
		var jobCount int
		err := rows.Scan(
			&target.Name, &target.TargetType, &target.AgentHost, &target.S3AccessID, &target.S3Host, &target.S3Bucket, &target.S3Secret,
			&target.S3Region, &target.S3UseSSL, &target.S3UsePathStyle,
			&target.LocalPath, &target.Auth, &target.TokenUsed, &target.MountScript, &target.OperatingSystem,
			&jobCount,
		)
		if err != nil {
			syslog.L.Error(fmt.Errorf("GetAllTargets: error scanning target row: %w", err)).Write()
			continue
		}

		target.JobCount = jobCount

		if withVolumes {
			volRows, err := database.readDb.Query(`
			SELECT
				volume_name, meta_type, meta_name, meta_fs,
				meta_total_bytes, meta_used_bytes, meta_free_bytes,
				meta_total, meta_used, meta_free
			FROM volumes
			WHERE target_name = ?
			ORDER BY volume_name
		`, target.Name)
			if err != nil {
				syslog.L.Error(fmt.Errorf("GetAllTargets: error querying volumes for %q: %w", target.Name, err)).Write()
				targets = append(targets, target)
				continue
			}
			var volumes []types.Volume
			for volRows.Next() {
				var v types.Volume
				if err := volRows.Scan(
					&v.VolumeName, &v.MetaType, &v.MetaName, &v.MetaFS,
					&v.MetaTotalBytes, &v.MetaUsedBytes, &v.MetaFreeBytes,
					&v.MetaTotal, &v.MetaUsed, &v.MetaFree,
				); err != nil {
					syslog.L.Error(fmt.Errorf("GetAllTargets: error scanning volume for %q: %w", target.Name, err)).Write()
					continue
				}
				v.TargetName = target.Name
				volumes = append(volumes, v)
			}
			_ = volRows.Close()
			if err := volRows.Err(); err != nil {
				syslog.L.Error(fmt.Errorf("GetAllTargets: error iterating volumes for %q: %w", target.Name, err)).Write()
			}
			target.Volumes = volumes
		}

		targets = append(targets, target)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllTargets: error iterating target rows: %w", err)
	}

	return targets, nil
}

func (database *Database) GetAllVolumes(targetName string) ([]types.Volume, error) {
	volRows, err := database.readDb.Query(`
			SELECT
				volume_name, meta_type, meta_name, meta_fs,
				meta_total_bytes, meta_used_bytes, meta_free_bytes,
				meta_total, meta_used, meta_free
			FROM volumes
			WHERE target_name = ?
			ORDER BY volume_name
		`, targetName)
	if err != nil {
		return nil, fmt.Errorf("GetAllVolumes: error querying volumes: %w", err)
	}
	var volumes []types.Volume
	for volRows.Next() {
		var v types.Volume
		if err := volRows.Scan(
			&v.VolumeName, &v.MetaType, &v.MetaName, &v.MetaFS,
			&v.MetaTotalBytes, &v.MetaUsedBytes, &v.MetaFreeBytes,
			&v.MetaTotal, &v.MetaUsed, &v.MetaFree,
		); err != nil {
			syslog.L.Error(fmt.Errorf("GetAllVolumes: error scanning volume for %q: %w", targetName, err)).Write()
			continue
		}
		v.TargetName = targetName
		volumes = append(volumes, v)
	}
	_ = volRows.Close()
	if err := volRows.Err(); err != nil {
		syslog.L.Error(fmt.Errorf("GetAllVolumes: error iterating volumes for %q: %w", targetName, err)).Write()
	}

	return volumes, nil
}

func (database *Database) UpdateTargetVolumes(tx *sql.Tx, targetName string, volumes []types.Volume) (err error) {
	var commitNeeded bool
	if tx == nil {
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("UpdateTargetVolumes: begin tx: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateTargetVolumes: rollback: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateTargetVolumes: commit: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateTargetVolumes: rollback: %w", rbErr)).Write()
				}
			}
		}()
	}

	if targetName == "" {
		return fmt.Errorf("UpdateTargetVolumes: targetName empty")
	}

	type key struct {
		name string
	}
	newSet := make(map[key]struct{}, len(volumes))

	for _, v := range volumes {
		newSet[key{name: v.VolumeName}] = struct{}{}

		if err := v.Validate(); err != nil {
			continue
		}

		_, err = tx.Exec(`
				INSERT INTO volumes (
					target_name, volume_name, meta_type, meta_name, meta_fs,
					meta_total_bytes, meta_used_bytes, meta_free_bytes, meta_total, meta_used, meta_free
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(target_name, volume_name) DO UPDATE SET
					meta_type=excluded.meta_type,
					meta_name=excluded.meta_name,
					meta_fs=excluded.meta_fs,
					meta_total_bytes=excluded.meta_total_bytes,
					meta_used_bytes=excluded.meta_used_bytes,
					meta_free_bytes=excluded.meta_free_bytes,
					meta_total=excluded.meta_total,
					meta_used=excluded.meta_used,
					meta_free=excluded.meta_free
			`,
			targetName, v.VolumeName, v.MetaType, v.MetaName, v.MetaFS,
			v.MetaTotalBytes, v.MetaUsedBytes, v.MetaFreeBytes, v.MetaTotal, v.MetaUsed, v.MetaFree,
		)
		if err != nil {
			return fmt.Errorf("UpdateTargetVolumes: upsert volume %q: %w", v.VolumeName, err)
		}
	}

	rows, err := tx.Query(`
		SELECT volume_name
		FROM volumes
		WHERE target_name = ?
	`, targetName)
	if err != nil {
		return fmt.Errorf("UpdateTargetVolumes: list existing volumes: %w", err)
	}
	defer rows.Close()

	var toDelete []string
	for rows.Next() {
		var vn string
		if err := rows.Scan(&vn); err != nil {
			return fmt.Errorf("UpdateTargetVolumes: scan volume: %w", err)
		}
		if _, ok := newSet[key{name: vn}]; !ok {
			toDelete = append(toDelete, vn)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("UpdateTargetVolumes: iterate volumes: %w", err)
	}

	for _, vn := range toDelete {
		_, err = tx.Exec(`
			DELETE FROM volumes
			WHERE target_name = ? AND volume_name = ?
		`, targetName, vn)
		if err != nil {
			return fmt.Errorf("UpdateTargetVolumes: delete volume %q: %w", vn, err)
		}
	}

	commitNeeded = true
	return nil
}
