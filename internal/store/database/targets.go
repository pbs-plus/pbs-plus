//go:build linux

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	s3url "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/url"
	secrets "github.com/pbs-plus/pbs-plus/internal/store/database/secrets"
	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func (database *Database) CreateTarget(tx *Transaction, target Target) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
	q = database.queries.WithTx(tx.Tx)

	// Validation
	if target.Path == "" && target.AgentHost.Name == "" {
		return fmt.Errorf("target path empty and no agent host specified")
	}

	if target.AgentHost.Name != "" {
		_, err := database.GetAgentHost(target.AgentHost.Name)
		if err != nil {
			return fmt.Errorf("agent host not found: %w", err)
		}
	}

	_, s3Err := s3url.Parse(target.Path)
	if target.Path != "" && !utils.ValidateTargetPath(target.Path) && s3Err != nil {
		return fmt.Errorf("invalid target path: %s", target.Path)
	}

	err = q.CreateTarget(database.ctx, sqlc.CreateTargetParams{
		Name:             target.Name,
		Path:             target.Path,
		AgentHost:        toNullString(target.AgentHost.Name),
		VolumeID:         toNullString(target.VolumeID),
		VolumeType:       toNullString(target.VolumeType),
		VolumeName:       toNullString(target.VolumeName),
		VolumeFs:         toNullString(target.VolumeFS),
		VolumeTotalBytes: toNullInt64(target.VolumeTotalBytes),
		VolumeUsedBytes:  toNullInt64(target.VolumeUsedBytes),
		VolumeFreeBytes:  toNullInt64(target.VolumeFreeBytes),
		VolumeTotal:      toNullString(target.VolumeTotal),
		VolumeUsed:       toNullString(target.VolumeUsed),
		VolumeFree:       toNullString(target.VolumeFree),
		MountScript:      target.MountScript,
	})

	if err != nil {
		return fmt.Errorf("CreateTarget: error inserting target: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) UpdateTarget(tx *Transaction, target Target) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
	q = database.queries.WithTx(tx.Tx)

	// Validation
	if target.Path == "" && target.AgentHost.Name == "" {
		return fmt.Errorf("target path empty and no agent host specified")
	}

	if target.AgentHost.Name != "" {
		_, err := database.GetAgentHost(target.AgentHost.Name)
		if err != nil {
			return fmt.Errorf("agent host not found: %w", err)
		}
	}

	_, s3Err := s3url.Parse(target.Path)
	if target.Path != "" && !utils.ValidateTargetPath(target.Path) && s3Err != nil {
		return fmt.Errorf("invalid target path: %s", target.Path)
	}

	err = q.UpdateTarget(database.ctx, sqlc.UpdateTargetParams{
		Path:             target.Path,
		AgentHost:        toNullString(target.AgentHost.Name),
		VolumeID:         toNullString(target.VolumeID),
		VolumeType:       toNullString(target.VolumeType),
		VolumeName:       toNullString(target.VolumeName),
		VolumeFs:         toNullString(target.VolumeFS),
		VolumeTotalBytes: toNullInt64(target.VolumeTotalBytes),
		VolumeUsedBytes:  toNullInt64(target.VolumeUsedBytes),
		VolumeFreeBytes:  toNullInt64(target.VolumeFreeBytes),
		VolumeTotal:      toNullString(target.VolumeTotal),
		VolumeUsed:       toNullString(target.VolumeUsed),
		VolumeFree:       toNullString(target.VolumeFree),
		MountScript:      target.MountScript,
		Name:             target.Name,
	})

	if err != nil {
		return fmt.Errorf("UpdateTarget: error updating target: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) AddS3Secret(tx *Transaction, targetName string, secret string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("AddS3Secret: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("AddS3Secret: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("AddS3Secret: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("AddS3Secret: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	encrypted, err := secrets.Encrypt(secret)
	if err != nil {
		return fmt.Errorf("AddS3Secret: error encrypting secret: %w", err)
	}

	err = q.UpdateTargetS3Secret(database.ctx, sqlc.UpdateTargetS3SecretParams{
		SecretS3: encrypted,
		Name:     targetName,
	})
	if err != nil {
		return fmt.Errorf("AddS3Secret: error adding secret to target: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) DeleteTarget(tx *Transaction, name string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
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
	q = database.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteTarget(database.ctx, name)
	if err != nil {
		return fmt.Errorf("DeleteTarget: error deleting target: %w", err)
	}

	if rowsAffected == 0 {
		return ErrTargetNotFound
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetTarget(name string) (Target, error) {
	row, err := database.readQueries.GetTarget(database.ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return Target{}, ErrTargetNotFound
	}
	if err != nil {
		return Target{}, fmt.Errorf("GetTarget: error fetching target: %w", err)
	}

	target := Target{
		Name: row.Name,
		Path: row.Path,
		AgentHost: AgentHost{
			Name:            row.AgentName.String,
			IP:              row.AgentIp.String,
			Auth:            row.AgentAuth.String,
			TokenUsed:       row.AgentTokenUsed.String,
			OperatingSystem: row.AgentOs.String,
		},
		VolumeID:         fromNullString(row.VolumeID),
		VolumeType:       fromNullString(row.VolumeType),
		VolumeName:       fromNullString(row.VolumeName),
		VolumeFS:         fromNullString(row.VolumeFs),
		VolumeTotalBytes: fromNullInt64(row.VolumeTotalBytes),
		VolumeUsedBytes:  fromNullInt64(row.VolumeUsedBytes),
		VolumeFreeBytes:  fromNullInt64(row.VolumeFreeBytes),
		VolumeTotal:      fromNullString(row.VolumeTotal),
		VolumeUsed:       fromNullString(row.VolumeUsed),
		VolumeFree:       fromNullString(row.VolumeFree),
		MountScript:      row.MountScript,
		JobCount:         int(row.JobCount),
	}

	target.populateInfo()

	return target, nil
}

func (database *Database) GetS3Secret(name string) (string, error) {
	encrypted, err := database.readQueries.GetTargetS3Secret(database.ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrSecretNotFound
	}
	if err != nil {
		return "", fmt.Errorf("GetS3Secret: error fetching target: %w", err)
	}

	if encrypted == "" {
		return "", ErrSecretNotFound
	}

	decrypted, err := secrets.Decrypt(encrypted)
	if err != nil {
		return "", fmt.Errorf("GetS3Secret: failed to decrypt secret: %w", err)
	}

	return decrypted, nil
}

func (database *Database) GetAllTargets() ([]Target, error) {
	rows, err := database.readQueries.ListAllTargets(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
	}

	targets := make([]Target, 0, len(rows))
	for _, row := range rows {
		target := Target{
			Name: row.Name,
			Path: row.Path,
			AgentHost: AgentHost{
				Name:            row.AgentName.String,
				IP:              row.AgentIp.String,
				Auth:            row.AgentAuth.String,
				TokenUsed:       row.AgentTokenUsed.String,
				OperatingSystem: row.AgentOs.String,
			},
			VolumeID:         fromNullString(row.VolumeID),
			VolumeType:       fromNullString(row.VolumeType),
			VolumeName:       fromNullString(row.VolumeName),
			VolumeFS:         fromNullString(row.VolumeFs),
			VolumeTotalBytes: fromNullInt64(row.VolumeTotalBytes),
			VolumeUsedBytes:  fromNullInt64(row.VolumeUsedBytes),
			VolumeFreeBytes:  fromNullInt64(row.VolumeFreeBytes),
			VolumeTotal:      fromNullString(row.VolumeTotal),
			VolumeUsed:       fromNullString(row.VolumeUsed),
			VolumeFree:       fromNullString(row.VolumeFree),
			MountScript:      row.MountScript,
			JobCount:         int(row.JobCount),
		}

		target.populateInfo()
		targets = append(targets, target)
	}

	return targets, nil
}

func (database *Database) GetAllTargetsByAgentHost(hostname string) ([]Target, error) {
	rows, err := database.readQueries.ListTargetsByAgentHost(database.ctx, toNullString(hostname))
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByAgentHost: error querying targets: %w", err)
	}

	targets := make([]Target, 0, len(rows))
	for _, row := range rows {
		target := Target{
			Name: row.Name,
			Path: row.Path,
			AgentHost: AgentHost{
				Name:            row.AgentName.String,
				IP:              row.AgentIp.String,
				Auth:            row.AgentAuth.String,
				TokenUsed:       row.AgentTokenUsed.String,
				OperatingSystem: row.AgentOs.String,
			},
			VolumeID:         fromNullString(row.VolumeID),
			VolumeType:       fromNullString(row.VolumeType),
			VolumeName:       fromNullString(row.VolumeName),
			VolumeFS:         fromNullString(row.VolumeFs),
			VolumeTotalBytes: fromNullInt64(row.VolumeTotalBytes),
			VolumeUsedBytes:  fromNullInt64(row.VolumeUsedBytes),
			VolumeFreeBytes:  fromNullInt64(row.VolumeFreeBytes),
			VolumeTotal:      fromNullString(row.VolumeTotal),
			VolumeUsed:       fromNullString(row.VolumeUsed),
			VolumeFree:       fromNullString(row.VolumeFree),
			MountScript:      row.MountScript,
		}

		target.populateInfo()
		targets = append(targets, target)
	}

	return targets, nil
}

func (t *Target) populateInfo() {
	if t.Path != "" {
		if strings.Contains(t.Path, "://") {
			if s3, err := s3url.Parse(t.Path); err == nil {
				t.S3Info = s3
				t.Type = TargetTypeS3
			} else if utils.IsValid(t.Path) {
				t.Type = TargetTypeLocal
			}
		}
	} else if t.AgentHost.Name != "" {
		t.Type = TargetTypeAgent
	}
}

func (t *Target) GetAgentHostPath() string {
	hostPath := ""
	if t.AgentHost.Name != "" {
		res := strings.ToLower(t.VolumeID)
		switch {
		case res == "root":
			hostPath = "/"
		case t.AgentHost.OperatingSystem == "windows":
			hostPath = res + ":\\"
		default:
			hostPath = res
		}
	}

	return hostPath
}

func (t *Target) GetHostname() string {
	if t.AgentHost.Name != "" {
		return t.AgentHost.Name
	}

	return t.Name
}

func (t *Target) IsAgent() bool {
	return t.Type == TargetTypeAgent
}

func (t *Target) IsS3() bool {
	return t.Type == TargetTypeS3
}

func (t *Target) IsLocal() bool {
	return t.Type == TargetTypeLocal
}
