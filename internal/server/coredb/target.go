//go:build linux

package coredb

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func normalizeTarget(target *Target) error {
	if target.Name == "" {
		return errors.New("target name is required")
	}

	switch target.Type {
	case "local":
		target.Type = TargetTypeFilesystem
		target.Access = FilesystemAccessLocal
	case "agent":
		target.Type = TargetTypeFilesystem
		target.Access = FilesystemAccessAgent
	}

	if target.Type == "" {
		if target.AgentHost.Name != "" {
			target.Type = TargetTypeFilesystem
			target.Access = FilesystemAccessAgent
		} else if _, err := ParseS3Url(target.Path); err == nil {
			target.Type = TargetTypeS3
		} else {
			target.Type = TargetTypeFilesystem
			target.Access = FilesystemAccessLocal
		}
	}

	switch target.Type {
	case TargetTypeFilesystem:
		if target.Access == "" {
			if target.AgentHost.Name != "" {
				target.Access = FilesystemAccessAgent
			} else {
				target.Access = FilesystemAccessLocal
			}
		}

		switch target.Access {
		case FilesystemAccessLocal:
			if target.AgentHost.Name != "" {
				return errors.New("local filesystem target cannot have an agent host")
			}
			if target.Path == "" {
				return errors.New("target path empty and no agent host specified")
			}
			if !validate.ValidateTargetPath(target.Path) {
				return fmt.Errorf("invalid target path: %s", target.Path)
			}
		case FilesystemAccessAgent:
			if target.AgentHost.Name == "" {
				return errors.New("agent filesystem target requires an agent host")
			}
		default:
			return fmt.Errorf("unsupported filesystem access %q", target.Access)
		}
	case TargetTypeS3:
		if target.AgentHost.Name != "" {
			return errors.New("S3 target cannot have an agent host")
		}
		if _, err := ParseS3Url(target.Path); err != nil {
			return fmt.Errorf("invalid S3 target URL: %w", err)
		}
		target.Access = ""
	case TargetTypePostgreSQL:
		if err := normalizeDatabaseTarget(target, 5432); err != nil {
			return err
		}
		if target.DatabaseTLSMode == "" {
			target.DatabaseTLSMode = "prefer"
		}
	case TargetTypeMySQL:
		if err := normalizeDatabaseTarget(target, 3306); err != nil {
			return err
		}
		if target.DatabaseVariant == "" {
			target.DatabaseVariant = "mysql"
		}
		if target.DatabaseVariant != "mysql" && target.DatabaseVariant != "mariadb" {
			return fmt.Errorf("unsupported MySQL variant %q", target.DatabaseVariant)
		}
		if target.DatabaseClientFamily == "" {
			target.DatabaseClientFamily = target.DatabaseVariant
		}
		if target.DatabaseClientFamily != "mysql" && target.DatabaseClientFamily != "mariadb" {
			return fmt.Errorf("unsupported MySQL client family %q", target.DatabaseClientFamily)
		}
		if target.DatabaseTLSMode == "" {
			target.DatabaseTLSMode = "preferred"
		}
	default:
		return fmt.Errorf("unsupported target type %q", target.Type)
	}

	return nil
}

func normalizeDatabaseTarget(target *Target, defaultPort int) error {
	if target.AgentHost.Name != "" {
		return errors.New("database target cannot have an agent host")
	}
	if target.DatabaseHost == "" {
		return errors.New("database target host is required")
	}
	if target.DatabaseUsername == "" {
		return errors.New("database target username is required")
	}
	if target.DatabasePort == 0 {
		target.DatabasePort = defaultPort
	}
	if target.DatabasePort < 1 || target.DatabasePort > 65535 {
		return fmt.Errorf("invalid database target port %d", target.DatabasePort)
	}
	if target.DatabaseDefaultClientDir != "" && !filepath.IsAbs(target.DatabaseDefaultClientDir) {
		return errors.New("database target client directory must be absolute")
	}
	if target.DatabaseCACertificate != "" && !filepath.IsAbs(target.DatabaseCACertificate) {
		return errors.New("database target CA certificate path must be absolute")
	}

	target.Access = ""
	target.Path = ""
	return nil
}

func (db *Store) storeTargetDetails(q *corequery.Queries, target Target) error {
	switch target.Type {
	case TargetTypeFilesystem:
		if err := q.DeleteTargetS3(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetPostgreSQL(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetMySQL(db.ctx, target.Name); err != nil {
			return err
		}
		return q.UpsertTargetFilesystem(db.ctx, corequery.UpsertTargetFilesystemParams{
			TargetName:       target.Name,
			Access:           string(target.Access),
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
		})
	case TargetTypeS3:
		if err := q.DeleteTargetFilesystem(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetPostgreSQL(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetMySQL(db.ctx, target.Name); err != nil {
			return err
		}
		return q.UpsertTargetS3(db.ctx, corequery.UpsertTargetS3Params{
			TargetName: target.Name,
			Url:        target.Path,
		})
	case TargetTypePostgreSQL:
		if err := q.DeleteTargetFilesystem(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetS3(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetMySQL(db.ctx, target.Name); err != nil {
			return err
		}
		return q.UpsertTargetPostgreSQL(db.ctx, corequery.UpsertTargetPostgreSQLParams{
			TargetName:       target.Name,
			Host:             target.DatabaseHost,
			Port:             int64(target.DatabasePort),
			Username:         target.DatabaseUsername,
			SslMode:          target.DatabaseTLSMode,
			CaCertificate:    target.DatabaseCACertificate,
			DefaultClientDir: target.DatabaseDefaultClientDir,
		})
	case TargetTypeMySQL:
		if err := q.DeleteTargetFilesystem(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetS3(db.ctx, target.Name); err != nil {
			return err
		}
		if err := q.DeleteTargetPostgreSQL(db.ctx, target.Name); err != nil {
			return err
		}
		return q.UpsertTargetMySQL(db.ctx, corequery.UpsertTargetMySQLParams{
			TargetName:          target.Name,
			Variant:             target.DatabaseVariant,
			Host:                target.DatabaseHost,
			Port:                int64(target.DatabasePort),
			Username:            target.DatabaseUsername,
			TlsMode:             target.DatabaseTLSMode,
			CaCertificate:       target.DatabaseCACertificate,
			DefaultClientFamily: target.DatabaseClientFamily,
			DefaultClientDir:    target.DatabaseDefaultClientDir,
		})
	default:
		return fmt.Errorf("unsupported target type %q", target.Type)
	}
}

func (db *Store) CreateTarget(tx *Transaction, target Target) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateTarget: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateTarget: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateTarget: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if err := normalizeTarget(&target); err != nil {
		return fmt.Errorf("CreateTarget: %w", err)
	}

	err = q.CreateTarget(db.ctx, corequery.CreateTargetParams{
		Name:        target.Name,
		TargetType:  string(target.Type),
		MountScript: target.MountScript,
	})
	if err != nil {
		return fmt.Errorf("CreateTarget: error inserting target: %w", err)
	}
	if err = db.storeTargetDetails(q, target); err != nil {
		return fmt.Errorf("CreateTarget: error storing target details: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) UpdateTarget(tx *Transaction, target Target) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateTarget: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateTarget: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateTarget: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if err := normalizeTarget(&target); err != nil {
		return fmt.Errorf("UpdateTarget: %w", err)
	}

	err = q.UpdateTarget(db.ctx, corequery.UpdateTargetParams{
		TargetType:  string(target.Type),
		MountScript: target.MountScript,
		Name:        target.Name,
	})
	if err != nil {
		return fmt.Errorf("UpdateTarget: error updating target: %w", err)
	}
	if err = db.storeTargetDetails(q, target); err != nil {
		return fmt.Errorf("UpdateTarget: error storing target details: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) UpsertTarget(tx *Transaction, target Target) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpsertTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpsertTarget: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpsertTarget: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpsertTarget: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if err := normalizeTarget(&target); err != nil {
		return fmt.Errorf("UpsertTarget: %w", err)
	}

	err = q.UpsertTarget(db.ctx, corequery.UpsertTargetParams{
		Name:        target.Name,
		TargetType:  string(target.Type),
		MountScript: target.MountScript,
	})
	if err != nil {
		return fmt.Errorf("UpsertTarget: error upserting target: %w", err)
	}
	if err = db.storeTargetDetails(q, target); err != nil {
		return fmt.Errorf("UpsertTarget: error storing target details: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) AddS3Secret(tx *Transaction, targetName string, secret string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("AddS3Secret: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("AddS3Secret: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("AddS3Secret: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("AddS3Secret: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	encrypted, err := Encrypt(secret)
	if err != nil {
		return fmt.Errorf("AddS3Secret: error encrypting secret: %w", err)
	}

	rows, err := q.UpdateTargetS3Secret(db.ctx, corequery.UpdateTargetS3SecretParams{
		Secret:     encrypted,
		TargetName: targetName,
	})
	if err != nil {
		return fmt.Errorf("AddS3Secret: error adding secret to target: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("AddS3Secret: target %q is not an S3 target", targetName)
	}

	commitNeeded = true
	return nil
}

func (db *Store) DeleteTarget(tx *Transaction, name string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteTarget: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteTarget: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteTarget: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteTarget: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteTarget(db.ctx, name)
	if err != nil {
		return fmt.Errorf("DeleteTarget: error deleting target: %w", err)
	}

	if rowsAffected == 0 {
		return ErrTargetNotFound
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetTarget(name string) (Target, error) {
	row, err := db.readQueries.GetTarget(db.ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return Target{}, ErrTargetNotFound
	}
	if err != nil {
		return Target{}, fmt.Errorf("GetTarget: error fetching target: %w", err)
	}

	target := Target{
		Name:   row.Name,
		Type:   TargetType(row.TargetType),
		Access: FilesystemAccess(row.FilesystemAccess),
		Path:   row.Path,
		AgentHost: AgentHost{
			Name:            row.AgentName.String,
			IP:              row.AgentIp.String,
			Auth:            row.AgentAuth.String,
			TokenUsed:       row.AgentTokenUsed.String,
			OperatingSystem: row.AgentOs.String,
		},
		VolumeID:                 fromNullString(row.VolumeID),
		VolumeType:               fromNullString(row.VolumeType),
		VolumeName:               fromNullString(row.VolumeName),
		VolumeFS:                 fromNullString(row.VolumeFs),
		VolumeTotalBytes:         fromNullInt64(row.VolumeTotalBytes),
		VolumeUsedBytes:          fromNullInt64(row.VolumeUsedBytes),
		VolumeFreeBytes:          fromNullInt64(row.VolumeFreeBytes),
		VolumeTotal:              fromNullString(row.VolumeTotal),
		VolumeUsed:               fromNullString(row.VolumeUsed),
		VolumeFree:               fromNullString(row.VolumeFree),
		MountScript:              row.MountScript,
		JobCount:                 int(row.JobCount),
		DatabaseHost:             row.DatabaseHost,
		DatabasePort:             int(row.DatabasePort),
		DatabaseUsername:         row.DatabaseUsername,
		DatabaseTLSMode:          row.DatabaseTlsMode,
		DatabaseCACertificate:    row.DatabaseCaCertificate,
		DatabaseDefaultClientDir: row.DatabaseDefaultClientDir,
		DatabaseVariant:          row.DatabaseVariant,
		DatabaseClientFamily:     row.DatabaseDefaultClientFamily,
	}

	target.populateInfo()

	return target, nil
}

func (db *Store) GetS3Secret(name string) (string, error) {
	encrypted, err := db.readQueries.GetTargetS3Secret(db.ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrSecretNotFound
	}
	if err != nil {
		return "", fmt.Errorf("GetS3Secret: error fetching target: %w", err)
	}

	if encrypted == "" {
		return "", ErrSecretNotFound
	}

	decrypted, err := Decrypt(encrypted)
	if err != nil {
		return "", fmt.Errorf("GetS3Secret: failed to decrypt secret: %w", err)
	}

	return decrypted, nil
}

func (db *Store) AddDatabasePassword(tx *Transaction, targetName string, password string) (err error) {
	var commitNeeded bool
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("AddDatabasePassword: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("AddDatabasePassword: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("AddDatabasePassword: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				log.Error(fmt.Errorf("AddDatabasePassword: failed to rollback transaction: %w", rbErr), "")
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	encrypted, err := Encrypt(password)
	if err != nil {
		return fmt.Errorf("AddDatabasePassword: error encrypting password: %w", err)
	}

	rows, err := q.UpdateTargetPostgreSQLPassword(db.ctx, corequery.UpdateTargetPostgreSQLPasswordParams{
		Password:   encrypted,
		TargetName: targetName,
	})
	if err != nil {
		return fmt.Errorf("AddDatabasePassword: error updating PostgreSQL target: %w", err)
	}
	if rows == 0 {
		rows, err = q.UpdateTargetMySQLPassword(db.ctx, corequery.UpdateTargetMySQLPasswordParams{
			Password:   encrypted,
			TargetName: targetName,
		})
		if err != nil {
			return fmt.Errorf("AddDatabasePassword: error updating MySQL target: %w", err)
		}
	}
	if rows == 0 {
		return fmt.Errorf("AddDatabasePassword: target %q is not a database target", targetName)
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetDatabasePassword(name string) (string, error) {
	target, err := db.GetTarget(name)
	if err != nil {
		return "", err
	}

	var encrypted string
	switch target.Type {
	case TargetTypePostgreSQL:
		encrypted, err = db.readQueries.GetTargetPostgreSQLPassword(db.ctx, name)
	case TargetTypeMySQL:
		encrypted, err = db.readQueries.GetTargetMySQLPassword(db.ctx, name)
	default:
		return "", fmt.Errorf("GetDatabasePassword: target %q is not a database target", name)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrSecretNotFound
	}
	if err != nil {
		return "", fmt.Errorf("GetDatabasePassword: error fetching target: %w", err)
	}
	if encrypted == "" {
		return "", ErrSecretNotFound
	}

	decrypted, err := Decrypt(encrypted)
	if err != nil {
		return "", fmt.Errorf("GetDatabasePassword: failed to decrypt password: %w", err)
	}
	return decrypted, nil
}

func (db *Store) GetAllTargets() ([]Target, error) {
	rows, err := db.readQueries.ListAllTargets(db.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error querying targets: %w", err)
	}

	targets := make([]Target, 0, len(rows))
	for _, row := range rows {
		target := Target{
			Name:   row.Name,
			Type:   TargetType(row.TargetType),
			Access: FilesystemAccess(row.FilesystemAccess),
			Path:   row.Path,
			AgentHost: AgentHost{
				Name:            row.AgentName.String,
				IP:              row.AgentIp.String,
				Auth:            row.AgentAuth.String,
				TokenUsed:       row.AgentTokenUsed.String,
				OperatingSystem: row.AgentOs.String,
			},
			VolumeID:                 fromNullString(row.VolumeID),
			VolumeType:               fromNullString(row.VolumeType),
			VolumeName:               fromNullString(row.VolumeName),
			VolumeFS:                 fromNullString(row.VolumeFs),
			VolumeTotalBytes:         fromNullInt64(row.VolumeTotalBytes),
			VolumeUsedBytes:          fromNullInt64(row.VolumeUsedBytes),
			VolumeFreeBytes:          fromNullInt64(row.VolumeFreeBytes),
			VolumeTotal:              fromNullString(row.VolumeTotal),
			VolumeUsed:               fromNullString(row.VolumeUsed),
			VolumeFree:               fromNullString(row.VolumeFree),
			MountScript:              row.MountScript,
			JobCount:                 int(row.JobCount),
			DatabaseHost:             row.DatabaseHost,
			DatabasePort:             int(row.DatabasePort),
			DatabaseUsername:         row.DatabaseUsername,
			DatabaseTLSMode:          row.DatabaseTlsMode,
			DatabaseCACertificate:    row.DatabaseCaCertificate,
			DatabaseDefaultClientDir: row.DatabaseDefaultClientDir,
			DatabaseVariant:          row.DatabaseVariant,
			DatabaseClientFamily:     row.DatabaseDefaultClientFamily,
		}

		target.populateInfo()
		targets = append(targets, target)
	}

	return targets, nil
}

func (db *Store) GetAllTargetsByAgentHost(hostname string) ([]Target, error) {
	rows, err := db.readQueries.ListTargetsByAgentHost(db.ctx, toNullString(hostname))
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByAgentHost: error querying targets: %w", err)
	}

	targets := make([]Target, 0, len(rows))
	for _, row := range rows {
		target := Target{
			Name:   row.Name,
			Type:   TargetType(row.TargetType),
			Access: FilesystemAccess(row.FilesystemAccess),
			Path:   row.Path,
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
	if !t.IsS3() {
		t.S3Info = nil
		return
	}

	if s3Info, err := ParseS3Url(t.Path); err == nil {
		t.S3Info = s3Info
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

func (t *Target) LegacyType() string {
	if t.IsAgent() {
		return "agent"
	}
	if t.IsLocal() {
		return "local"
	}
	return string(t.Type)
}

func (t *Target) IsFilesystem() bool {
	return t.Type == TargetTypeFilesystem
}

func (t *Target) IsAgent() bool {
	return t.IsFilesystem() && t.Access == FilesystemAccessAgent
}

func (t *Target) IsS3() bool {
	return t.Type == TargetTypeS3
}

func (t *Target) IsLocal() bool {
	return t.IsFilesystem() && t.Access == FilesystemAccessLocal
}

func (t *Target) IsDatabase() bool {
	return t.Type == TargetTypePostgreSQL || t.Type == TargetTypeMySQL
}

type Target struct {
	Name                     string           `json:"name"`
	Type                     TargetType       `json:"target_type"`
	Access                   FilesystemAccess `json:"access,omitempty"`
	Path                     string           `json:"path"`
	AgentHost                AgentHost        `json:"agent_host"`
	VolumeID                 string           `json:"volume_id,omitempty"`
	MountScript              string           `json:"mount_script"`
	AgentVersion             string           `json:"agent_version"`
	ConnectionStatus         bool             `json:"connection_status"`
	JobCount                 int              `json:"job_count"`
	VolumeType               string           `json:"volume_type"`
	VolumeName               string           `json:"volume_name"`
	VolumeFS                 string           `json:"volume_fs"`
	VolumeTotalBytes         int              `json:"volume_total_bytes,omitempty"`
	VolumeUsedBytes          int              `json:"volume_used_bytes,omitempty"`
	VolumeFreeBytes          int              `json:"volume_free_bytes,omitempty"`
	VolumeTotal              string           `json:"volume_total"`
	VolumeUsed               string           `json:"volume_used"`
	VolumeFree               string           `json:"volume_free"`
	S3Info                   *S3Url           `json:"s3_info"`
	DatabaseHost             string           `json:"database_host,omitempty"`
	DatabasePort             int              `json:"database_port,omitempty"`
	DatabaseUsername         string           `json:"database_username,omitempty"`
	DatabaseTLSMode          string           `json:"database_tls_mode,omitempty"`
	DatabaseCACertificate    string           `json:"database_ca_certificate,omitempty"`
	DatabaseDefaultClientDir string           `json:"database_default_client_dir,omitempty"`
	DatabaseVariant          string           `json:"database_variant,omitempty"`
	DatabaseClientFamily     string           `json:"database_default_client_family,omitempty"`
}

type AgentHost struct {
	Name            string `json:"name"`
	IP              string `json:"ip"`
	Auth            string `json:"-"`
	TokenUsed       string `json:"-"`
	OperatingSystem string `json:"os"`
}

type TargetType string
