//go:build linux

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/pattern"
	_ "modernc.org/sqlite"
)

func (database *Database) CreateExclusion(tx *sql.Tx, exclusion Exclusion) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateExclusion: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
		q = database.queries.WithTx(tx)
	}

	if exclusion.Path == "" {
		return errors.New("path is empty")
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	err = q.CreateExclusion(database.ctx, sqlc.CreateExclusionParams{
		JobID:   sql.NullString{String: exclusion.JobId, Valid: exclusion.JobId != ""},
		Path:    exclusion.Path,
		Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
	})
	if err != nil {
		return fmt.Errorf("CreateExclusion: error inserting exclusion: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetAllBackupExclusions(backupId string) ([]Exclusion, error) {
	rows, err := database.readQueries.GetBackupExclusions(database.ctx, sql.NullString{
		String: backupId,
		Valid:  backupId != "",
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("GetAllBackupExclusions: error querying exclusions: %w", err)
	}

	exclusions := make([]Exclusion, 0, len(rows))
	seenPaths := make(map[string]bool)

	for _, row := range rows {
		path := row.Path
		if seenPaths[path] {
			continue
		}
		seenPaths[path] = true

		excl := Exclusion{
			JobId:   row.JobID.String,
			Path:    path,
			Comment: row.Comment.String,
		}
		exclusions = append(exclusions, excl)
	}

	return exclusions, nil
}

func (database *Database) GetAllGlobalExclusions() ([]Exclusion, error) {
	rows, err := database.readQueries.ListGlobalExclusions(database.ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("GetAllGlobalExclusions: error querying exclusions: %w", err)
	}

	exclusions := make([]Exclusion, 0, len(rows))
	seenPaths := make(map[string]bool)

	for _, row := range rows {
		path := row.Path
		if seenPaths[path] {
			continue
		}
		seenPaths[path] = true

		excl := Exclusion{
			JobId:   "",
			Path:    path,
			Comment: row.Comment.String,
		}
		exclusions = append(exclusions, excl)
	}

	return exclusions, nil
}

func (database *Database) GetExclusion(path string) (*Exclusion, error) {
	row, err := database.readQueries.GetExclusion(database.ctx, sqlc.GetExclusionParams{
		Path: path,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("GetExclusion: error fetching exclusion for path %s: %w", path, err)
	}

	excl := &Exclusion{
		JobId:   row.JobID.String,
		Path:    row.Path,
		Comment: row.Comment.String,
	}
	return excl, nil
}

func (database *Database) UpdateExclusion(tx *sql.Tx, exclusion Exclusion) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateExclusion: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
		q = database.queries.WithTx(tx)
	}

	if exclusion.Path == "" {
		return errors.New("path is empty")
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	affected, err := q.UpdateExclusion(database.ctx, sqlc.UpdateExclusionParams{
		JobID:   sql.NullString{String: exclusion.JobId, Valid: exclusion.JobId != ""},
		Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
		Path:    exclusion.Path,
	})
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error updating exclusion: %w", err)
	}

	if affected == 0 {
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}

func (database *Database) DeleteExclusion(tx *sql.Tx, path string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteExclusion: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteExclusion: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
		q = database.queries.WithTx(tx)
	}

	path = strings.ReplaceAll(path, "\\", "/")

	err = q.DeleteExclusion(database.ctx, sqlc.DeleteExclusionParams{
		Path: path,
	})
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error deleting exclusion: %w", err)
	}

	commitNeeded = true
	return nil
}
