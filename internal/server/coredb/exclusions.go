//go:build linux

package coredb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
	"github.com/pbs-plus/pbs-plus/internal/validate"
	_ "modernc.org/sqlite"
)

func (db *Store) CreateExclusion(tx *Transaction, exclusion Exclusion) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateExclusion: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateExclusion: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateExclusion: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if exclusion.Path == "" {
		return fmt.Errorf("%w: path is empty", ErrValidationFailed)
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !validate.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	err = q.CreateExclusion(db.ctx, corequery.CreateExclusionParams{
		JobID:   exclusion.JobID,
		Path:    exclusion.Path,
		Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
	})
	if err != nil {
		return fmt.Errorf("CreateExclusion: error inserting exclusion: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetAllBackupExclusions(backupID string) ([]Exclusion, error) {
	rows, err := db.readQueries.GetBackupExclusions(db.ctx, backupID)
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
			JobID:   row.JobID,
			Path:    path,
			Comment: row.Comment.String,
		}
		exclusions = append(exclusions, excl)
	}

	return exclusions, nil
}

func (db *Store) GetAllGlobalExclusions() ([]Exclusion, error) {
	rows, err := db.readQueries.ListGlobalExclusions(db.ctx)
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
			JobID:   "",
			Path:    path,
			Comment: row.Comment.String,
		}
		exclusions = append(exclusions, excl)
	}

	return exclusions, nil
}

func (db *Store) GetExclusion(path string) (*Exclusion, error) {
	row, err := db.readQueries.GetExclusion(db.ctx, corequery.GetExclusionParams{
		JobID: "",
		Path:  path,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("GetExclusion: error fetching exclusion for path %s: %w", path, err)
	}

	excl := &Exclusion{
		JobID:   row.JobID,
		Path:    row.Path,
		Comment: row.Comment.String,
	}
	return excl, nil
}

func (db *Store) UpdateExclusion(tx *Transaction, exclusion Exclusion) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateExclusion: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateExclusion: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateExclusion: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if exclusion.Path == "" {
		return fmt.Errorf("%w: path is empty", ErrValidationFailed)
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !validate.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	affected, err := q.UpdateExclusion(db.ctx, corequery.UpdateExclusionParams{
		Comment: sql.NullString{String: exclusion.Comment, Valid: exclusion.Comment != ""},
		JobID:   exclusion.JobID,
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

func (db *Store) DeleteExclusion(tx *Transaction, path string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteExclusion: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteExclusion: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteExclusion: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteExclusion: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	path = strings.ReplaceAll(path, "\\", "/")

	err = q.DeleteExclusion(db.ctx, corequery.DeleteExclusionParams{
		JobID: "",
		Path:  path,
	})
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error deleting exclusion: %w", err)
	}

	commitNeeded = true
	return nil
}
