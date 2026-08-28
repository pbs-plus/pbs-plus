//go:build linux

package coredb

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

func (db *Store) CreateScript(tx *Transaction, script Script) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateScript: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateScript: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateScript: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if script.Path == "" {
		return fmt.Errorf("script path empty")
	}

	err = q.CreateScript(db.ctx, corequery.CreateScriptParams{
		Path:        script.Path,
		Description: toNullString(script.Description),
	})
	if err != nil {
		return fmt.Errorf("CreateScript: error inserting script: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) UpdateScript(tx *Transaction, script Script) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateScript: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateScript: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateScript: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	if script.Path == "" {
		return fmt.Errorf("script path empty")
	}

	err = q.UpdateScript(db.ctx, corequery.UpdateScriptParams{
		Description: toNullString(script.Description),
		Path:        script.Path,
	})
	if err != nil {
		return fmt.Errorf("UpdateScript: error updating script: %w", err)
	}

	commitNeeded = true
	return nil
}

func (db *Store) DeleteScript(tx *Transaction, name string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteScript: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteScript: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteScript: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteScript(db.ctx, name)
	if err != nil {
		return fmt.Errorf("DeleteScript: error deleting script: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetScript(path string) (Script, error) {
	row, err := db.readQueries.GetScript(db.ctx, path)
	if errors.Is(err, sql.ErrNoRows) {
		return Script{}, sql.ErrNoRows
	}
	if err != nil {
		return Script{}, fmt.Errorf("GetScript: error fetching script: %w", err)
	}

	return Script{
		Path:        row.Path,
		Description: fromNullString(row.Description),
		JobCount:    int(row.JobCount),
		TargetCount: int(row.TargetCount),
	}, nil
}

func (db *Store) GetAllScripts() ([]Script, error) {
	rows, err := db.readQueries.ListAllScripts(db.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllScripts: error querying scripts: %w", err)
	}

	scripts := make([]Script, len(rows))
	for i, row := range rows {
		scripts[i] = Script{
			Path:        row.Path,
			Description: fromNullString(row.Description),
			JobCount:    int(row.JobCount),
			TargetCount: int(row.TargetCount),
		}
	}

	return scripts, nil
}

type Script struct {
	Path        string `json:"path"`
	Description string `json:"description"`
	JobCount    int    `json:"job_count"`
	TargetCount int    `json:"target_count"`
	Script      string `json:"script"`
}
