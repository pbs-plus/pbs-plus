//go:build linux

package database

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (database *Database) CreateScript(tx *Transaction, script Script) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateScript: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	if script.Path == "" {
		return fmt.Errorf("script path empty")
	}

	err = q.CreateScript(database.ctx, sqlc.CreateScriptParams{
		Path:        script.Path,
		Description: toNullString(script.Description),
	})
	if err != nil {
		return fmt.Errorf("CreateScript: error inserting script: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) UpdateScript(tx *Transaction, script Script) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateScript: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	if script.Path == "" {
		return fmt.Errorf("script path empty")
	}

	err = q.UpdateScript(database.ctx, sqlc.UpdateScriptParams{
		Description: toNullString(script.Description),
		Path:        script.Path,
	})
	if err != nil {
		return fmt.Errorf("UpdateScript: error updating script: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) DeleteScript(tx *Transaction, name string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteScript: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteScript: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteScript: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteScript(database.ctx, name)
	if err != nil {
		return fmt.Errorf("DeleteScript: error deleting script: %w", err)
	}

	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetScript(path string) (Script, error) {
	row, err := database.readQueries.GetScript(database.ctx, path)
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

func (database *Database) GetAllScripts() ([]Script, error) {
	rows, err := database.readQueries.ListAllScripts(database.ctx)
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
