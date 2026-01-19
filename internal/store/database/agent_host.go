//go:build linux

package database

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (database *Database) CreateAgentHost(tx *Transaction, host AgentHost) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateAgentHost: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateAgentHost: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateAgentHost: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("CreateAgentHost: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	err = q.CreateAgentHost(database.ctx, sqlc.CreateAgentHostParams{
		Name:      host.Name,
		Ip:        host.IP,
		Auth:      toNullString(host.Auth),
		TokenUsed: toNullString(host.TokenUsed),
		Os:        host.OperatingSystem,
	})

	if err != nil {
		return fmt.Errorf("CreateAgentHost: error inserting agent host: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) UpdateAgentHost(tx *Transaction, host AgentHost) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateAgentHost: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateAgentHost: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateAgentHost: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("UpdateAgentHost: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	err = q.UpdateAgentHost(database.ctx, sqlc.UpdateAgentHostParams{
		Ip:        host.IP,
		Auth:      toNullString(host.Auth),
		TokenUsed: toNullString(host.TokenUsed),
		Os:        host.OperatingSystem,
		Name:      host.Name,
	})

	if err != nil {
		return fmt.Errorf("UpdateAgentHost: error updating agent host: %w", err)
	}

	commitNeeded = true
	return nil
}

func (database *Database) DeleteAgentHost(tx *Transaction, name string) (err error) {
	var commitNeeded bool = false
	q := database.queries

	if tx == nil {
		tx, err = database.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteAgentHost: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteAgentHost: failed to rollback transaction: %w", rbErr)).Write()
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteAgentHost: failed to commit transaction: %w", cErr)
					syslog.L.Error(err).Write()
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					syslog.L.Error(fmt.Errorf("DeleteAgentHost: failed to rollback transaction: %w", rbErr)).Write()
				}
			}
		}()
	}
	q = database.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteAgentHost(database.ctx, name)
	if err != nil {
		return fmt.Errorf("DeleteAgentHost: error deleting target: %w", err)
	}

	if rowsAffected == 0 {
		return ErrTargetNotFound
	}

	commitNeeded = true
	return nil
}

func (database *Database) GetAgentHost(name string) (AgentHost, error) {
	row, err := database.readQueries.GetAgentHost(database.ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentHost{}, ErrAgentHostNotFound
	}
	if err != nil {
		return AgentHost{}, fmt.Errorf("GetAgentHost: error fetching agent host: %w", err)
	}

	return AgentHost{
		Name:            row.Name,
		IP:              row.Ip,
		Auth:            fromNullString(row.Auth),
		TokenUsed:       fromNullString(row.TokenUsed),
		OperatingSystem: row.Os,
	}, nil
}

func (database *Database) GetAllAgentHosts() ([]AgentHost, error) {
	rows, err := database.readQueries.ListAllAgentHosts(database.ctx)
	if err != nil {
		return nil, fmt.Errorf("GetAllAgentHosts: error querying agent hosts: %w", err)
	}

	hosts := make([]AgentHost, 0, len(rows))
	for _, row := range rows {
		hosts = append(hosts, AgentHost{
			Name:            row.Name,
			IP:              row.Ip,
			Auth:            fromNullString(row.Auth),
			TokenUsed:       fromNullString(row.TokenUsed),
			OperatingSystem: row.Os,
		})
	}

	return hosts, nil
}

func (database *Database) GetAgentHostAuth(hostname string) (string, error) {
	auth, err := database.readQueries.GetAgentHostAuth(database.ctx, hostname)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrAgentHostNotFound
	}
	if err != nil {
		return "", fmt.Errorf("GetAgentHostAuth: error fetching auth: %w", err)
	}

	return fromNullString(auth), nil
}
