//go:build linux

package coredb

import (
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

func (db *Store) CreateAgentHost(tx *Transaction, host AgentHost) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("CreateAgentHost: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateAgentHost: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("CreateAgentHost: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("CreateAgentHost: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	err = q.CreateAgentHost(db.ctx, corequery.CreateAgentHostParams{
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

func (db *Store) UpdateAgentHost(tx *Transaction, host AgentHost) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("UpdateAgentHost: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateAgentHost: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("UpdateAgentHost: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("UpdateAgentHost: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	err = q.UpdateAgentHost(db.ctx, corequery.UpdateAgentHostParams{
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

func (db *Store) DeleteAgentHost(tx *Transaction, name string) (err error) {
	var commitNeeded bool = false
	q := db.queries

	if tx == nil {
		tx, err = db.NewTransaction()
		if err != nil {
			return fmt.Errorf("DeleteAgentHost: failed to begin transaction: %w", err)
		}
		defer func() {
			if p := recover(); p != nil {
				if err := tx.Rollback(); err != nil {
					log.Error(err, "")
				}
				panic(p)
			} else if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteAgentHost: failed to rollback transaction: %w", rbErr), "")
				}
			} else if commitNeeded {
				if cErr := tx.Commit(); cErr != nil {
					err = fmt.Errorf("DeleteAgentHost: failed to commit transaction: %w", cErr)
					log.Error(err, "")
				}
			} else {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					log.Error(fmt.Errorf("DeleteAgentHost: failed to rollback transaction: %w", rbErr), "")
				}
			}
		}()
	}
	q = db.queries.WithTx(tx.Tx)

	rowsAffected, err := q.DeleteAgentHost(db.ctx, name)
	if err != nil {
		return fmt.Errorf("DeleteAgentHost: error deleting target: %w", err)
	}

	if rowsAffected == 0 {
		return ErrTargetNotFound
	}

	commitNeeded = true
	return nil
}

func (db *Store) GetAgentHost(name string) (AgentHost, error) {
	row, err := db.readQueries.GetAgentHost(db.ctx, name)
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

func (db *Store) GetAllAgentHosts() ([]AgentHost, error) {
	rows, err := db.readQueries.ListAllAgentHosts(db.ctx)
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

func (db *Store) GetAgentHostAuth(hostname string) (string, error) {
	auth, err := db.readQueries.GetAgentHostAuth(db.ctx, hostname)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrAgentHostNotFound
	}
	if err != nil {
		return "", fmt.Errorf("GetAgentHostAuth: error fetching auth: %w", err)
	}

	return fromNullString(auth), nil
}

func (db *Store) LoadAgentHostCert(hostname string) (*x509.Certificate, error) {
	authValue, err := db.GetAgentHostAuth(hostname)
	if err != nil {
		return nil, fmt.Errorf("failed to get auth values for hostname %s: %w", hostname, err)
	}

	decodedCert, err := base64.StdEncoding.DecodeString(authValue)
	if err != nil {
		return nil, fmt.Errorf("auth: failed to decode certificate: %w", err)
	}

	block, _ := pem.Decode(decodedCert)
	if block != nil {
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("auth: failed to parse PEM certificate: %w", err)
		}

		now := time.Now()
		if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
			return nil, fmt.Errorf("certificate for hostname %s is expired or not yet valid", hostname)
		}

		return cert, nil
	}

	cert, err := x509.ParseCertificate(decodedCert)
	if err != nil {
		return nil, fmt.Errorf("auth: failed to parse raw certificate: %w", err)
	}

	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return nil, fmt.Errorf("certificate for hostname %s is expired or not yet valid", hostname)
	}

	return cert, nil
}
