//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	_ "modernc.org/sqlite"
)

// CreateExclusion inserts a new exclusion into the database.
func (database *Database) CreateExclusion(tx *sql.Tx, exclusion types.Exclusion) error {
	if tx == nil {
		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	if exclusion.Path == "" {
		return errors.New("path is required for an exclusion")
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	_, err := tx.Exec(`
        INSERT INTO exclusions (section_id, job_id, path, comment)
        VALUES (?, ?, ?, ?)
    `, sectionID, exclusion.JobID, exclusion.Path, exclusion.Comment)
	if err != nil {
		return fmt.Errorf("CreateExclusion: error inserting exclusion: %w", err)
	}
	return nil
}

// GetAllJobExclusions returns all exclusions associated with a job.
func (database *Database) GetAllJobExclusions(jobId string) ([]types.Exclusion, error) {
	rows, err := database.readDb.Query(`
        SELECT section_id, job_id, path, comment FROM exclusions
        WHERE job_id = ?
    `, jobId)
	if err != nil {
		return nil, fmt.Errorf("GetAllJobExclusions: error querying exclusions: %w", err)
	}
	defer rows.Close()

	var exclusions []types.Exclusion
	seenPaths := make(map[string]bool)

	for rows.Next() {
		var sectionID string
		var excl types.Exclusion
		if err := rows.Scan(&sectionID, &excl.JobID, &excl.Path, &excl.Comment); err != nil {
			continue // Skip problematic rows.
		}
		if seenPaths[excl.Path] {
			continue
		}
		seenPaths[excl.Path] = true
		exclusions = append(exclusions, excl)
	}
	return exclusions, nil
}

// GetAllGlobalExclusions returns all exclusions that are not tied to any job.
func (database *Database) GetAllGlobalExclusions() ([]types.Exclusion, error) {
	rows, err := database.readDb.Query(`
        SELECT section_id, job_id, path, comment FROM exclusions
        WHERE job_id IS NULL OR job_id = ''
    `)
	if err != nil {
		return nil, fmt.Errorf("GetAllGlobalExclusions: error querying exclusions: %w", err)
	}
	defer rows.Close()

	var exclusions []types.Exclusion
	seenPaths := make(map[string]bool)
	for rows.Next() {
		var sectionID string
		var excl types.Exclusion
		if err := rows.Scan(&sectionID, &excl.JobID, &excl.Path, &excl.Comment); err != nil {
			continue
		}
		if seenPaths[excl.Path] {
			continue
		}
		seenPaths[excl.Path] = true
		exclusions = append(exclusions, excl)
	}
	return exclusions, nil
}

// GetExclusion retrieves a single exclusion by its path.
func (database *Database) GetExclusion(path string) (*types.Exclusion, error) {
	sectionID := fmt.Sprintf("excl-%s", path)
	row := database.readDb.QueryRow(`
        SELECT job_id, path, comment FROM exclusions WHERE section_id = ?
    `, sectionID)
	var excl types.Exclusion
	err := row.Scan(&excl.JobID, &excl.Path, &excl.Comment)
	if err != nil {
		return nil, fmt.Errorf("GetExclusion: exclusion not found for path: %s", path)
	}
	return &excl, nil
}

// UpdateExclusion updates an existing exclusion.
func (database *Database) UpdateExclusion(tx *sql.Tx, exclusion types.Exclusion) error {
	if tx == nil {
		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	if exclusion.Path == "" {
		return errors.New("path is required for an exclusion")
	}

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	res, err := tx.Exec(`
        UPDATE exclusions SET job_id = ?, comment = ? WHERE section_id = ?
    `, exclusion.JobID, exclusion.Comment, sectionID)
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error updating exclusion: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil || affected == 0 {
		return fmt.Errorf("UpdateExclusion: exclusion not found for path: %s", exclusion.Path)
	}
	return nil
}

// DeleteExclusion removes an exclusion from the database.
func (database *Database) DeleteExclusion(tx *sql.Tx, path string) error {
	if tx == nil {
		var err error
		tx, err = database.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Commit()
	}

	path = strings.ReplaceAll(path, "\\", "/")
	sectionID := fmt.Sprintf("excl-%s", path)
	res, err := tx.Exec(`
        DELETE FROM exclusions WHERE section_id = ?
    `, sectionID)
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error deleting exclusion: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil || affected == 0 {
		return fmt.Errorf("DeleteExclusion: exclusion not found for path: %s", path)
	}
	return nil
}
