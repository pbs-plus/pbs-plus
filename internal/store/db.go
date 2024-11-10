//go:build linux

package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

// Job represents the pbs-disk-backup-job-status model
type Job struct {
	ID               string  `db:"id" json:"id"`
	Store            string  `db:"store" json:"store"`
	Target           string  `db:"target" json:"target"`
	Schedule         string  `db:"schedule" json:"schedule"`
	Comment          string  `db:"comment" json:"comment"`
	NotificationMode string  `db:"notification_mode" json:"notification-mode"`
	Namespace        string  `db:"namespace" json:"ns"`
	NextRun          *int64  `db:"next_run" json:"next-run"`
	LastRunUpid      *string `db:"last_run_upid" json:"last-run-upid"`
	LastRunState     *string `json:"last-run-state"`
	LastRunEndtime   *int64  `json:"last-run-endtime"`
	Duration         *int64  `json:"duration"`
}

// Target represents the pbs-model-targets model
type Target struct {
	Name             string `db:"name" json:"name"`
	Path             string `db:"path" json:"path"`
	ConnectionStatus bool   `json:"connection_status"`
}

// Store holds the database instance
type Store struct {
	Db         *sql.DB
	LastToken  *Token
	APIToken   *APIToken
	HTTPClient *http.Client
}

// Initialize initializes the database connection and returns a Store instance
func Initialize() (*Store, error) {
	db, err := sql.Open("sqlite3", filepath.Join(DbBasePath, "d2d.db"))
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing sqlite database -> %w", err)
	}

	return &Store{Db: db}, nil
}

// CreateTables creates the necessary tables in the SQLite database
func (store *Store) CreateTables() error {
	// Create Job table if it doesn't exist
	createJobTable := `
    CREATE TABLE IF NOT EXISTS d2d_jobs (
        id TEXT PRIMARY KEY,
        store TEXT,
        target TEXT,
        schedule TEXT,
        comment TEXT,
        next_run INTEGER,
        last_run_upid TEXT,
				namespace TEXT,
				notification_mode TEXT
    );`

	_, err := store.Db.Exec(createJobTable)
	if err != nil {
		return fmt.Errorf("CreateTables: error creating job table -> %w", err)
	}

	// Create Target table if it doesn't exist
	createTargetTable := `
    CREATE TABLE IF NOT EXISTS targets (
        name TEXT PRIMARY KEY,
        path TEXT
    );`

	_, err = store.Db.Exec(createTargetTable)
	if err != nil {
		return fmt.Errorf("CreateTables: error creating target table -> %w", err)
	}

	return nil
}

// Job CRUD

// CreateJob inserts a new Job into the database
func (store *Store) CreateJob(job Job) error {
	query := `INSERT INTO d2d_jobs (id, store, target, schedule, comment, next_run, last_run_upid, notification_mode, namespace) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`
	_, err := store.Db.Exec(query, job.ID, job.Store, job.Target, job.Schedule, job.Comment, job.NextRun, job.LastRunUpid, job.NotificationMode, job.Namespace)
	if err != nil {
		return fmt.Errorf("CreateJob: error inserting data to job table -> %w", err)
	}

	err = store.SetSchedule(job)
	if err != nil {
		return fmt.Errorf("CreateJob: error setting job schedule -> %w", err)
	}

	return nil
}

// GetJob retrieves a Job by ID
func (store *Store) GetJob(id string) (*Job, error) {
	query := `SELECT id, store, target, schedule, comment, next_run, last_run_upid, notification_mode, namespace FROM d2d_jobs WHERE id = ?;`
	row := store.Db.QueryRow(query, id)

	var job Job
	err := row.Scan(&job.ID, &job.Store, &job.Target, &job.Schedule, &job.Comment, &job.NextRun, &job.LastRunUpid, &job.NotificationMode, &job.Namespace)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetJob: error scanning job row -> %w", err)
	}

	if job.LastRunUpid != nil {
		task, err := store.GetTaskByUPID(*job.LastRunUpid)
		if err != nil {
			return nil, fmt.Errorf("GetJob: error getting task by UPID -> %w", err)
		}

		job.LastRunEndtime = &task.EndTime

		if task.Status == "stopped" {
			job.LastRunState = &task.ExitStatus

			tmpDuration := task.EndTime - task.StartTime
			job.Duration = &tmpDuration
		} else {
			tmpDuration := time.Now().Unix() - task.StartTime
			job.Duration = &tmpDuration
		}
	}

	nextSchedule, err := getNextSchedule(&job)
	if err != nil {
		nextSchedule = nil
	}

	if nextSchedule != nil {
		nextSchedUnix := nextSchedule.Unix()
		job.NextRun = &nextSchedUnix
	}

	return &job, nil
}

// UpdateJob updates an existing Job in the database
func (store *Store) UpdateJob(job Job) error {
	query := `UPDATE d2d_jobs SET store = ?, target = ?, schedule = ?, comment = ?, next_run = ?, last_run_upid = ?, notification_mode = ?, namespace = ? WHERE id = ?;`
	_, err := store.Db.Exec(query, job.Store, job.Target, job.Schedule, job.Comment, job.NextRun, job.LastRunUpid, job.NotificationMode, job.Namespace, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error updating job table -> %w", err)
	}

	err = store.SetSchedule(job)
	if err != nil {
		return fmt.Errorf("UpdateJob: error setting job schedule -> %w", err)
	}

	return nil
}

func (store *Store) SetSchedule(job Job) error {
	svcPath := fmt.Sprintf("pbs-plus-job-%s.service", strings.ReplaceAll(job.ID, " ", "-"))
	fullSvcPath := filepath.Join(TimerBasePath, svcPath)

	timerPath := fmt.Sprintf("pbs-plus-job-%s.timer", strings.ReplaceAll(job.ID, " ", "-"))
	fullTimerPath := filepath.Join(TimerBasePath, timerPath)

	if job.Schedule == "" {
		err := os.Remove(fullSvcPath)
		if err != nil {
			return fmt.Errorf("SetSchedule: error removing existing service -> %w", err)
		}

		err = os.Remove(fullTimerPath)
		if err != nil {
			return fmt.Errorf("SetSchedule: error removing existing timer -> %w", err)
		}
	} else {
		err := generateService(&job)
		if err != nil {
			return fmt.Errorf("SetSchedule: error generating service -> %w", err)
		}

		err = generateTimer(&job)
		if err != nil {
			return fmt.Errorf("SetSchedule: error generating timer -> %w", err)
		}
	}

	cmd := exec.Command("/usr/bin/systemctl", "daemon-reload")
	cmd.Env = os.Environ()
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("SetSchedule: error running daemon reload -> %w", err)
	}

	if job.Schedule == "" {
		return nil
	}

	cmd = exec.Command("/usr/bin/systemctl", "enable", "--now", timerPath)
	cmd.Env = os.Environ()
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("SetSchedule: error running enable --now -> %w", err)
	}

	return nil
}

// DeleteJob deletes a Job from the database
func (store *Store) DeleteJob(id string) error {
	query := `DELETE FROM d2d_jobs WHERE id = ?;`
	_, err := store.Db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("DeleteJob: error deleting job from table -> %w", err)
	}
	deleteSchedule(id)

	return nil
}

// Target CRUD

// CreateTarget inserts a new Target into the database
func (store *Store) CreateTarget(target Target) error {
	query := `INSERT INTO targets (name, path) VALUES (?, ?);`
	_, err := store.Db.Exec(query, target.Name, target.Path)
	if err != nil {
		return fmt.Errorf("CreateTarget: error inserting value to targets table -> %w", err)
	}

	return nil
}

// GetTarget retrieves a Target by ID
func (store *Store) GetTarget(name string) (*Target, error) {
	query := `SELECT name, path FROM targets WHERE name = ?;`
	row := store.Db.QueryRow(query, name)

	var target Target
	err := row.Scan(&target.Name, &target.Path)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetTarget: error scanning row from targets table -> %w", err)
	}

	syslogger, err := syslog.InitializeLogger()
	if err != nil {
		return nil, fmt.Errorf("GetTarget: failed to initialize logger -> %w", err)
	}

	if strings.HasPrefix(target.Path, "agent://") {
		target.ConnectionStatus, err = store.AgentPing(&target)
		if err != nil {
			syslogger.Errorf("GetTarget: error agent ping -> %v", err)
		}
	} else {
		target.ConnectionStatus = utils.IsValid(target.Path)
	}

	return &target, nil
}

// UpdateTarget updates an existing Target in the database
func (store *Store) UpdateTarget(target Target) error {
	query := `UPDATE targets SET path = ? WHERE name = ?;`
	_, err := store.Db.Exec(query, target.Path, target.Name)
	if err != nil {
		return fmt.Errorf("UpdateTarget: error updating targets table -> %w", err)
	}
	return nil
}

// DeleteTarget deletes a Target from the database
func (store *Store) DeleteTarget(name string) error {
	query := `DELETE FROM targets WHERE name = ?;`
	_, err := store.Db.Exec(query, name)
	if err != nil {
		return fmt.Errorf("DeleteTarget: error deleting target -> %w", err)
	}

	return nil
}

// GetAllJobes retrieves all Job records from the database
func (store *Store) GetAllJobs() ([]Job, error) {
	query := `SELECT id, store, target, schedule, comment, next_run, last_run_upid, notification_mode, namespace FROM d2d_jobs WHERE id IS NOT NULL;`
	rows, err := store.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllJobs: error getting job select query -> %w", err)
	}
	defer rows.Close()

	var jobs []Job
	jobs = make([]Job, 0)
	for rows.Next() {
		var job Job
		err := rows.Scan(&job.ID, &job.Store, &job.Target, &job.Schedule, &job.Comment, &job.NextRun, &job.LastRunUpid, &job.NotificationMode, &job.Namespace)
		if err != nil {
			return nil, fmt.Errorf("GetAllJobs: error scanning job row -> %w", err)
		}

		if job.LastRunUpid != nil {
			task, err := store.GetTaskByUPID(*job.LastRunUpid)
			if err != nil {
				return nil, fmt.Errorf("GetAllJobs: error getting task by UPID -> %w", err)
			}

			job.LastRunEndtime = &task.EndTime

			if task.Status == "stopped" {
				job.LastRunState = &task.ExitStatus

				tmpDuration := task.EndTime - task.StartTime
				job.Duration = &tmpDuration
			} else {
				tmpDuration := time.Now().Unix() - task.StartTime
				job.Duration = &tmpDuration
			}
		}

		nextSchedule, err := getNextSchedule(&job)
		if err != nil {
			return nil, fmt.Errorf("GetAllJobs: error getting next schedule -> %w", err)
		}

		if nextSchedule != nil {
			nextSchedUnix := nextSchedule.Unix()
			job.NextRun = &nextSchedUnix
		}

		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// Target CRUD

// GetAllTargets retrieves all Target records from the database
func (store *Store) GetAllTargets() ([]Target, error) {
	query := `SELECT name, path FROM targets WHERE name IS NOT NULL;`
	rows, err := store.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error getting select query -> %w", err)
	}
	defer rows.Close()

	var targets []Target
	targets = make([]Target, 0)
	for rows.Next() {
		var target Target
		err := rows.Scan(&target.Name, &target.Path)
		if err != nil {
			return nil, fmt.Errorf("GetAllTargets: error scanning row from targets -> %w", err)
		}

		syslogger, err := syslog.InitializeLogger()
		if err != nil {
			return nil, fmt.Errorf("GetTarget: failed to initialize logger -> %w", err)
		}
		if strings.HasPrefix(target.Path, "agent://") {
			target.ConnectionStatus, err = store.AgentPing(&target)
			if err != nil {
				syslogger.Errorf("GetTarget: error agent ping -> %v", err)
			}
		} else {
			target.ConnectionStatus = utils.IsValid(target.Path)
		}

		targets = append(targets, target)
	}

	return targets, rows.Err()
}
