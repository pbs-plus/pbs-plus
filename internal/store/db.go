//go:build linux

package store

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

// Job represents the pbs-disk-backup-job-status model
type Job struct {
	ID               string      `db:"id" json:"id"`
	Store            string      `db:"store" json:"store"`
	Target           string      `db:"target" json:"target"`
	Subpath          string      `db:"subpath" json:"subpath"`
	Schedule         string      `db:"schedule" json:"schedule"`
	Comment          string      `db:"comment" json:"comment"`
	NotificationMode string      `db:"notification_mode" json:"notification-mode"`
	Namespace        string      `db:"namespace" json:"ns"`
	NextRun          *int64      `db:"next_run" json:"next-run"`
	LastRunUpid      *string     `db:"last_run_upid" json:"last-run-upid"`
	LastRunState     *string     `json:"last-run-state"`
	LastRunEndtime   *int64      `json:"last-run-endtime"`
	Duration         *int64      `json:"duration"`
	Exclusions       []Exclusion `json:"exclusions"`
	RawExclusions    string      `json:"rawexclusions"`
}

// Target represents the pbs-model-targets model
type Target struct {
	Name             string `db:"name" json:"name"`
	Path             string `db:"path" json:"path"`
	IsAgent          bool   `json:"is_agent"`
	ConnectionStatus bool   `json:"connection_status"`
}

type Exclusion struct {
	JobID   string `db:"job_id" json:"job_id"`
	Path    string `db:"path" json:"path"`
	Comment string `db:"comment" json:"comment"`
}

type PartialFile struct {
	Path    string `db:"path" json:"path"`
	Comment string `db:"comment" json:"comment"`
}

// Store holds the database instance
type Store struct {
	Db         *sql.DB
	LastToken  *Token
	APIToken   *APIToken
	HTTPClient *http.Client
	WSHub      *websockets.Server
}

// Initialize initializes the database connection and returns a Store instance
func Initialize(wsHub *websockets.Server) (*Store, error) {
	db, err := sql.Open("sqlite3", filepath.Join(DbBasePath, "d2d.db"))
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing sqlite database -> %w", err)
	}

	return &Store{Db: db, WSHub: wsHub}, nil
}

// CreateTables creates the necessary tables in the SQLite database
func (store *Store) CreateTables() error {
	// Create Job table if it doesn't exist
	createJobTable := `
    CREATE TABLE IF NOT EXISTS d2d_jobs (
        id TEXT PRIMARY KEY NOT NULL,
        store TEXT NOT NULL,
        target TEXT NOT NULL,
				subpath TEXT,
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
        name TEXT PRIMARY KEY NOT NULL,
        path TEXT NOT NULL
    );`

	_, err = store.Db.Exec(createTargetTable)
	if err != nil {
		return fmt.Errorf("CreateTables: error creating target table -> %w", err)
	}

	_, exclusionCheck := store.Db.Query("SELECT * FROM exclusions;")

	createExclusionTable := `
    CREATE TABLE IF NOT EXISTS exclusions (
        path TEXT NOT NULL,
        job_id TEXT,
				comment TEXT,
				PRIMARY KEY (path, job_id)
    );`

	_, err = store.Db.Exec(createExclusionTable)
	if err != nil {
		return fmt.Errorf("CreateTables: error creating exclusions table -> %w", err)
	}

	if exclusionCheck != nil {
		for _, path := range defaultExclusions {
			_ = store.CreateExclusion(Exclusion{
				Path:    path,
				Comment: "Generated from default list of exclusions",
			})
		}
	}

	createPartialFileTable := `
    CREATE TABLE IF NOT EXISTS partial_files (
        path TEXT PRIMARY KEY NOT NULL,
				comment TEXT
    );`

	_, err = store.Db.Exec(createPartialFileTable)
	if err != nil {
		return fmt.Errorf("CreateTables: error creating partial_files table -> %w", err)
	}

	return nil
}

// Job CRUD

// CreateJob inserts a new Job into the database
func (store *Store) CreateJob(job Job) error {
	if !utils.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("CreateJob: invalid namespace string -> %s", job.Namespace)
	}

	if !utils.IsValidPathString(job.Subpath) {
		return fmt.Errorf("CreateJob: invalid subpath string -> %s", job.Subpath)
	}

	query := `INSERT INTO d2d_jobs (id, store, target, subpath, schedule, comment, next_run, last_run_upid, notification_mode, namespace) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
	_, err := store.Db.Exec(query, job.ID, job.Store, job.Target, job.Subpath, job.Schedule, job.Comment, job.NextRun, job.LastRunUpid, job.NotificationMode, job.Namespace)
	if err != nil {
		return fmt.Errorf("CreateJob: error inserting data to job table -> %w", err)
	}

	if len(job.Exclusions) > 0 {
		for _, exclusion := range job.Exclusions {
			err := store.CreateExclusion(exclusion)
			if err != nil {
				continue
			}
		}
	}

	err = store.SetSchedule(job)
	if err != nil {
		return fmt.Errorf("CreateJob: error setting job schedule -> %w", err)
	}

	return nil
}

// GetJob retrieves a Job by ID
func (store *Store) GetJob(id string) (*Job, error) {
	query := `SELECT id, store, target, subpath, schedule, comment, next_run, last_run_upid, notification_mode, namespace FROM d2d_jobs WHERE id = ?;`
	row := store.Db.QueryRow(query, id)

	var job Job
	err := row.Scan(&job.ID, &job.Store, &job.Target, &job.Subpath, &job.Schedule, &job.Comment, &job.NextRun, &job.LastRunUpid, &job.NotificationMode, &job.Namespace)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetJob: error scanning job row -> %w", err)
	}

	exclusions, err := store.GetAllJobExclusions(id)
	if err == nil {
		job.Exclusions = exclusions

		pathSlice := []string{}
		for _, exclusion := range exclusions {
			pathSlice = append(pathSlice, exclusion.Path)
		}

		job.RawExclusions = strings.Join(pathSlice, "\n")
	}

	globalExclusions, err := store.GetAllGlobalExclusions()
	if err == nil {
		for _, exclusion := range globalExclusions {
			job.Exclusions = append(job.Exclusions, exclusion)
		}
	}

	if job.LastRunUpid != nil && *job.LastRunUpid != "" {
		task, err := store.GetTaskByUPID(*job.LastRunUpid)
		if err != nil {
			log.Printf("GetJob: error getting task by UPID -> %v\n", err)
		} else {
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
	if !utils.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("UpdateJob: invalid namespace string -> %s", job.Namespace)
	}

	if !utils.IsValidPathString(job.Subpath) {
		return fmt.Errorf("UpdateJob: invalid subpath string -> %s", job.Subpath)
	}

	query := `UPDATE d2d_jobs SET store = ?, target = ?, subpath = ?, schedule = ?, comment = ?, next_run = ?, last_run_upid = ?, notification_mode = ?, namespace = ? WHERE id = ?;`
	_, err := store.Db.Exec(query, job.Store, job.Target, job.Subpath, job.Schedule, job.Comment, job.NextRun, job.LastRunUpid, job.NotificationMode, job.Namespace, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error updating job table -> %w", err)
	}

	query = `DELETE FROM exclusions WHERE job_id = ?;`
	_, err = store.Db.Exec(query, job.ID)
	if err != nil {
		return fmt.Errorf("UpdateJob: error deleting exclusions from table -> %w", err)
	}

	if len(job.Exclusions) > 0 {
		for _, exclusion := range job.Exclusions {
			if exclusion.JobID != job.ID {
				continue
			}

			err := store.CreateExclusion(exclusion)
			if err != nil {
				continue
			}
		}
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
		cmd := exec.Command("/usr/bin/systemctl", "disable", "--now", timerPath)
		cmd.Env = os.Environ()
		_ = cmd.Run()

		_ = os.Remove(fullSvcPath)
		_ = os.Remove(fullTimerPath)
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
		return fmt.Errorf("SetSchedule: error running enable -> %w", err)
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

// GetAllJobs retrieves all Job records from the database
func (store *Store) GetAllJobs() ([]Job, error) {
	query := `SELECT id, store, target, subpath, schedule, comment, next_run, last_run_upid, notification_mode, namespace FROM d2d_jobs WHERE id IS NOT NULL;`
	rows, err := store.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllJobs: error getting job select query -> %w", err)
	}
	defer rows.Close()

	var jobs []Job
	jobs = make([]Job, 0)
	for rows.Next() {
		var job Job
		err := rows.Scan(&job.ID, &job.Store, &job.Target, &job.Subpath, &job.Schedule, &job.Comment, &job.NextRun, &job.LastRunUpid, &job.NotificationMode, &job.Namespace)
		if err != nil {
			return nil, fmt.Errorf("GetAllJobs: error scanning job row -> %w", err)
		}

		exclusions, err := store.GetAllJobExclusions(job.ID)
		if err == nil {
			job.Exclusions = exclusions
			pathSlice := []string{}
			for _, exclusion := range exclusions {
				pathSlice = append(pathSlice, exclusion.Path)
			}

			job.RawExclusions = strings.Join(pathSlice, "\n")
		}

		if job.LastRunUpid != nil && *job.LastRunUpid != "" {
			task, err := store.GetTaskByUPID(*job.LastRunUpid)
			if err != nil {
				log.Printf("GetJob: error getting task by UPID -> %v\n", err)
			} else {
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
		}

		nextSchedule, err := getNextSchedule(&job)
		if err != nil {
			nextSchedule = nil
		}

		if nextSchedule != nil {
			nextSchedUnix := nextSchedule.Unix()
			job.NextRun = &nextSchedUnix
		}

		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

func (store *Store) GetAllJobExclusions(jobId string) ([]Exclusion, error) {
	query := `SELECT path, comment, job_id FROM exclusions WHERE job_id = ?;`
	rows, err := store.Db.Query(query, jobId)
	if err != nil {
		return nil, fmt.Errorf("GetAllJobExclusions: error getting job exclusions select query -> %w", err)
	}
	defer rows.Close()

	var exclusions []Exclusion
	exclusions = make([]Exclusion, 0)
	for rows.Next() {
		var exclusion Exclusion
		err := rows.Scan(&exclusion.Path, &exclusion.Comment, &exclusion.JobID)
		if err != nil {
			return nil, fmt.Errorf("GetAllJobExclusions: error scanning job row -> %w", err)
		}

		exclusions = append(exclusions, exclusion)
	}

	return exclusions, rows.Err()
}

// Target CRUD

// CreateTarget inserts a new Target into the database
func (store *Store) CreateTarget(target Target) error {
	query := `INSERT INTO targets (name, path) VALUES (?, ?);`
	_, err := store.Db.Exec(query, target.Name, target.Path)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return store.UpdateTarget(target)
		}
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

	if strings.HasPrefix(target.Path, "agent://") {
		target.ConnectionStatus = store.AgentPing(&target)
		target.IsAgent = true
	} else {
		target.ConnectionStatus = utils.IsValid(target.Path)
		target.IsAgent = false
	}

	return &target, nil
}

func (store *Store) GetAllTargetsByIP(ip string) ([]Target, error) {
	query := `SELECT name, path FROM targets WHERE path LIKE ?;`
	rows, err := store.Db.Query(query, fmt.Sprintf("agent://%s/%%", ip))
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error getting select query -> %w", err)
	}
	defer rows.Close()

	var targets []Target
	targets = make([]Target, 0)
	for rows.Next() {
		var target Target
		err := rows.Scan(&target.Name, &target.Path)
		if err != nil {
			return nil, fmt.Errorf("GetAllTargetsByIP: error scanning row from targets -> %w", err)
		}

		if strings.HasPrefix(target.Path, "agent://") {
			target.ConnectionStatus = store.AgentPing(&target)
			target.IsAgent = true
		} else {
			target.ConnectionStatus = utils.IsValid(target.Path)
			target.IsAgent = false
		}

		targets = append(targets, target)
	}

	return targets, rows.Err()
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

		if strings.HasPrefix(target.Path, "agent://") {
			target.ConnectionStatus = store.AgentPing(&target)
			target.IsAgent = true
		} else {
			target.ConnectionStatus = utils.IsValid(target.Path)
			target.IsAgent = false
		}

		targets = append(targets, target)
	}

	return targets, rows.Err()
}

func (store *Store) CreateExclusion(exclusion Exclusion) error {
	query := `INSERT INTO exclusions (path, comment, job_id) 
              VALUES (?, ?, ?);`

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")

	if !utils.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	_, err := store.Db.Exec(query, exclusion.Path, exclusion.Comment, exclusion.JobID)
	if err != nil {
		return fmt.Errorf("CreateExclusion: error inserting data to table -> %w", err)
	}

	return nil
}

func (store *Store) GetExclusion(path string) (*Exclusion, error) {
	query := `SELECT path, job_id, comment FROM exclusions WHERE path = ?;`
	row := store.Db.QueryRow(query, path)

	var exclusion Exclusion
	err := row.Scan(&exclusion.Path, &exclusion.JobID, &exclusion.Comment)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetExclusion: error scanning row from exclusions table -> %w", err)
	}

	return &exclusion, nil
}

// UpdateExclusion updates an existing Exclusion in the database
func (store *Store) UpdateExclusion(exclusion Exclusion) error {
	query := `UPDATE exclusions SET comment = ? WHERE path = ? AND job_id = ?;`

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")

	if !utils.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	_, err := store.Db.Exec(query, exclusion.Comment, exclusion.Path, exclusion.JobID)
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error updating exclusions table -> %w", err)
	}
	return nil
}

// DeleteExclusion deletes a Exclusion from the database
func (store *Store) DeleteExclusion(path string) error {
	query := `DELETE FROM exclusions WHERE path = ? AND (job_id IS NULL OR job_id = '');`
	_, err := store.Db.Exec(query, path)
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error deleting exclusion -> %w", err)
	}

	return nil
}

func (store *Store) GetAllGlobalExclusions() ([]Exclusion, error) {
	query := `SELECT path, job_id, comment FROM exclusions WHERE path IS NOT NULL AND (job_id IS NULL OR job_id = '');`
	rows, err := store.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllExclusions: error getting select query -> %w", err)
	}
	defer rows.Close()

	var exclusions []Exclusion
	exclusions = make([]Exclusion, 0)
	for rows.Next() {
		var exclusion Exclusion
		err := rows.Scan(&exclusion.Path, &exclusion.JobID, &exclusion.Comment)
		if err != nil {
			return nil, fmt.Errorf("GetAllExclusions: error scanning row from exclusions -> %w", err)
		}

		exclusions = append(exclusions, exclusion)
	}

	return exclusions, rows.Err()
}

func (store *Store) CreatePartialFile(partialFile PartialFile) error {
	query := `INSERT INTO partial_files (path, comment) 
              VALUES (?, ?);`

	_, err := store.Db.Exec(query, partialFile.Path, partialFile.Comment)
	if err != nil {
		return fmt.Errorf("CreatePartialFile: error inserting data to table -> %w", err)
	}

	return nil
}
func (store *Store) GetPartialFile(path string) (*PartialFile, error) {
	query := `SELECT path, comment FROM partial_files WHERE path = ?;`
	row := store.Db.QueryRow(query, path)

	var partialFile PartialFile
	err := row.Scan(&partialFile.Path, &partialFile.Comment)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetPartialFile: error scanning row from partialFiles table -> %w", err)
	}

	return &partialFile, nil
}

// UpdatePartialFile updates an existing PartialFile in the database
func (store *Store) UpdatePartialFile(partialFile PartialFile) error {
	query := `UPDATE partial_files SET comment = ? WHERE path = ?;`
	_, err := store.Db.Exec(query, partialFile.Comment, partialFile.Path)
	if err != nil {
		return fmt.Errorf("UpdatePartialFile: error updating partialFiles table -> %w", err)
	}
	return nil
}

// DeletePartialFile deletes a PartialFile from the database
func (store *Store) DeletePartialFile(path string) error {
	query := `DELETE FROM partial_files WHERE path = ?;`
	_, err := store.Db.Exec(query, path)
	if err != nil {
		return fmt.Errorf("DeletePartialFile: error deleting partialFile -> %w", err)
	}

	return nil
}

func (store *Store) GetAllPartialFiles() ([]PartialFile, error) {
	query := `SELECT path, comment FROM partial_files WHERE path IS NOT NULL;`
	rows, err := store.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("GetAllPartialFiles: error getting select query -> %w", err)
	}
	defer rows.Close()

	var partialFiles []PartialFile
	partialFiles = make([]PartialFile, 0)
	for rows.Next() {
		var partialFile PartialFile
		err := rows.Scan(&partialFile.Path, &partialFile.Comment)
		if err != nil {
			return nil, fmt.Errorf("GetAllPartialFiles: error scanning row from partialFiles -> %w", err)
		}

		partialFiles = append(partialFiles, partialFile)
	}

	return partialFiles, rows.Err()
}
