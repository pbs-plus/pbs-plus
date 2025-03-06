//go:build linux

package database

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/system"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

const maxAttempts = 100

func (database *Database) generateUniqueJobID(job *types.Job) error {
	baseID := utils.Slugify(job.Target)
	if baseID == "" {
		return fmt.Errorf("invalid target: slugified value is empty")
	}

	for idx := 0; idx < maxAttempts; idx++ {
		var newID string
		if idx == 0 {
			newID = baseID
		} else {
			newID = fmt.Sprintf("%s-%d", baseID, idx)
		}

		existing, _ := database.GetJob(newID)
		if existing == nil {
			// Unique id found; assign and exit.
			job.ID = newID
			return nil
		}
	}
	return fmt.Errorf("failed to generate a unique job ID after %d attempts", maxAttempts)
}

func (database *Database) RegisterJobPlugin() {
	plugin := &configLib.SectionPlugin[types.Job]{
		TypeName:   "job",
		FolderPath: database.paths["jobs"],
		Validate: func(config types.Job) error {
			if !utils.IsValidNamespace(config.Namespace) && config.Namespace != "" {
				return fmt.Errorf("invalid namespace string: %s", config.Namespace)
			}
			if err := utils.ValidateOnCalendar(config.Schedule); err != nil && config.Schedule != "" {
				return fmt.Errorf("invalid schedule string: %s", config.Schedule)
			}
			if !utils.IsValidPathString(config.Subpath) {
				return fmt.Errorf("invalid subpath string: %s", config.Subpath)
			}
			return nil
		},
	}

	database.jobsConfig = configLib.NewSectionConfig(plugin)
}

func (database *Database) CreateJob(job types.Job) error {
	if job.ID == "" {
		err := database.generateUniqueJobID(&job)
		if err != nil {
			return fmt.Errorf("CreateJob: failed to generate unique id -> %w", err)
		}
	}

	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("CreateJob: invalid id string -> %s", job.ID)
	}

	jobLogsPath := filepath.Join(constants.JobLogsBasePath, job.ID)
	if err := os.MkdirAll(jobLogsPath, 0755); err != nil {
		syslog.L.Errorf("CreateJob: error creating log directory: %v", err)
	}

	// Convert job to config format
	configData := &configLib.ConfigData[types.Job]{
		Sections: map[string]*configLib.Section[types.Job]{
			job.ID: {
				Type: "job",
				ID:   job.ID,
				Properties: types.Job{
					Store:            job.Store,
					Mode:             job.Mode,
					Target:           job.Target,
					Subpath:          job.Subpath,
					Schedule:         job.Schedule,
					Comment:          job.Comment,
					NotificationMode: job.NotificationMode,
					Namespace:        job.Namespace,
					CurrentPID:       job.CurrentPID,
					LastRunUpid:      job.LastRunUpid,
					LastRunPlusError: job.LastRunPlusError,
					LastRunPlusTime:  job.LastRunPlusTime,
					Retry:            job.Retry,
				},
			},
		},
		Order: []string{job.ID},
	}

	if err := database.jobsConfig.Write(configData); err != nil {
		return fmt.Errorf("CreateJob: error writing config: %w", err)
	}

	// Handle exclusions
	if len(job.Exclusions) > 0 {
		for _, exclusion := range job.Exclusions {
			err := database.CreateExclusion(exclusion)
			if err != nil {
				continue
			}
		}
	}

	if err := system.SetSchedule(job); err != nil {
		syslog.L.Errorf("CreateJob: error setting schedule: %v", err)
	}

	return nil
}

func (database *Database) GetJob(id string) (*types.Job, error) {
	job, err := database.getJob(id)
	if err != nil {
		return nil, err
	}

	if job == nil {
		return nil, nil
	}

	// Get UPIDs
	jobLogsPath := filepath.Join(constants.JobLogsBasePath, job.ID)
	upids, err := os.ReadDir(jobLogsPath)
	if err == nil {
		job.UPIDs = make([]string, len(upids))
		for i, upid := range upids {
			job.UPIDs[i] = upid.Name()
		}
	}

	// Get global exclusions
	globalExclusions, err := database.GetAllGlobalExclusions()
	if err == nil && globalExclusions != nil {
		job.Exclusions = append(job.Exclusions, globalExclusions...)
	}

	return job, nil
}

func (database *Database) getJob(id string) (*types.Job, error) {
	jobPath := filepath.Join(database.paths["jobs"], utils.EncodePath(id)+".cfg")
	configData, err := database.jobsConfig.Parse(jobPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetJob: error reading config: %w", err)
	}

	section, exists := configData.Sections[id]
	if !exists {
		return nil, fmt.Errorf("GetJob: section %s does not exist", id)
	}

	// Convert config to Job struct
	job := &section.Properties
	job.ID = id

	// Get exclusions
	exclusions, err := database.GetAllJobExclusions(id)
	if err == nil && exclusions != nil {
		job.Exclusions = exclusions
		pathSlice := []string{}
		for _, exclusion := range exclusions {
			pathSlice = append(pathSlice, exclusion.Path)
		}
		job.RawExclusions = strings.Join(pathSlice, "\n")
	}

	if job.LastRunPlusError != "" {
		job.LastRunState = &job.LastRunPlusError
		lastRunPlusTime := int64(job.LastRunPlusTime)
		job.LastRunEndtime = &lastRunPlusTime
		job.LastRunUpid = ""
	} else if job.LastRunUpid != "" {
		task, err := proxmox.Session.GetTaskByUPID(job.LastRunUpid)
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

	if job.LastSuccessfulUpid != "" {
		successTask, err := proxmox.Session.GetTaskByUPID(job.LastSuccessfulUpid)
		if err != nil {
			log.Printf("GetJob: error getting task by UPID -> %v\n", err)
		} else {
			job.LastSuccessfulEndtime = &successTask.EndTime
		}
	}

	// Get next schedule
	nextSchedule, err := system.GetNextSchedule(job)
	if err == nil && nextSchedule != nil {
		nextSchedUnix := nextSchedule.Unix()
		job.NextRun = &nextSchedUnix
	}

	return job, nil
}

func (database *Database) UpdateJob(job types.Job) error {
	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("UpdateJob: invalid id string -> %s", job.ID)
	}

	// Convert job to config format
	configData := &configLib.ConfigData[types.Job]{
		Sections: map[string]*configLib.Section[types.Job]{
			job.ID: {
				Type:       "job",
				ID:         job.ID,
				Properties: job,
			},
		},
		Order: []string{job.ID},
	}

	if err := database.jobsConfig.Write(configData); err != nil {
		return fmt.Errorf("UpdateJob: error writing config: %w", err)
	}

	// Update exclusions
	exclusionFileName := utils.EncodePath(job.ID)
	exclusionPath := filepath.Join(database.paths["exclusions"], exclusionFileName+".cfg")
	if err := os.RemoveAll(exclusionPath); err != nil {
		return fmt.Errorf("UpdateJob: error removing old exclusions: %w", err)
	}

	for _, exclusion := range job.Exclusions {
		if exclusion.JobID != job.ID {
			continue
		}
		err := database.CreateExclusion(exclusion)
		if err != nil {
			syslog.L.Errorf("UpdateJob: error creating job exclusion: %v", err)
			continue
		}
	}

	if err := system.SetSchedule(job); err != nil {
		syslog.L.Errorf("UpdateJob: error setting schedule: %v", err)
	}

	if job.LastRunUpid != "" {
		jobLogsPath := filepath.Join(constants.JobLogsBasePath, job.ID)
		if err := os.MkdirAll(jobLogsPath, 0755); err != nil {
			syslog.L.Errorf("UpdateJob: error creating log directory: %v", err)
		} else {
			jobLogPath := filepath.Join(jobLogsPath, job.LastRunUpid)
			if _, err := os.Lstat(jobLogPath); err != nil {
				origLogPath, err := proxmox.GetLogPath(job.LastRunUpid)
				if err != nil {
					syslog.L.Errorf("UpdateJob: failed to get original log path %s: %v", jobLogPath, err)
				}
				err = os.Symlink(origLogPath, jobLogPath)
				if err != nil {
					syslog.L.Errorf("UpdateJob: failed to link original log %s: %v", jobLogPath, err)
				}
			}
		}
	}

	return nil
}

func (database *Database) GetAllJobs() ([]types.Job, error) {
	files, err := os.ReadDir(database.paths["jobs"])
	if err != nil {
		return nil, fmt.Errorf("GetAllJobs: error reading jobs directory: %w", err)
	}

	var jobs []types.Job
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		job, err := database.getJob(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil || job == nil {
			syslog.L.Errorf("GetAllJobs: error getting job: %v", err)
			continue
		}

		target, err := database.GetTarget(job.Target)
		if err == nil && target != nil {
			job.ExpectedSize = utils.HumanReadableBytes(int64(target.DriveUsedBytes))
		}

		jobs = append(jobs, *job)
	}

	return jobs, nil
}

func (database *Database) DeleteJob(id string) error {
	jobPath := filepath.Join(database.paths["jobs"], utils.EncodePath(id)+".cfg")
	if err := os.Remove(jobPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeleteJob: error deleting job file: %w", err)
		}
	}

	jobLogsPath := filepath.Join(constants.JobLogsBasePath, id)
	if err := os.RemoveAll(jobLogsPath); err != nil {
		if !os.IsNotExist(err) {
			syslog.L.Errorf("DeleteJob: error deleting job logs folder: %v", err)
		}
	}

	if err := system.DeleteSchedule(id); err != nil {
		syslog.L.Errorf("DeleteJob: error deleting schedule: %v", err)
	}

	return nil
}
