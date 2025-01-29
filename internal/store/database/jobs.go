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
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/system"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

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
	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("CreateJob: invalid id string -> %s", job.ID)
	}

	lastRunUpid := ""
	if job.LastRunUpid != nil {
		lastRunUpid = *job.LastRunUpid
	}

	// Convert job to config format
	configData := &configLib.ConfigData[types.Job]{
		Sections: map[string]*configLib.Section[types.Job]{
			job.ID: {
				Type: "job",
				ID:   job.ID,
				Properties: types.Job{
					Store:            job.Store,
					Target:           job.Target,
					Subpath:          job.Subpath,
					Schedule:         job.Schedule,
					Comment:          job.Comment,
					NotificationMode: job.NotificationMode,
					Namespace:        job.Namespace,
					LastRunUpid:      &lastRunUpid,
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
		return nil, nil
	}

	lastRunUpid := section.Properties.LastRunUpid

	// Convert config to Job struct
	job := &section.Properties
	job.ID = id
	job.LastRunUpid = lastRunUpid

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

	// Get global exclusions
	globalExclusions, err := database.GetAllGlobalExclusions()
	if err == nil && globalExclusions != nil {
		job.Exclusions = append(job.Exclusions, globalExclusions...)
	}

	// Update dynamic fields
	if job.LastRunUpid != nil && *job.LastRunUpid != "" {
		task, err := proxmox.Session.GetTaskByUPID(*job.LastRunUpid)
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

	lastRunUpid := ""
	if job.LastRunUpid != nil {
		lastRunUpid = *job.LastRunUpid
	}

	job.LastRunUpid = &lastRunUpid

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
	exclusionPath := filepath.Join(database.paths["exclusions"], job.ID+".cfg")
	if err := os.RemoveAll(exclusionPath); err != nil {
		return fmt.Errorf("UpdateJob: error removing old exclusions: %w", err)
	}

	if len(job.Exclusions) > 0 {
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
	}

	if err := system.SetSchedule(job); err != nil {
		syslog.L.Errorf("UpdateJob: error setting schedule: %v", err)
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

		job, err := database.GetJob(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil || job == nil {
			syslog.L.Errorf("GetAllJobs: error getting job: %v", err)
			continue
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

	if err := system.DeleteSchedule(id); err != nil {
		syslog.L.Errorf("DeleteJob: error deleting schedule: %v", err)
	}

	return nil
}
