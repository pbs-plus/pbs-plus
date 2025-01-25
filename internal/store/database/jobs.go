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
	plugin := &configLib.SectionPlugin{
		FolderPath: database.paths["jobs"],
		TypeName:   "job",
		Properties: map[string]*configLib.Schema{
			"store": {
				Type:        configLib.TypeString,
				Description: "Storage name",
				Required:    true,
			},
			"target": {
				Type:        configLib.TypeString,
				Description: "Backup target",
				Required:    true,
			},
			"subpath": {
				Type:        configLib.TypeString,
				Description: "Backup subpath",
				Required:    false,
			},
			"schedule": {
				Type:        configLib.TypeString,
				Description: "Backup schedule",
				Required:    false,
			},
			"comment": {
				Type:        configLib.TypeString,
				Description: "Comment",
				Required:    false,
			},
			"notification_mode": {
				Type:        configLib.TypeString,
				Description: "Notification mode",
				Required:    false,
			},
			"namespace": {
				Type:        configLib.TypeString,
				Description: "Namespace",
				Required:    false,
			},
			"last_run_upid": {
				Type:        configLib.TypeString,
				Description: "UPID of last ran task",
				Required:    false,
			},
		},
	}

	database.config.RegisterPlugin(plugin)
}

func (database *Database) CreateJob(job types.Job) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	if !utils.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("CreateJob: invalid namespace string -> %s", job.Namespace)
	}

	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("CreateJob: invalid id string -> %s", job.Namespace)
	}

	if err := utils.ValidateOnCalendar(job.Schedule); err != nil && job.Schedule != "" {
		return fmt.Errorf("CreateJob: invalid schedule string -> %s", job.Schedule)
	}

	if !utils.IsValidPathString(job.Subpath) {
		return fmt.Errorf("CreateJob: invalid subpath string -> %s", job.Subpath)
	}

	lastRunUpid := ""
	if job.LastRunUpid != nil {
		lastRunUpid = *job.LastRunUpid
	}

	// Convert job to config format
	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			job.ID: {
				Type: "job",
				ID:   job.ID,
				Properties: map[string]string{
					"store":             job.Store,
					"target":            job.Target,
					"subpath":           job.Subpath,
					"schedule":          job.Schedule,
					"comment":           job.Comment,
					"notification_mode": job.NotificationMode,
					"namespace":         job.Namespace,
					"last_run_upid":     lastRunUpid,
				},
			},
		},
		Order: []string{job.ID},
	}

	if err := database.config.Write(configData); err != nil {
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

	// Set up systemd schedule
	system.SetSchedule(job)

	return nil
}

func (database *Database) GetJob(id string) (*types.Job, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	plugin := database.config.GetPlugin("job")
	jobPath := filepath.Join(plugin.FolderPath, utils.EncodePath(id)+".cfg")
	configData, err := database.config.Parse(jobPath)
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

	lastRunUpid := section.Properties["last_run_upid"]

	// Convert config to Job struct
	job := &types.Job{
		ID:               id,
		Store:            section.Properties["store"],
		Target:           section.Properties["target"],
		Subpath:          section.Properties["subpath"],
		Schedule:         section.Properties["schedule"],
		Comment:          section.Properties["comment"],
		NotificationMode: section.Properties["notification_mode"],
		Namespace:        section.Properties["namespace"],
		LastRunUpid:      &lastRunUpid,
	}

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
	database.mu.Lock()
	defer database.mu.Unlock()

	if !utils.IsValidNamespace(job.Namespace) && job.Namespace != "" {
		return fmt.Errorf("UpdateJob: invalid namespace string -> %s", job.Namespace)
	}

	if !utils.IsValidID(job.ID) && job.ID != "" {
		return fmt.Errorf("CreateJob: invalid id string -> %s", job.Namespace)
	}

	if err := utils.ValidateOnCalendar(job.Schedule); err != nil && job.Schedule != "" {
		return fmt.Errorf("CreateJob: invalid schedule string -> %s", job.Schedule)
	}

	if !utils.IsValidPathString(job.Subpath) {
		return fmt.Errorf("CreateJob: invalid subpath string -> %s", job.Subpath)
	}

	lastRunUpid := ""
	if job.LastRunUpid != nil {
		lastRunUpid = *job.LastRunUpid
	}

	// Convert job to config format
	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			job.ID: {
				Type: "job",
				ID:   job.ID,
				Properties: map[string]string{
					"store":             job.Store,
					"target":            job.Target,
					"subpath":           job.Subpath,
					"schedule":          job.Schedule,
					"comment":           job.Comment,
					"notification_mode": job.NotificationMode,
					"namespace":         job.Namespace,
					"last_run_upid":     lastRunUpid,
				},
			},
		},
		Order: []string{job.ID},
	}

	if err := database.config.Write(configData); err != nil {
		return fmt.Errorf("UpdateJob: error writing config: %w", err)
	}

	// Update exclusions
	plugin := database.config.GetPlugin("exclusion")
	exclusionPath := filepath.Join(plugin.FolderPath, job.ID+".cfg")
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

	system.SetSchedule(job)

	return nil
}

func (database *Database) GetAllJobs() ([]types.Job, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	plugin := database.config.GetPlugin("job")
	files, err := os.ReadDir(plugin.FolderPath)
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
	database.mu.Lock()
	defer database.mu.Unlock()

	plugin := database.config.GetPlugin("job")
	jobPath := filepath.Join(plugin.FolderPath, utils.EncodePath(id)+".cfg")
	if err := os.Remove(jobPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeleteJob: error deleting job file: %w", err)
		}
	}

	system.DeleteSchedule(id)
	return nil
}
