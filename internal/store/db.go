//go:build linux

package store

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

var defaultPaths = map[string]string{
	"jobs":         "/etc/proxmox-backup/pbs-plus/jobs.d",
	"targets":      "/etc/proxmox-backup/pbs-plus/targets.d",
	"exclusions":   "/etc/proxmox-backup/pbs-plus/exclusions.d",
	"partialfiles": "/etc/proxmox-backup/pbs-plus/partialfiles.d",
}

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

// Store holds the configuration system
type Store struct {
	mu         sync.RWMutex
	config     *configLib.SectionConfig
	LastToken  *Token
	APIToken   *APIToken
	HTTPClient *http.Client
	WSHub      *websockets.Server
}

func Initialize(wsHub *websockets.Server, paths map[string]string) (*Store, error) {
	// Create base directories
	if paths == nil {
		paths = defaultPaths
	}

	for _, path := range paths {
		if err := os.MkdirAll(path, 0750); err != nil {
			return nil, fmt.Errorf("Initialize: error creating directory %s: %w", path, err)
		}
	}

	// Initialize config system
	minLength := 3
	idSchema := &configLib.Schema{
		Type:        configLib.TypeString,
		Description: "Section identifier",
		Required:    true,
		MinLength:   &minLength,
	}

	config := configLib.NewSectionConfig(idSchema)

	// Register plugins
	jobPlugin := &configLib.SectionPlugin{
		FolderPath: paths["jobs"],
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
		},
	}

	targetPlugin := &configLib.SectionPlugin{
		FolderPath: paths["targets"],
		TypeName:   "target",
		Properties: map[string]*configLib.Schema{
			"path": {
				Type:        configLib.TypeString,
				Description: "Target path",
				Required:    true,
			},
		},
	}

	exclusionPlugin := &configLib.SectionPlugin{
		FolderPath: paths["exclusions"],
		TypeName:   "exclusion",
		Properties: map[string]*configLib.Schema{
			"path": {
				Type:        configLib.TypeString,
				Description: "Exclusion path pattern",
				Required:    true,
			},
			"comment": {
				Type:        configLib.TypeString,
				Description: "Exclusion comment",
				Required:    false,
			},
			"job_id": {
				Type:        configLib.TypeString,
				Description: "Associated job ID",
				Required:    false,
			},
		},
		Validations: []configLib.ValidationFunc{
			func(data map[string]string) error {
				if !utils.IsValidPattern(data["path"]) {
					return fmt.Errorf("invalid exclusion pattern: %s", data["path"])
				}
				return nil
			},
		},
	}

	partialFilePlugin := &configLib.SectionPlugin{
		FolderPath: paths["partialfiles"],
		TypeName:   "partialfile",
		Properties: map[string]*configLib.Schema{
			"path": {
				Type:        configLib.TypeString,
				Description: "File path",
				Required:    true,
			},
			"comment": {
				Type:        configLib.TypeString,
				Description: "File comment",
				Required:    false,
			},
		},
	}

	config.RegisterPlugin(jobPlugin)
	config.RegisterPlugin(targetPlugin)
	config.RegisterPlugin(exclusionPlugin)
	config.RegisterPlugin(partialFilePlugin)

	store := &Store{
		config:     config,
		HTTPClient: &http.Client{},
		WSHub:      wsHub,
	}

	return store, nil
}

// Job CRUD functions maintaining the same interface

func (store *Store) CreateJob(job Job) error {
	store.mu.Lock()
	defer store.mu.Unlock()

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
				},
			},
		},
		Order: []string{job.ID},
	}

	if err := store.config.Write(configData); err != nil {
		return fmt.Errorf("CreateJob: error writing config: %w", err)
	}

	// Handle exclusions
	if len(job.Exclusions) > 0 {
		for _, exclusion := range job.Exclusions {
			err := store.CreateExclusion(exclusion)
			if err != nil {
				continue
			}
		}
	}

	// Set up systemd schedule
	store.SetSchedule(job)

	return nil
}

func (store *Store) GetJob(id string) (*Job, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	plugin := store.config.GetPlugin("job")
	jobPath := filepath.Join(plugin.FolderPath, utils.EncodePath(id)+".cfg")
	configData, err := store.config.Parse(jobPath)
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

	// Convert config to Job struct
	job := &Job{
		ID:               id,
		Store:            section.Properties["store"],
		Target:           section.Properties["target"],
		Subpath:          section.Properties["subpath"],
		Schedule:         section.Properties["schedule"],
		Comment:          section.Properties["comment"],
		NotificationMode: section.Properties["notification_mode"],
		Namespace:        section.Properties["namespace"],
	}

	// Get exclusions
	exclusions, err := store.GetAllJobExclusions(id)
	if err == nil && exclusions != nil {
		job.Exclusions = exclusions
		pathSlice := []string{}
		for _, exclusion := range exclusions {
			pathSlice = append(pathSlice, exclusion.Path)
		}
		job.RawExclusions = strings.Join(pathSlice, "\n")
	}

	// Get global exclusions
	globalExclusions, err := store.GetAllGlobalExclusions()
	if err == nil && globalExclusions != nil {
		job.Exclusions = append(job.Exclusions, globalExclusions...)
	}

	// Update dynamic fields
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

	// Get next schedule
	nextSchedule, err := getNextSchedule(job)
	if err == nil && nextSchedule != nil {
		nextSchedUnix := nextSchedule.Unix()
		job.NextRun = &nextSchedUnix
	}

	return job, nil
}

func (store *Store) UpdateJob(job Job) error {
	store.mu.Lock()
	defer store.mu.Unlock()

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
				},
			},
		},
		Order: []string{job.ID},
	}

	if err := store.config.Write(configData); err != nil {
		return fmt.Errorf("UpdateJob: error writing config: %w", err)
	}

	// Update exclusions
	plugin := store.config.GetPlugin("exclusion")
	exclusionPath := filepath.Join(plugin.FolderPath, job.ID+".cfg")
	if err := os.RemoveAll(exclusionPath); err != nil {
		return fmt.Errorf("UpdateJob: error removing old exclusions: %w", err)
	}

	if len(job.Exclusions) > 0 {
		for _, exclusion := range job.Exclusions {
			if exclusion.JobID != job.ID {
				continue
			}
			err := store.CreateExclusion(exclusion)
			if err != nil {
				syslog.L.Errorf("UpdateJob: error creating job exclusion: %v", err)
				continue
			}
		}
	}

	store.SetSchedule(job)

	return nil
}

func (store *Store) GetAllJobs() ([]Job, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	plugin := store.config.GetPlugin("job")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllJobs: error reading jobs directory: %w", err)
	}

	var jobs []Job
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		job, err := store.GetJob(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil || job == nil {
			syslog.L.Errorf("GetAllJobs: error getting job: %v", err)
			continue
		}
		jobs = append(jobs, *job)
	}

	return jobs, nil
}

func (store *Store) DeleteJob(id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	plugin := store.config.GetPlugin("job")
	jobPath := filepath.Join(plugin.FolderPath, utils.EncodePath(id)+".cfg")
	if err := os.Remove(jobPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeleteJob: error deleting job file: %w", err)
		}
	}

	deleteSchedule(id)
	return nil
}

// Target functions
func (store *Store) CreateTarget(target Target) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if target.Path == "" {
		return fmt.Errorf("UpdateTarget: target path empty -> %s", target.Path)
	}

	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("UpdateTarget: invalid target path -> %s", target.Path)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			target.Name: {
				Type: "target",
				ID:   target.Name,
				Properties: map[string]string{
					"path": target.Path,
				},
			},
		},
		Order: []string{target.Name},
	}

	if err := store.config.Write(configData); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return store.UpdateTarget(target)
		}
		return fmt.Errorf("CreateTarget: error writing config: %w", err)
	}

	return nil
}

func (store *Store) GetTarget(name string) (*Target, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	plugin := store.config.GetPlugin("target")
	targetPath := filepath.Join(plugin.FolderPath, utils.EncodePath(name)+".cfg")
	configData, err := store.config.Parse(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetTarget: error reading config: %w", err)
	}

	section, exists := configData.Sections[name]
	if !exists {
		return nil, nil
	}

	target := &Target{
		Name: name,
		Path: section.Properties["path"],
	}

	if strings.HasPrefix(target.Path, "agent://") {
		target.ConnectionStatus = store.AgentPing(target)
		target.IsAgent = true
	} else {
		target.ConnectionStatus = utils.IsValid(target.Path)
		target.IsAgent = false
	}

	return target, nil
}

func (store *Store) UpdateTarget(target Target) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if target.Path == "" {
		return fmt.Errorf("UpdateTarget: target path empty -> %s", target.Path)
	}

	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("UpdateTarget: invalid target path -> %s", target.Path)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			target.Name: {
				Type: "target",
				ID:   target.Name,
				Properties: map[string]string{
					"path": target.Path,
				},
			},
		},
		Order: []string{target.Name},
	}

	if err := store.config.Write(configData); err != nil {
		return fmt.Errorf("UpdateTarget: error writing config: %w", err)
	}

	return nil
}

func (store *Store) DeleteTarget(name string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	plugin := store.config.GetPlugin("target")
	targetPath := filepath.Join(plugin.FolderPath, utils.EncodePath(name)+".cfg")
	if err := os.Remove(targetPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeleteTarget: error deleting target file: %w", err)
		}
	}

	return nil
}

func (store *Store) GetAllTargets() ([]Target, error) {
	plugin := store.config.GetPlugin("target")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error reading targets directory: %w", err)
	}

	var targets []Target
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		target, err := store.GetTarget(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil {
			syslog.L.Errorf("GetAllTargets: error getting target: %v", err)
			continue
		}
		targets = append(targets, *target)
	}

	return targets, nil
}

func (store *Store) GetAllTargetsByIP(clientIP string) ([]Target, error) {
	// Get all targets first
	plugin := store.config.GetPlugin("target")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error reading targets directory: %w", err)
	}

	var targets []Target
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		target, err := store.GetTarget(utils.DecodePath(file.Name()))
		if err != nil || target == nil {
			syslog.L.Errorf("GetAllTargetsByIP: error getting target: %v", err)
			continue
		}

		// Check if it's an agent target and matches the clientIP
		if target.IsAgent {
			// Split path into parts: ["agent:", "", "<clientIP>", "<driveLetter>"]
			parts := strings.Split(target.Path, "/")
			if len(parts) >= 3 && parts[2] == clientIP {
				targets = append(targets, *target)
			}
		}
	}

	return targets, nil
}

// Exclusion functions
func (store *Store) CreateExclusion(exclusion Exclusion) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")

	if !utils.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	sectionType := "exclusion"
	filename := utils.EncodePath(exclusion.JobID)
	sectionProperties := map[string]string{
		"path":    exclusion.Path,
		"comment": exclusion.Comment,
		"job_id":  exclusion.JobID,
	}
	plugin := store.config.GetPlugin("exclusion")

	if exclusion.JobID == "" {
		sectionProperties = map[string]string{
			"path":    exclusion.Path,
			"comment": exclusion.Comment,
		}
		filename = "global"
	}

	configPath := filepath.Join(plugin.FolderPath, filename+".cfg")

	// Read existing exclusions
	var configData *configLib.ConfigData
	existing, err := store.config.Parse(configPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("CreateExclusion: error reading existing config: %w", err)
	}

	if existing != nil {
		configData = existing
	} else {
		configData = &configLib.ConfigData{
			Sections: make(map[string]*configLib.Section),
			Order:    make([]string, 0),
			FilePath: configPath,
		}
	}

	// Add new exclusion
	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	configData.Sections[sectionID] = &configLib.Section{
		Type:       sectionType,
		ID:         sectionID,
		Properties: sectionProperties,
	}
	configData.Order = append(configData.Order, sectionID)

	if err := store.config.Write(configData); err != nil {
		return fmt.Errorf("CreateExclusion: error writing config: %w", err)
	}

	return nil
}

func (store *Store) GetAllJobExclusions(jobId string) ([]Exclusion, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	plugin := store.config.GetPlugin("exclusion")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(jobId)+".cfg")
	configData, err := store.config.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []Exclusion{}, nil
		}
		return nil, fmt.Errorf("GetAllJobExclusions: error reading config: %w", err)
	}
	var exclusions []Exclusion
	seenPaths := make(map[string]bool)

	for _, sectionID := range configData.Order {
		section, exists := configData.Sections[sectionID]
		if !exists || section.Type != "exclusion" {
			continue
		}
		path := section.Properties["path"]
		if seenPaths[path] {
			continue // Skip duplicates
		}
		seenPaths[path] = true
		exclusions = append(exclusions, Exclusion{
			Path:    path,
			Comment: section.Properties["comment"],
			JobID:   jobId,
		})
	}
	return exclusions, nil
}

func (store *Store) GetAllGlobalExclusions() ([]Exclusion, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	plugin := store.config.GetPlugin("exclusion")
	configPath := filepath.Join(plugin.FolderPath, "global.cfg")
	configData, err := store.config.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []Exclusion{}, nil
		}
		return nil, fmt.Errorf("GetAllGlobalExclusions: error reading config: %w", err)
	}
	var exclusions []Exclusion
	seenPaths := make(map[string]bool)

	for _, sectionID := range configData.Order {
		section, exists := configData.Sections[sectionID]
		if !exists || section.Type != "exclusion" {
			continue
		}
		path := section.Properties["path"]
		if seenPaths[path] {
			continue // Skip duplicates
		}
		seenPaths[path] = true
		exclusions = append(exclusions, Exclusion{
			Path:    path,
			Comment: section.Properties["comment"],
		})
	}
	return exclusions, nil
}

func (store *Store) GetExclusion(path string) (*Exclusion, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	// Check global exclusions first
	plugin := store.config.GetPlugin("exclusion")
	globalPath := filepath.Join(plugin.FolderPath, "global.cfg")
	if configData, err := store.config.Parse(globalPath); err == nil {
		sectionID := fmt.Sprintf("excl-%s", path)
		if section, exists := configData.Sections[sectionID]; exists && section.Type == "exclusion" {
			return &Exclusion{
				Path:    section.Properties["path"],
				Comment: section.Properties["comment"],
			}, nil
		}
	}

	// Check job-specific exclusions
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetExclusion: error reading directory: %w", err)
	}

	for _, file := range files {
		if file.Name() == "global" {
			continue
		}
		configPath := filepath.Join(plugin.FolderPath, file.Name())
		configData, err := store.config.Parse(configPath)
		if err != nil {
			continue
		}

		sectionID := fmt.Sprintf("excl-%s", path)
		if section, exists := configData.Sections[sectionID]; exists && section.Type == "exclusion" {
			return &Exclusion{
				Path:    section.Properties["path"],
				Comment: section.Properties["comment"],
				JobID:   section.Properties["job_id"],
			}, nil
		}
	}

	return nil, fmt.Errorf("GetExclusion: exclusion not found for path: %s", path)
}

func (store *Store) UpdateExclusion(exclusion Exclusion) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !utils.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	// Try to update in global exclusions first
	plugin := store.config.GetPlugin("exclusion")
	configPath := filepath.Join(plugin.FolderPath, "global.cfg")
	if exclusion.JobID != "" {
		configPath = filepath.Join(plugin.FolderPath, utils.EncodePath(exclusion.JobID)+".cfg")
	}

	configData, err := store.config.Parse(configPath)
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error reading config: %w", err)
	}

	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	section, exists := configData.Sections[sectionID]
	if !exists {
		return fmt.Errorf("UpdateExclusion: exclusion not found for path: %s", exclusion.Path)
	}

	// Update properties
	if exclusion.JobID == "" {
		section.Properties = map[string]string{
			"path":    exclusion.Path,
			"comment": exclusion.Comment,
		}
	} else {
		section.Properties = map[string]string{
			"path":    exclusion.Path,
			"comment": exclusion.Comment,
			"job_id":  exclusion.JobID,
		}
	}

	return store.config.Write(configData)
}

func (store *Store) DeleteExclusion(path string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	path = strings.ReplaceAll(path, "\\", "/")
	plugin := store.config.GetPlugin("exclusion")
	sectionID := fmt.Sprintf("excl-%s", path)

	// Try job-specific exclusions first
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error reading directory: %w", err)
	}

	for _, file := range files {
		if file.Name() == "global" {
			continue
		}
		configPath := filepath.Join(plugin.FolderPath, file.Name())
		configData, err := store.config.Parse(configPath)
		if err != nil {
			continue
		}

		if _, exists := configData.Sections[sectionID]; exists {
			delete(configData.Sections, sectionID)
			newOrder := make([]string, 0)
			for _, id := range configData.Order {
				if id != sectionID {
					newOrder = append(newOrder, id)
				}
			}
			configData.Order = newOrder

			// If the config is empty after deletion, remove the file
			if len(configData.Sections) == 0 {
				if err := os.Remove(configPath); err != nil {
					return fmt.Errorf("DeleteExclusion: error removing empty config file: %w", err)
				}
				return nil
			}

			// Otherwise write the updated config
			if err := store.config.Write(configData); err != nil {
				return fmt.Errorf("DeleteExclusion: error writing config: %w", err)
			}
			return nil
		}
	}

	// Try global exclusion
	globalPath := filepath.Join(plugin.FolderPath, "global.cfg")
	if configData, err := store.config.Parse(globalPath); err == nil {
		if _, exists := configData.Sections[sectionID]; exists {
			delete(configData.Sections, sectionID)
			newOrder := make([]string, 0)
			for _, id := range configData.Order {
				if id != sectionID {
					newOrder = append(newOrder, id)
				}
			}
			configData.Order = newOrder

			// If the global config is empty after deletion, remove the file
			if len(configData.Sections) == 0 {
				if err := os.Remove(globalPath); err != nil {
					return fmt.Errorf("DeleteExclusion: error removing empty global config file: %w", err)
				}
				return nil
			}

			// Otherwise write the updated config
			if err := store.config.Write(configData); err != nil {
				return fmt.Errorf("DeleteExclusion: error writing global config: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("DeleteExclusion: exclusion not found for path: %s", path)
}

// PartialFile functions
func (store *Store) CreatePartialFile(partialFile PartialFile) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !utils.IsValidPattern(partialFile.Path) {
		return fmt.Errorf("CreatePartialFile: invalid path pattern -> %s", partialFile.Path)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			partialFile.Path: {
				Type: "partialfile",
				ID:   partialFile.Path,
				Properties: map[string]string{
					"path":    partialFile.Path,
					"comment": partialFile.Comment,
				},
			},
		},
		Order: []string{partialFile.Path},
	}

	if err := store.config.Write(configData); err != nil {
		return fmt.Errorf("CreatePartialFile: error writing config: %w", err)
	}

	return nil
}

func (store *Store) GetPartialFile(path string) (*PartialFile, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	plugin := store.config.GetPlugin("partialfile")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(path)+".cfg")
	configData, err := store.config.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetPartialFile: error reading config: %w", err)
	}

	section, exists := configData.Sections[path]
	if !exists {
		return nil, nil
	}

	return &PartialFile{
		Path:    section.Properties["path"],
		Comment: section.Properties["comment"],
	}, nil
}

func (store *Store) UpdatePartialFile(partialFile PartialFile) error {
	return store.CreatePartialFile(partialFile)
}

func (store *Store) DeletePartialFile(path string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	plugin := store.config.GetPlugin("partialfile")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(path)+".cfg")
	if err := os.Remove(configPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeletePartialFile: error deleting file: %w", err)
		}
	}

	return nil
}

func (store *Store) GetAllPartialFiles() ([]PartialFile, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	plugin := store.config.GetPlugin("partialfile")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllPartialFiles: error reading directory: %w", err)
	}

	var partialFiles []PartialFile
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		configPath := filepath.Join(plugin.FolderPath, file.Name())
		configData, err := store.config.Parse(configPath)
		if err != nil {
			syslog.L.Errorf("GetAllPartialFiles: error getting partial file: %v", err)
			continue
		}

		for _, section := range configData.Sections {
			partialFiles = append(partialFiles, PartialFile{
				Path:    section.Properties["path"],
				Comment: section.Properties["comment"],
			})
		}
	}

	return partialFiles, nil
}
