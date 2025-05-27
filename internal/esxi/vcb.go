package esxi

import (
	"context"
	"fmt"
	"os"
	"time"
)

// DefaultConfig returns a configuration with default values
func DefaultConfig() *BackupConfig {
	return &BackupConfig{
		DiskBackupFormat:        "thin",
		PowerVMDownBeforeBackup: false,
		EnableHardPowerOff:      false,
		IterToWaitShutdown:      3,
		PowerDownTimeout:        5,
		SnapshotTimeout:         15,
		VMSnapshotMemory:        false,
		VMSnapshotQuiesce:       false,
		AllowVMsWithSnapshots:   false,
		VMDKFilesToBackup:       "all",
		NFSVersion:              "nfs",
		RSyncLink:               false,
		WorkdirDebug:            false,
		NFSIOHackLoopMax:        10,
		NFSIOHackSleepTimer:     60,
		NFSBackupDelay:          0,
		EnableNFSIOHack:         false,
	}
}

// NewGhettoVCB creates a new GhettoVCB instance
func NewGhettoVCB(config *BackupConfig, sshConfig *SSHConfig) (*GhettoVCB, error) {
	if config == nil {
		config = DefaultConfig()
	}

	g := &GhettoVCB{
		config:    config,
		sshConfig: sshConfig,
		logger:    NewLogger("info", os.Stdout),
		workDir:   "/tmp/pbs-ghettoVCB.work",
	}

	if err := g.connectSSH(); err != nil {
		return nil, fmt.Errorf("failed to connect via SSH: %w", err)
	}

	if err := g.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize: %w", err)
	}

	return g, nil
}

// initialize sets up the GhettoVCB environment
func (g *GhettoVCB) initialize() error {
	// Detect VMware commands
	if err := g.detectVMwareCommands(); err != nil {
		return err
	}

	// Detect ESX version
	if err := g.detectESXVersion(); err != nil {
		return err
	}

	// Create work directory
	if err := g.createWorkDir(); err != nil {
		return err
	}

	return nil
}

// BackupVMs performs backup operations on the specified VMs
func (g *GhettoVCB) BackupVMs(ctx context.Context, job *BackupJob) (*BackupResult, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	startTime := time.Now()
	result := &BackupResult{
		StartTime: startTime,
		Status:    "RUNNING",
	}

	g.logger.Info("============================== ghettoVCB LOG START ==============================")

	// Set up logging
	if job.LogLevel != "" {
		g.logger.level = job.LogLevel
	}

	// Get list of VMs to backup
	vms, err := g.getVMsToBackup(job)
	if err != nil {
		result.Success = false
		result.ErrorMessage = err.Error()
		result.Status = "ERROR"
		return result, err
	}

	if job.DryRun {
		return g.performDryRun(vms, result)
	}

	// Perform actual backup
	return g.performBackup(ctx, vms, result)
}

// Close closes the GhettoVCB instance and cleans up resources
func (g *GhettoVCB) Close() error {
	// Clean up work directory
	if g.workDir != "" && !g.config.WorkdirDebug {
		g.executeCommand(fmt.Sprintf("rm -rf %s", g.workDir))
	}

	_ = g.unmountNFS()

	// Close SSH connection
	if g.sshClient != nil {
		return g.sshClient.Close()
	}

	// Close log file
	if g.logger.logFile != nil {
		return g.logger.logFile.Close()
	}

	return nil
}
