//go:build linux

package esxi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DefaultConfig returns a configuration with default values
func DefaultConfig() *BackupConfig {
	return &BackupConfig{
		VMBackupVolume:          "/vmfs/volumes/mini-local-datastore-hdd/backups",
		DiskBackupFormat:        "thin",
		VMBackupRotationCount:   3,
		PowerVMDownBeforeBackup: false,
		EnableHardPowerOff:      false,
		IterToWaitShutdown:      3,
		PowerDownTimeout:        5,
		SnapshotTimeout:         15,
		EnableCompression:       false,
		VMSnapshotMemory:        false,
		VMSnapshotQuiesce:       false,
		AllowVMsWithSnapshots:   false,
		VMDKFilesToBackup:       "all",
		EnableNonPersistentNFS:  false,
		UnmountNFS:              false,
		NFSVersion:              "nfs",
		EmailAlert:              false,
		EmailLog:                false,
		EmailDelayInterval:      1,
		EmailServerPort:         25,
		EmailFrom:               "root@ghettoVCB",
		RSyncLink:               false,
		WorkdirDebug:            false,
		NFSIOHackLoopMax:        10,
		NFSIOHackSleepTimer:     60,
		NFSBackupDelay:          0,
		EnableNFSIOHack:         false,
		VMBackupDirNamingConv:   time.Now().Format("2006-01-02_15-04-05"),
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
		workDir:   "/tmp/ghettoVCB.work",
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

// executeCommand executes a command via SSH
func (g *GhettoVCB) executeCommand(cmd string) (string, error) {
	if g.sshClient == nil {
		return "", fmt.Errorf("SSH client not connected")
	}

	session, err := g.sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	output, err := session.CombinedOutput(cmd)
	return string(output), err
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
	return g.performBackup(ctx, vms, job, result)
}

// getVMsToBackup gets the list of VMs to backup based on the job configuration
func (g *GhettoVCB) getVMsToBackup(job *BackupJob) ([]*VMInfo, error) {
	// Get all VMs on the host
	allVMs, err := g.getAllVMs()
	if err != nil {
		return nil, fmt.Errorf("failed to get VM list: %w", err)
	}

	var targetVMs []*VMInfo

	if job.BackupAll {
		targetVMs = allVMs
	} else {
		// Filter VMs based on the provided list
		vmMap := make(map[string]*VMInfo)
		for _, vm := range allVMs {
			vmMap[vm.Name] = vm
		}

		for _, vmName := range job.VMNames {
			if vm, exists := vmMap[vmName]; exists {
				targetVMs = append(targetVMs, vm)
			} else {
				g.logger.Info(fmt.Sprintf("WARNING: VM '%s' not found", vmName))
			}
		}
	}

	// Apply exclusions
	if len(job.ExcludeVMs) > 0 {
		excludeMap := make(map[string]bool)
		for _, vmName := range job.ExcludeVMs {
			excludeMap[vmName] = true
		}

		var filteredVMs []*VMInfo
		for _, vm := range targetVMs {
			if !excludeMap[vm.Name] {
				filteredVMs = append(filteredVMs, vm)
			}
		}
		targetVMs = filteredVMs
	}

	return targetVMs, nil
}

// getAllVMs retrieves information about all VMs on the host
func (g *GhettoVCB) getAllVMs() ([]*VMInfo, error) {
	output, err := g.executeCommand(fmt.Sprintf("%s vmsvc/getallvms", g.vmwareCmd))
	if err != nil {
		return nil, err
	}

	var vms []*VMInfo
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Vmid") {
			continue
		}

		// Parse VM information from the line
		// Format: Vmid Name File Guest OS Version Annotation
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}

		vm := &VMInfo{
			ID:   parts[0],
			Name: parts[1],
		}

		// Extract VMX path (handle spaces in path)
		vmxStart := strings.Index(line, "[")
		vmxEnd := strings.Index(line, "]")
		if vmxStart != -1 && vmxEnd != -1 {
			vmxInfo := line[vmxStart+1 : vmxEnd]
			pathParts := strings.SplitN(vmxInfo, " ", 2)
			if len(pathParts) == 2 {
				vm.Volume = pathParts[0]
				vm.VMXConf = pathParts[1]
				vm.VMXPath = fmt.Sprintf("/vmfs/volumes/%s/%s", vm.Volume, vm.VMXConf)
				vm.VMXDir = filepath.Dir(vm.VMXPath)
			}
		}

		vms = append(vms, vm)
	}

	// Get VMDK information for each VM
	for _, vm := range vms {
		if err := g.getVMDKInfo(vm); err != nil {
			g.logger.Debug(fmt.Sprintf("Failed to get VMDK info for %s: %v", vm.Name, err))
		}
	}

	return vms, nil
}

// getVMDKInfo retrieves VMDK information for a VM
func (g *GhettoVCB) getVMDKInfo(vm *VMInfo) error {
	if vm.VMXPath == "" {
		return fmt.Errorf("VMX path not set")
	}

	// Read VMX file to get VMDK information
	output, err := g.executeCommand(fmt.Sprintf("cat '%s'", vm.VMXPath))
	if err != nil {
		return err
	}

	lines := strings.Split(output, "\n")
	diskMap := make(map[string]map[string]string)

	// Parse VMX file for disk information
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, ".fileName") && strings.Contains(line, ".vmdk") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.Trim(strings.TrimSpace(parts[1]), "\"")

				// Extract disk ID (e.g., "scsi0:0" from "scsi0:0.fileName")
				diskID := strings.Split(key, ".")[0]

				if diskMap[diskID] == nil {
					diskMap[diskID] = make(map[string]string)
				}
				diskMap[diskID]["fileName"] = value
			}
		}
	}

	// Check for present and independent disks
	for _, line := range lines {
		line = strings.TrimSpace(line)
		for diskID := range diskMap {
			if strings.HasPrefix(line, diskID+".present") && strings.Contains(line, "TRUE") {
				diskMap[diskID]["present"] = "true"
			}
			if strings.HasPrefix(line, diskID+".mode") && strings.Contains(line, "independent") {
				diskMap[diskID]["independent"] = "true"
			}
		}
	}

	// Process valid VMDKs
	for _, diskInfo := range diskMap {
		if diskInfo["present"] == "true" && diskInfo["fileName"] != "" {
			vmdkPath := diskInfo["fileName"]

			// Handle absolute vs relative paths
			if !strings.HasPrefix(vmdkPath, "/vmfs/volumes") {
				vmdkPath = filepath.Join(vm.VMXDir, vmdkPath)
			}

			// Get VMDK size
			size, err := g.getVMDKSize(vmdkPath)
			if err != nil {
				g.logger.Debug(fmt.Sprintf("Failed to get size for VMDK %s: %v", vmdkPath, err))
				size = 0
			}

			vmdkInfo := VMDKInfo{
				Path: vmdkPath,
				Size: size,
			}

			if diskInfo["independent"] == "true" {
				vm.IndepVMDKs = append(vm.IndepVMDKs, vmdkInfo)
			} else {
				vm.VMDKs = append(vm.VMDKs, vmdkInfo)
				vm.TotalSize += size
			}
		}
	}

	return nil
}

// getVMDKSize gets the size of a VMDK file
func (g *GhettoVCB) getVMDKSize(vmdkPath string) (int64, error) {
	output, err := g.executeCommand(fmt.Sprintf("cat '%s' | grep 'VMFS' | grep '.vmdk' | awk '{print $2}'", vmdkPath))
	if err != nil {
		return 0, err
	}

	sizeStr := strings.TrimSpace(output)
	if sizeStr == "" {
		return 0, fmt.Errorf("unable to parse VMDK size")
	}

	sectors, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}

	// Convert sectors to bytes (512 bytes per sector)
	return sectors * 512, nil
}

// performDryRun performs a dry run without actually backing up
func (g *GhettoVCB) performDryRun(vms []*VMInfo, result *BackupResult) (*BackupResult, error) {
	g.logger.DryRun("###############################################")
	g.logger.DryRun("DRY RUN MODE - No actual backup will be performed")
	g.logger.DryRun("###############################################")

	for _, vm := range vms {
		g.logger.DryRun(fmt.Sprintf("Virtual Machine: %s", vm.Name))
		g.logger.DryRun(fmt.Sprintf("VM_ID: %s", vm.ID))
		g.logger.DryRun(fmt.Sprintf("VMX_PATH: %s", vm.VMXPath))
		g.logger.DryRun(fmt.Sprintf("VMX_DIR: %s", vm.VMXDir))
		g.logger.DryRun(fmt.Sprintf("VMFS_VOLUME: %s", vm.Volume))
		g.logger.DryRun("VMDK(s):")

		for _, vmdk := range vm.VMDKs {
			sizeGB := vmdk.Size / (1024 * 1024 * 1024)
			g.logger.DryRun(fmt.Sprintf("\t%s\t%d GB", vmdk.Path, sizeGB))
		}

		if len(vm.IndepVMDKs) > 0 {
			g.logger.DryRun("INDEPENDENT VMDK(s):")
			for _, vmdk := range vm.IndepVMDKs {
				sizeGB := vmdk.Size / (1024 * 1024 * 1024)
				g.logger.DryRun(fmt.Sprintf("\t%s\t%d GB", vmdk.Path, sizeGB))
			}
			g.logger.DryRun("Snapshots can not be taken for independent disks!")
			g.logger.DryRun("THIS VIRTUAL MACHINE WILL NOT HAVE ALL ITS VMDKS BACKED UP!")
		}

		totalSizeGB := vm.TotalSize / (1024 * 1024 * 1024)
		g.logger.DryRun(fmt.Sprintf("TOTAL_VM_SIZE_TO_BACKUP: %d GB", totalSizeGB))

		// Check for existing snapshots
		hasSnapshots, err := g.hasExistingSnapshots(vm)
		if err != nil {
			g.logger.DryRun(fmt.Sprintf("Error checking snapshots: %v", err))
		} else if hasSnapshots {
			if !g.config.AllowVMsWithSnapshots {
				g.logger.DryRun("Snapshots found for this VM, please commit all snapshots before continuing!")
				g.logger.DryRun("THIS VIRTUAL MACHINE WILL NOT BE BACKED UP DUE TO EXISTING SNAPSHOTS!")
			} else {
				g.logger.DryRun("Snapshots found for this VM, ALL EXISTING SNAPSHOTS WILL BE CONSOLIDATED PRIOR TO BACKUP!")
			}
		}

		if vm.TotalSize == 0 {
			g.logger.DryRun("THIS VIRTUAL MACHINE WILL NOT BE BACKED UP DUE TO EMPTY VMDK LIST!")
		}

		g.logger.DryRun("###############################################")
	}

	result.Success = true
	result.Status = "OK"
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result, nil
}

// hasExistingSnapshots checks if a VM has existing snapshots
func (g *GhettoVCB) hasExistingSnapshots(vm *VMInfo) (bool, error) {
	// Check for delta files in VM directory
	output, err := g.executeCommand(fmt.Sprintf("ls '%s' | grep '\\-delta\\.vmdk' | wc -l", vm.VMXDir))
	if err != nil {
		return false, err
	}

	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// performBackup performs the actual backup operation
func (g *GhettoVCB) performBackup(ctx context.Context, vms []*VMInfo, job *BackupJob, result *BackupResult) (*BackupResult, error) {
	var vmOK, vmFailed, vmdkFailed int

	// Process VM shutdown order if specified
	if g.config.VMShutdownOrder != "" {
		if err := g.processVMShutdownOrder(); err != nil {
			g.logger.Info(fmt.Sprintf("Error in VM shutdown order: %v", err))
		}
	}

	// Backup each VM
	for _, vm := range vms {
		select {
		case <-ctx.Done():
			result.ErrorMessage = "Backup cancelled"
			result.Status = "CANCELLED"
			return result, ctx.Err()
		default:
		}

		g.logger.Info(fmt.Sprintf("Starting backup for VM: %s", vm.Name))

		success, vmdkErr := g.backupSingleVM(ctx, vm)
		if success {
			vmOK++
			g.logger.Info(fmt.Sprintf("Successfully completed backup for %s", vm.Name))
		} else {
			vmFailed++
			g.logger.Info(fmt.Sprintf("Failed to backup %s", vm.Name))
		}

		if vmdkErr {
			vmdkFailed++
		}

		// Apply NFS backup delay if configured
		if g.config.NFSBackupDelay > 0 {
			time.Sleep(time.Duration(g.config.NFSBackupDelay) * time.Second)
		}
	}

	// Process VM startup order if specified
	if g.config.VMStartupOrder != "" {
		if err := g.processVMStartupOrder(); err != nil {
			g.logger.Info(fmt.Sprintf("Error in VM startup order: %v", err))
		}
	}

	// Determine final status
	result.VMsOK = vmOK
	result.VMsFailed = vmFailed
	result.VMDKsFailed = vmdkFailed
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	if vmOK > 0 && vmFailed == 0 && vmdkFailed == 0 {
		result.Success = true
		result.Status = "OK"
	} else if vmOK > 0 && vmFailed == 0 && vmdkFailed > 0 {
		result.Success = true
		result.Status = "WARNING"
	} else if vmOK > 0 && vmFailed > 0 {
		result.Success = false
		result.Status = "ERROR"
	} else {
		result.Success = false
		result.Status = "ERROR"
	}

	g.logger.Info("============================== ghettoVCB LOG END ==============================")

	return result, nil
}

// backupSingleVM backs up a single virtual machine
func (g *GhettoVCB) backupSingleVM(ctx context.Context, vm *VMInfo) (bool, bool) {
	var vmdkError bool

	// Check for existing snapshots
	if hasSnapshots, _ := g.hasExistingSnapshots(vm); hasSnapshots {
		if !g.config.AllowVMsWithSnapshots {
			g.logger.Info(fmt.Sprintf("Snapshot found for %s, backup will not take place", vm.Name))
			return false, false
		} else {
			g.logger.Info(fmt.Sprintf("Snapshot found for %s, consolidating ALL snapshots now...", vm.Name))
			if err := g.removeAllSnapshots(vm); err != nil {
				g.logger.Info(fmt.Sprintf("Failed to remove snapshots for %s: %v", vm.Name, err))
				return false, false
			}
		}
	}

	// Create backup directory
	backupDir, err := g.createBackupDirectory(vm)
	if err != nil {
		g.logger.Info(fmt.Sprintf("Failed to create backup directory for %s: %v", vm.Name, err))
		return false, false
	}

	// Copy VMX file
	if err := g.copyVMXFile(vm, backupDir); err != nil {
		g.logger.Info(fmt.Sprintf("Failed to copy VMX file for %s: %v", vm.Name, err))
		return false, false
	}

	// Get original power state
	originalPowerState, err := g.getVMPowerState(vm)
	if err != nil {
		g.logger.Info(fmt.Sprintf("Failed to get power state for %s: %v", vm.Name, err))
		return false, false
	}

	// Power down VM if configured
	if g.config.PowerVMDownBeforeBackup {
		if err := g.powerOffVM(vm); err != nil {
			g.logger.Info(fmt.Sprintf("Failed to power off %s: %v", vm.Name, err))
			return false, false
		}
	}

	// Create snapshot for powered-on VMs
	var snapshotName string
	if !g.config.PowerVMDownBeforeBackup && originalPowerState == "Powered on" {
		snapshotName = fmt.Sprintf("ghettoVCB-snapshot-%s", time.Now().Format("2006-01-02"))
		if err := g.createSnapshot(vm, snapshotName); err != nil {
			g.logger.Info(fmt.Sprintf("Failed to create snapshot for %s: %v", vm.Name, err))
			return false, false
		}
	}

	// Backup VMDKs
	for _, vmdk := range vm.VMDKs {
		if err := g.backupVMDK(vm, vmdk, backupDir); err != nil {
			g.logger.Info(fmt.Sprintf("Failed to backup VMDK %s: %v", vmdk.Path, err))
			vmdkError = true
		}
	}

	// Remove snapshot if created
	if snapshotName != "" {
		if err := g.removeSnapshot(vm, snapshotName); err != nil {
			g.logger.Info(fmt.Sprintf("Failed to remove snapshot for %s: %v", vm.Name, err))
		}
	}

	// Power on VM if it was originally powered on and we powered it down
	if g.config.PowerVMDownBeforeBackup && originalPowerState == "Powered on" {
		if err := g.powerOnVM(vm); err != nil {
			g.logger.Info(fmt.Sprintf("Failed to power on %s: %v", vm.Name, err))
		}
	}

	// Compress backup if configured
	if g.config.EnableCompression {
		if err := g.compressBackup(vm, backupDir); err != nil {
			g.logger.Info(fmt.Sprintf("Failed to compress backup for %s: %v", vm.Name, err))
			vmdkError = true
		}
	}

	// Rotate old backups
	if err := g.rotateBackups(vm); err != nil {
		g.logger.Info(fmt.Sprintf("Failed to rotate backups for %s: %v", vm.Name, err))
	}

	return !vmdkError, vmdkError
}

// getVMPowerState gets the current power state of a VM
func (g *GhettoVCB) getVMPowerState(vm *VMInfo) (string, error) {
	output, err := g.executeCommand(fmt.Sprintf("%s vmsvc/power.getstate %s", g.vmwareCmd, vm.ID))
	if err != nil {
		return "", err
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) > 0 {
		return strings.TrimSpace(lines[len(lines)-1]), nil
	}

	return "", fmt.Errorf("unable to determine power state")
}

// powerOffVM powers off a virtual machine
func (g *GhettoVCB) powerOffVM(vm *VMInfo) error {
	g.logger.Info(fmt.Sprintf("Powering off %s", vm.Name))

	// Try graceful shutdown first
	_, err := g.executeCommand(fmt.Sprintf("%s vmsvc/power.shutdown %s", g.vmwareCmd, vm.ID))
	if err != nil {
		return err
	}

	// Wait for VM to power off
	timeout := time.Duration(g.config.PowerDownTimeout) * time.Minute
	start := time.Now()

	for time.Since(start) < timeout {
		state, err := g.getVMPowerState(vm)
		if err != nil {
			return err
		}

		if state == "Powered off" {
			g.logger.Info(fmt.Sprintf("VM %s is powered off", vm.Name))
			return nil
		}

		// Check if we should do hard power off
		if g.config.EnableHardPowerOff && time.Since(start) > time.Duration(g.config.IterToWaitShutdown)*time.Minute {
			g.logger.Info(fmt.Sprintf("Hard power off for %s", vm.Name))
			_, err := g.executeCommand(fmt.Sprintf("%s vmsvc/power.off %s", g.vmwareCmd, vm.ID))
			if err != nil {
				return err
			}
			time.Sleep(60 * time.Second) // Wait for hard power off to take effect
			break
		}

		time.Sleep(60 * time.Second)
	}

	// Final check
	state, err := g.getVMPowerState(vm)
	if err != nil {
		return err
	}

	if state != "Powered off" {
		return fmt.Errorf("unable to power off VM %s within timeout", vm.Name)
	}

	return nil
}

// powerOnVM powers on a virtual machine
func (g *GhettoVCB) powerOnVM(vm *VMInfo) error {
	g.logger.Info(fmt.Sprintf("Powering on %s", vm.Name))

	_, err := g.executeCommand(fmt.Sprintf("%s vmsvc/power.on %s", g.vmwareCmd, vm.ID))
	return err
}

// createSnapshot creates a snapshot of a virtual machine
func (g *GhettoVCB) createSnapshot(vm *VMInfo, snapshotName string) error {
	g.logger.Info(fmt.Sprintf("Creating snapshot '%s' for %s", snapshotName, vm.Name))

	memory := "0"
	if g.config.VMSnapshotMemory {
		memory = "1"
	}

	quiesce := "0"
	if g.config.VMSnapshotQuiesce {
		quiesce = "1"
	}

	_, err := g.executeCommand(fmt.Sprintf("%s vmsvc/snapshot.create %s '%s' '%s' %s %s",
		g.vmwareCmd, vm.ID, snapshotName, snapshotName, memory, quiesce))
	if err != nil {
		return err
	}

	// Wait for snapshot creation to complete
	timeout := time.Duration(g.config.SnapshotTimeout) * time.Minute
	start := time.Now()

	for time.Since(start) < timeout {
		output, err := g.executeCommand(fmt.Sprintf("%s vmsvc/snapshot.get %s", g.vmwareCmd, vm.ID))
		if err != nil {
			return err
		}

		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) > 1 { // More than just the header line means snapshot exists
			g.logger.Info(fmt.Sprintf("Snapshot '%s' created successfully", snapshotName))
			return nil
		}

		time.Sleep(60 * time.Second)
	}

	return fmt.Errorf("snapshot creation timed out for %s", vm.Name)
}

// removeSnapshot removes a snapshot from a virtual machine
func (g *GhettoVCB) removeSnapshot(vm *VMInfo, snapshotName string) error {
	g.logger.Info(fmt.Sprintf("Removing snapshot from %s", vm.Name))

	// Get snapshot ID
	_, err := g.executeCommand(fmt.Sprintf("%s vmsvc/snapshot.get %s", g.vmwareCmd, vm.ID))
	if err != nil {
		return err
	}

	// Parse snapshot ID (this is a simplified version)
	_, err = g.executeCommand(fmt.Sprintf("%s vmsvc/snapshot.remove %s", g.vmwareCmd, vm.ID))
	if err != nil {
		return err
	}

	// Wait for snapshot removal to complete
	for {
		hasSnapshots, err := g.hasExistingSnapshots(vm)
		if err != nil {
			return err
		}

		if !hasSnapshots {
			break
		}

		time.Sleep(5 * time.Second)
	}

	return nil
}

// removeAllSnapshots removes all snapshots from a virtual machine
func (g *GhettoVCB) removeAllSnapshots(vm *VMInfo) error {
	_, err := g.executeCommand(fmt.Sprintf("%s vmsvc/snapshot.removeall %s", g.vmwareCmd, vm.ID))
	return err
}

// backupVMDK backs up a single VMDK file
func (g *GhettoVCB) backupVMDK(vm *VMInfo, vmdk VMDKInfo, backupDir string) error {
	g.logger.Debug(fmt.Sprintf("Backing up VMDK: %s", vmdk.Path))

	// Determine destination path
	var destPath string
	if strings.HasPrefix(vmdk.Path, "/vmfs/volumes") {
		// Handle VMDKs in different datastores
		parts := strings.Split(vmdk.Path, "/")
		if len(parts) >= 4 {
			dsUUID := parts[3]
			vmdkName := filepath.Base(vmdk.Path)
			destDir := filepath.Join(backupDir, dsUUID)

			_, err := g.executeCommand(fmt.Sprintf("mkdir -p '%s'", destDir))
			if err != nil {
				return err
			}

			destPath = filepath.Join(destDir, vmdkName)
		} else {
			destPath = filepath.Join(backupDir, filepath.Base(vmdk.Path))
		}
	} else {
		destPath = filepath.Join(backupDir, filepath.Base(vmdk.Path))
	}

	// Check if it's a physical RDM
	output, err := g.executeCommand(fmt.Sprintf("grep 'vmfsPassthroughRawDeviceMap' '%s'", vmdk.Path))
	if err == nil && strings.TrimSpace(output) != "" {
		g.logger.Info(fmt.Sprintf("WARNING: Physical RDM '%s' found for %s, which will not be backed up", vmdk.Path, vm.Name))
		return fmt.Errorf("physical RDM cannot be backed up")
	}

	// Build vmkfstools command
	var formatOption string
	switch g.config.DiskBackupFormat {
	case "zeroedthick":
		if g.version >= 4 {
			formatOption = "-d zeroedthick"
		}
	case "2gbsparse":
		formatOption = "-d 2gbsparse"
	case "thin":
		formatOption = "-d thin"
	case "eagerzeroedthick":
		if g.version >= 4 {
			formatOption = "-d eagerzeroedthick"
		}
	default:
		return fmt.Errorf("unknown disk backup format: %s", g.config.DiskBackupFormat)
	}

	// Get adapter type
	adapterOutput, err := g.executeCommand(fmt.Sprintf("grep -i 'ddb.adapterType' '%s' | awk -F '=' '{print $2}' | sed -e 's/^[[:blank:]]*//;s/[[:blank:]]*$//;s/\"//g'", vmdk.Path))
	var adapterFormat string
	if err == nil && strings.TrimSpace(adapterOutput) != "" {
		adapterFormat = fmt.Sprintf("-a %s", strings.TrimSpace(adapterOutput))
	}

	// Execute vmkfstools
	cmd := fmt.Sprintf("%s -i '%s' %s %s '%s'", g.vmkfstools, vmdk.Path, adapterFormat, formatOption, destPath)
	g.logger.Debug(fmt.Sprintf("Executing: %s", cmd))

	_, err = g.executeCommand(cmd)
	if err != nil {
		return fmt.Errorf("vmkfstools failed: %w", err)
	}

	return nil
}

// compressBackup compresses the backup directory
func (g *GhettoVCB) compressBackup(vm *VMInfo, backupDir string) error {
	g.logger.Info(fmt.Sprintf("Compressing backup for %s", vm.Name))

	baseDir := filepath.Dir(backupDir)
	backupDirName := filepath.Base(backupDir)
	compressedFile := fmt.Sprintf("%s.gz", backupDir)

	// Use tar to compress
	cmd := fmt.Sprintf("cd '%s' && tar -czf '%s' '%s'", baseDir, compressedFile, backupDirName)
	_, err := g.executeCommand(cmd)
	if err != nil {
		return err
	}

	// Remove original directory
	_, err = g.executeCommand(fmt.Sprintf("rm -rf '%s'", backupDir))
	return err
}

// rotateBackups rotates old backups according to the retention policy
func (g *GhettoVCB) rotateBackups(vm *VMInfo) error {
	backupBaseDir := filepath.Join(g.config.VMBackupVolume, vm.Name)

	// List existing backups
	pattern := fmt.Sprintf("%s-*", vm.Name)
	output, err := g.executeCommand(fmt.Sprintf("ls -t '%s' | grep '%s'", backupBaseDir, pattern))
	if err != nil {
		return nil // No existing backups
	}

	backups := strings.Split(strings.TrimSpace(output), "\n")
	if len(backups) <= g.config.VMBackupRotationCount {
		return nil // No rotation needed
	}

	// Remove old backups
	for i := g.config.VMBackupRotationCount; i < len(backups); i++ {
		backupPath := filepath.Join(backupBaseDir, strings.TrimSpace(backups[i]))
		g.logger.Info(fmt.Sprintf("Removing old backup: %s", backupPath))

		_, err := g.executeCommand(fmt.Sprintf("rm -rf '%s'", backupPath))
		if err != nil {
			g.logger.Info(fmt.Sprintf("Failed to remove old backup %s: %v", backupPath, err))
		}
	}

	return nil
}

// processVMShutdownOrder processes the VM shutdown order
func (g *GhettoVCB) processVMShutdownOrder() error {
	if g.config.VMShutdownOrder == "" {
		return nil
	}

	vmNames := strings.Split(g.config.VMShutdownOrder, ",")
	for _, vmName := range vmNames {
		vmName = strings.TrimSpace(vmName)
		if vmName == "" {
			continue
		}

		// Find VM by name
		vms, err := g.getAllVMs()
		if err != nil {
			return err
		}

		for _, vm := range vms {
			if vm.Name == vmName {
				if err := g.powerOffVM(vm); err != nil {
					g.logger.Info(fmt.Sprintf("Failed to power off %s: %v", vmName, err))
				}
				break
			}
		}
	}

	return nil
}

// processVMStartupOrder processes the VM startup order
func (g *GhettoVCB) processVMStartupOrder() error {
	if g.config.VMStartupOrder == "" {
		return nil
	}

	vmNames := strings.Split(g.config.VMStartupOrder, ",")
	for _, vmName := range vmNames {
		vmName = strings.TrimSpace(vmName)
		if vmName == "" {
			continue
		}

		// Find VM by name
		vms, err := g.getAllVMs()
		if err != nil {
			return err
		}

		for _, vm := range vms {
			if vm.Name == vmName {
				if err := g.powerOnVM(vm); err != nil {
					g.logger.Info(fmt.Sprintf("Failed to power on %s: %v", vmName, err))
				}
				break
			}
		}
	}

	return nil
}

// getHostname gets the hostname
func (g *GhettoVCB) getHostname() string {
	output, err := g.executeCommand("hostname -s")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(output)
}

// Close closes the GhettoVCB instance and cleans up resources
func (g *GhettoVCB) Close() error {
	// Clean up work directory
	if g.workDir != "" && !g.config.WorkdirDebug {
		g.executeCommand(fmt.Sprintf("rm -rf %s", g.workDir))
	}

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
