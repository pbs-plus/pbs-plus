package esxi

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

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

func (g *GhettoVCB) getAllVMs() ([]*VMInfo, error) {
	output, err := g.executeCommand(fmt.Sprintf("%s vmsvc/getallvms", g.vmwareCmd))
	if err != nil {
		return nil, err
	}

	var vms []*VMInfo
	lines := strings.Split(output, "\n")

	// Regex:
	// Group 1: Vmid
	// Group 2: Name
	// Group 3: Datastore (without brackets)
	// Group 4: VMX Path relative to datastore
	// Group 5: Rest of the line (Guest OS, Version, Annotation) - Optional
	vmLineRegex := regexp.MustCompile(`^(\d+)\s+(.+?)\s+\[([^\]]+)\]\s+(.+?\.vmx)\s+(.*)$`)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Vmid") {
			continue
		}

		g.logger.Debug(fmt.Sprintf("Processing line: '%s'", line))
		matches := vmLineRegex.FindStringSubmatch(line)

		if matches == nil {
			g.logger.Debug("Regex did not match the line.")
			continue
		}

		// Log all captured groups
		// g.logger.Debug(fmt.Sprintf("Full match (matches[0]): '%s'", matches[0]))
		// for k, v := range matches {
		// 	g.logger.Debug(fmt.Sprintf("Match group %d: '%s'", k, v))
		// }
		// More concise logging for relevant groups:
		if len(matches) > 0 {
			g.logger.Debug(fmt.Sprintf("Full match: '%s'", matches[0]))
		}
		if len(matches) > 1 {
			g.logger.Debug(fmt.Sprintf("Group 1 (ID): '%s'", matches[1]))
		}
		if len(matches) > 2 {
			g.logger.Debug(fmt.Sprintf("Group 2 (Name): '%s'", matches[2]))
		}
		if len(matches) > 3 {
			g.logger.Debug(fmt.Sprintf("Group 3 (Datastore): '%s'", matches[3]))
		}
		if len(matches) > 4 {
			g.logger.Debug(fmt.Sprintf("Group 4 (VMXPathRaw): '%s'", matches[4]))
		}
		if len(matches) > 5 {
			g.logger.Debug(fmt.Sprintf("Group 5 (Rest): '%s'", matches[5]))
		}

		// We need at least up to Group 5 (GuestOS) for our core fields.
		// So, len(matches) should be at least 1 (full match) + 5 groups = 6.
		if len(matches) < 6 {
			g.logger.Debug(fmt.Sprintf("Skipping line due to insufficient match groups (expected at least 6, got %d)", len(matches)))
			continue
		}

		vm := &VMInfo{
			ID:     matches[1],
			Name:   strings.TrimSpace(matches[2]),
			Volume: matches[3], // Datastore name from Group 3
		}
		g.logger.Debug(fmt.Sprintf("Assigned Volume (from matches[3]): '%s'", vm.Volume))

		// VMXConf is from Group 4, trimmed
		vm.VMXConf = strings.TrimSpace(matches[4])
		g.logger.Debug(fmt.Sprintf("Assigned VMXConf (from trimmed matches[4]): '%s'", vm.VMXConf))

		if vm.Volume != "" && vm.VMXConf != "" {
			vm.VMXPath = fmt.Sprintf("/vmfs/volumes/%s/%s", vm.Volume, vm.VMXConf)
			vm.VMXDir = filepath.ToSlash(filepath.Dir(vm.VMXPath))
			g.logger.Debug(fmt.Sprintf("Derived VMXPath: '%s'", vm.VMXPath))
			g.logger.Debug(fmt.Sprintf("Derived VMXDir: '%s'", vm.VMXDir))
		} else {
			g.logger.Debug("Volume or VMXConf is empty, skipping VMXPath/VMXDir derivation.")
		}

		// For debugging the final struct state before appending
		// g.logger.Debug(fmt.Sprintf("VM struct state before append: %+v", vm))

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
				vmdkPath = filepath.ToSlash(filepath.Join(vm.VMXDir, vmdkPath))
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
