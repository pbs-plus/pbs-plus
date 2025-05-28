package esxi

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

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

	backupDir := filepath.ToSlash(filepath.Join(g.getLocalMountPath(), vm.Name))

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
		snapshotName = fmt.Sprintf("pbs-plus-snapshot-%s", time.Now().Format("2006-01-02"))
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

// parseSnapshotField extracts key and value from a snapshot detail line.
// Example: "--Snapshot Name        : My Snapshot" -> ("Snapshot Name", "My Snapshot", true)
// It handles varying leading hyphens.
func parseSnapshotField(line string) (key string, value string, found bool) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) == 2 {
		rawKey := strings.TrimSpace(parts[0])
		// Remove all leading hyphens
		processedKey := rawKey
		for strings.HasPrefix(processedKey, "-") {
			processedKey = strings.TrimPrefix(processedKey, "-")
		}
		// The actual key name might still have leading/trailing spaces if not part of hyphens
		key = strings.TrimSpace(processedKey)

		value = strings.TrimSpace(parts[1])
		return key, value, true
	}
	return "", "", false
}

// getSnapshotIDByName retrieves the ID of a snapshot given its name for a specific VM.
func (g *GhettoVCB) getSnapshotIDByName(vmID string, targetSnapshotName string) (string, error) {
	cmd := fmt.Sprintf("%s vmsvc/snapshot.get %s", g.vmwareCmd, vmID)
	output, err := g.executeCommand(cmd)
	if err != nil {
		return "", fmt.Errorf(
			"failed to execute snapshot.get for VMID %s: %w",
			vmID,
			err,
		)
	}

	lines := strings.Split(output, "\n")
	var currentParsedName string
	// inPotentialSnapshotBlock is true if we've parsed a "Snapshot Name"
	// and are expecting its related fields, particularly "Snapshot Id".
	var inPotentialSnapshotBlock bool

	for _, line := range lines {
		key, value, ok := parseSnapshotField(line)
		if !ok {
			// Line is not a key-value field (e.g., "Get Snapshot:", "|-ROOT", empty lines)
			continue
		}

		g.logger.Debug(
			fmt.Sprintf("Parsed snapshot field: Key='%s', Value='%s'", key, value),
		)

		if key == "Snapshot Name" {
			currentParsedName = value
			inPotentialSnapshotBlock = true // We've found a name, now look for its ID.
			g.logger.Debug(
				fmt.Sprintf("Found Snapshot Name: '%s'", currentParsedName),
			)
		} else if key == "Snapshot Id" && inPotentialSnapshotBlock {
			// This ID belongs to the 'currentParsedName' we just read.
			g.logger.Debug(fmt.Sprintf(
				"Found Snapshot Id: '%s' for Name: '%s'",
				value,
				currentParsedName,
			))
			if currentParsedName == targetSnapshotName {
				g.logger.Info(fmt.Sprintf(
					"Target snapshot '%s' found with ID '%s' for VMID '%s'",
					targetSnapshotName,
					value,
					vmID,
				))
				return value, nil // Target found
			}
			// Reset: this ID has been processed (matched or not).
			// We are no longer looking for an ID for *this* specific currentParsedName.
			inPotentialSnapshotBlock = false
			currentParsedName = ""
		} else if inPotentialSnapshotBlock {
			// We are in a snapshot block (e.g. reading "Snapshot Desciption", "Snapshot Created On")
			// These fields belong to currentParsedName, but are not the ID itself.
			// We continue, waiting for "Snapshot Id" or a new "Snapshot Name".
			g.logger.Debug(fmt.Sprintf(
				"Other field '%s' for current snapshot name '%s'",
				key,
				currentParsedName,
			))
		}
	}

	g.logger.Info(fmt.Sprintf(
		"Snapshot with name '%s' not found for VMID '%s'",
		targetSnapshotName,
		vmID,
	))
	return "", fmt.Errorf(
		"snapshot named '%s' not found for VMID %s",
		targetSnapshotName,
		vmID,
	)
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

	output, err := g.executeCommand(fmt.Sprintf("%s vmsvc/snapshot.create %s '%s' '%s' %s %s",
		g.vmwareCmd, vm.ID, snapshotName, snapshotName, memory, quiesce))
	if err != nil {
		g.logger.Info(output)
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
	snapshotID, err := g.getSnapshotIDByName(vm.ID, snapshotName)
	if err != nil {
		g.logger.Info(snapshotID)
		return err
	}

	command := fmt.Sprintf("%s vmsvc/snapshot.remove %s %s", g.vmwareCmd, vm.ID, snapshotID)

	// Parse snapshot ID (this is a simplified version)
	output, err := g.executeCommand(command)
	if err != nil {
		g.logger.Info(output)
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
