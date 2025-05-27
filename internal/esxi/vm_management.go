package esxi

import (
	"context"
	"fmt"
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
