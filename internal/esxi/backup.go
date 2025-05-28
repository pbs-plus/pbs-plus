package esxi

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// performBackup performs the actual backup operation
func (g *GhettoVCB) performBackup(ctx context.Context, vms []*VMInfo, result *BackupResult) (*BackupResult, error) {
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

	return result, nil
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
			destPath = filepath.Join(destDir, vmdkName)
		} else {
			destPath = filepath.Join(backupDir, filepath.Base(vmdk.Path))
		}
	} else {
		destPath = filepath.Join(backupDir, filepath.Base(vmdk.Path))
	}

	destPath = filepath.ToSlash(destPath)

	mkdirCmd := fmt.Sprintf("mkdir -p '%s'", filepath.ToSlash(filepath.Dir(destPath)))
	output, err := g.executeCommand(mkdirCmd)
	if err != nil {
		return err
	}

	// Check if it's a physical RDM
	output, err = g.executeCommand(fmt.Sprintf("grep 'vmfsPassthroughRawDeviceMap' '%s'", vmdk.Path))
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
	g.logger.Info(fmt.Sprintf("Executing: %s", cmd))

	outputChan, getCmdErr, err := g.executeCommandStream(cmd)
	if err != nil {
		return fmt.Errorf("vmkfstools failed: %w", err)
	}

	for line := range outputChan {
		g.logger.Info(fmt.Sprintf("vmkfstools > %s", line))
	}

	// After the channel is closed, get the final command error
	cmdErr := getCmdErr()
	if cmdErr != nil {
		if exitErr, ok := cmdErr.(*ssh.ExitError); ok {
			g.logger.Info(fmt.Sprintf(
				"Command finished with error. Exit status: %d. Error: %v",
				exitErr.ExitStatus(),
				cmdErr,
			))
		} else {
			g.logger.Info(fmt.Sprintf("Command finished with error: %v", cmdErr))
		}
	}

	return nil
}
