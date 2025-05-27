//go:build linux

package esxi

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

// detectVMwareCommands finds the correct VMware command paths
func (g *GhettoVCB) detectVMwareCommands() error {
	// Try different possible paths for VMware commands
	vmwarePaths := []string{
		"/usr/bin/vmware-vim-cmd",
		"/bin/vim-cmd",
	}

	vmkfstoolsPaths := []string{
		"/usr/sbin/vmkfstools",
		"/sbin/vmkfstools",
	}

	var vmwareCmd, vmkfstools string

	for _, path := range vmwarePaths {
		if g.commandExists(path) {
			vmwareCmd = path
			break
		}
	}

	for _, path := range vmkfstoolsPaths {
		if g.commandExists(path) {
			vmkfstools = path
			break
		}
	}

	if vmwareCmd == "" {
		return fmt.Errorf("unable to locate VMware vim-cmd")
	}

	if vmkfstools == "" {
		return fmt.Errorf("unable to locate vmkfstools")
	}

	g.vmwareCmd = vmwareCmd
	g.vmkfstools = vmkfstools

	return nil
}

// commandExists checks if a command exists
func (g *GhettoVCB) commandExists(cmd string) bool {
	_, err := g.executeCommand(fmt.Sprintf("test -f %s", cmd))
	return err == nil
}

// detectESXVersion detects the ESX version
func (g *GhettoVCB) detectESXVersion() error {
	output, err := g.executeCommand("vmware -v")
	if err != nil {
		return fmt.Errorf("failed to detect ESX version: %w", err)
	}

	// Parse version from output like "VMware ESXi 7.0.0 build-12345"
	re := regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(output)
	if len(matches) < 2 {
		return fmt.Errorf("unable to parse ESX version from: %s", output)
	}

	majorVersion, err := strconv.Atoi(matches[1])
	if err != nil {
		return fmt.Errorf("invalid major version: %s", matches[1])
	}

	g.version = majorVersion
	return nil
}

// createWorkDir creates the working directory
func (g *GhettoVCB) createWorkDir() error {
	if g.workDir == "" {
		g.workDir = fmt.Sprintf("/tmp/ghettoVCB.work.%d", os.Getpid())
	}

	_, err := g.executeCommand(fmt.Sprintf("mkdir -p %s", g.workDir))
	return err
}

// createBackupDirectory creates the backup directory for a VM
func (g *GhettoVCB) createBackupDirectory(vm *VMInfo) (string, error) {
	baseDir := filepath.Join(g.config.VMBackupVolume, vm.Name)
	backupDir := filepath.Join(baseDir, fmt.Sprintf("%s-%s", vm.Name, g.config.VMBackupDirNamingConv))

	_, err := g.executeCommand(fmt.Sprintf("mkdir -p '%s'", backupDir))
	if err != nil {
		return "", err
	}

	return backupDir, nil
}

// copyVMXFile copies the VMX file to the backup directory
func (g *GhettoVCB) copyVMXFile(vm *VMInfo, backupDir string) error {
	_, err := g.executeCommand(fmt.Sprintf("cp '%s' '%s/'", vm.VMXPath, backupDir))
	return err
}
