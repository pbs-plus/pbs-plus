package esxi

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
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

	minorVersion, err := strconv.Atoi(matches[2])
	if err != nil {
		return fmt.Errorf("invalid minor version: %s", matches[2])
	}

	patchVersion, err := strconv.Atoi(matches[3])
	if err != nil {
		return fmt.Errorf("invalid patch version: %s", matches[3])
	}

	g.version = majorVersion
	g.minorVersion = minorVersion
	g.patchVersion = patchVersion
	return nil
}

// createWorkDir creates the working directory
func (g *GhettoVCB) createWorkDir() error {
	if g.workDir == "" {
		g.workDir = fmt.Sprintf("/tmp/pbs-plus-ghetto-vcb.work.%d", os.Getpid())
	}

	_, err := g.executeCommand(fmt.Sprintf("mkdir -p %s", g.workDir))
	return err
}

func (g *GhettoVCB) mountNFS() error {
	g.logger.Info(fmt.Sprintf("Mounting NFS: %s:%s to %s", g.config.NFSServer, g.config.NFSMount, g.getLocalMountPath()))

	command := fmt.Sprintf("%s hostsvc/datastore/nas_create %s %s %s 0 %s", g.vmwareCmd, g.config.NFSLocalName, g.config.NFSVersion, g.config.NFSMount, g.config.NFSServer)

	if g.version < 5 || (g.version == 5 && g.minorVersion == 0) {
		command = fmt.Sprintf("%s hostsvc/datastore/nas_create %s %s %s 0", g.vmwareCmd, g.config.NFSLocalName, g.config.NFSServer, g.config.NFSMount)
	}

	_, err := g.executeCommand(command)
	return err
}

func (g *GhettoVCB) unmountNFS() {
	g.logger.Info("Sleeping for 30 seconds before unmounting NFS volume to let async operations finish")
	time.Sleep(30 * time.Second)

	output, err := g.executeCommand(fmt.Sprintf("%s hostsvc/datastore/destroy %s", g.vmwareCmd, g.config.NFSLocalName))
	if err != nil {
		g.logger.Info(err.Error())
		g.logger.Info(output)
	}

	return
}

func (g *GhettoVCB) getLocalMountPath() string {
	return fmt.Sprintf("/vmfs/volumes/%s", g.config.NFSLocalName)
}

// copyVMXFile copies the VMX file to the backup directory
func (g *GhettoVCB) copyVMXFile(vm *VMInfo, backupDir string) error {
	_, err := g.executeCommand(fmt.Sprintf("cp '%s' '%s/'", vm.VMXPath, backupDir))
	return err
}

// getHostname gets the hostname
func (g *GhettoVCB) getHostname() string {
	output, err := g.executeCommand("hostname -s")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(output)
}
