package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/esxi"
	"github.com/spf13/cobra"
)

var (
	// SSH configuration flags
	sshHost          string
	sshPort          int
	sshUsername      string
	sshPassword      string
	sshKeyFile       string
	sshKeyPassphrase string
	sshTimeout       time.Duration

	// Backup configuration flags
	diskBackupFormat        string
	powerVMDownBeforeBackup bool
	enableHardPowerOff      bool
	iterToWaitShutdown      int
	powerDownTimeout        int
	snapshotTimeout         int
	vmSnapshotMemory        bool
	vmSnapshotQuiesce       bool
	allowVMsWithSnapshots   bool
	vmdkFilesToBackup       string
	backupFilesChmod        string
	nfsServer               string
	nfsVersion              string
	nfsMount                string
	nfsLocalName            string
	nfsVMBackupDir          string
	vmShutdownOrder         string
	vmStartupOrder          string
	rsyncLink               bool
	workdirDebug            bool
	nfsioHackLoopMax        int
	nfsioHackSleepTimer     int
	nfsBackupDelay          int
	enableNFSIOHack         bool

	// Job configuration flags
	vmNames    []string
	excludeVMs []string
	backupAll  bool
	dryRun     bool
	logLevel   string
	workDir    string
	configDir  string
)

var rootCmd = &cobra.Command{
	Use:   "esxi-nfs-backup",
	Short: "A CLI tool to backup ESXi VMs to NFS using ghettoVCB logic",
	Long:  `esxi-nfs-backup provides a command-line interface to perform backups of VMware ESXi virtual machines to an NFS share, leveraging the principles of ghettoVCB.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Default behavior: show help
		cmd.Help()
	},
}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Perform a backup of specified or all VMs",
	Run: func(cmd *cobra.Command, args []string) {
		sshConfig := &esxi.SSHConfig{
			Host:          sshHost,
			Port:          sshPort,
			Username:      sshUsername,
			Password:      sshPassword,
			KeyFile:       sshKeyFile,
			KeyPassphrase: sshKeyPassphrase,
			Timeout:       sshTimeout,
		}

		backupConfig := esxi.DefaultConfig()
		// Override default config with flag values
		backupConfig.DiskBackupFormat = diskBackupFormat
		backupConfig.PowerVMDownBeforeBackup = powerVMDownBeforeBackup
		backupConfig.EnableHardPowerOff = enableHardPowerOff
		backupConfig.IterToWaitShutdown = iterToWaitShutdown
		backupConfig.PowerDownTimeout = powerDownTimeout
		backupConfig.SnapshotTimeout = snapshotTimeout
		backupConfig.VMSnapshotMemory = vmSnapshotMemory
		backupConfig.VMSnapshotQuiesce = vmSnapshotQuiesce
		backupConfig.AllowVMsWithSnapshots = allowVMsWithSnapshots
		backupConfig.VMDKFilesToBackup = vmdkFilesToBackup
		backupConfig.BackupFilesChmod = backupFilesChmod
		backupConfig.NFSServer = nfsServer
		backupConfig.NFSVersion = nfsVersion
		backupConfig.NFSMount = nfsMount
		backupConfig.NFSLocalName = nfsLocalName
		backupConfig.NFSVMBackupDir = nfsVMBackupDir
		backupConfig.VMShutdownOrder = vmShutdownOrder
		backupConfig.VMStartupOrder = vmStartupOrder
		backupConfig.RSyncLink = rsyncLink
		backupConfig.WorkdirDebug = workdirDebug
		backupConfig.NFSIOHackLoopMax = nfsioHackLoopMax
		backupConfig.NFSIOHackSleepTimer = nfsioHackSleepTimer
		backupConfig.NFSBackupDelay = nfsBackupDelay
		backupConfig.EnableNFSIOHack = enableNFSIOHack

		gvc, err := esxi.NewGhettoVCB(backupConfig, sshConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing GhettoVCB: %v\n", err)
			os.Exit(1)
		}
		defer gvc.Close()

		job := &esxi.BackupJob{
			VMNames:    vmNames,
			ExcludeVMs: excludeVMs,
			BackupAll:  backupAll,
			DryRun:     dryRun,
			LogLevel:   logLevel,
			WorkDir:    workDir,   // Note: The provided code seems to manage its own workDir internally based on a hardcoded path and the config.WorkdirDebug flag. You might need to adjust this part depending on how you want the user-provided WorkDir to interact.
			ConfigDir:  configDir, // Note: The provided code doesn't seem to use a ConfigDir in BackupJob or GhettoVCB. You might need to add functionality for this if you intend to use it.
		}

		ctx := context.Background() // Use a context for cancellation/timeout if needed
		result, err := gvc.BackupVMs(ctx, job)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Backup failed: %v\n", err)
			// You can also print result details here if the error is a controlled one within the BackupVMs function
			os.Exit(1)
		}

		fmt.Println("Backup operation completed.")
		fmt.Printf("Status: %s\n", result.Status)
		if result.ErrorMessage != "" {
			fmt.Printf("Error Message: %s\n", result.ErrorMessage)
		}
		fmt.Printf("VMs OK: %d\n", result.VMsOK)
		fmt.Printf("VMs Failed: %d\n", result.VMsFailed)
		fmt.Printf("VMDKs Failed: %d\n", result.VMDKsFailed)
		fmt.Printf("Duration: %s\n", result.Duration)
		// You might want to print the LogOutput as well, depending on its size and content

		if !result.Success {
			os.Exit(1)
		}
	},
}

func init() {
	// Add the backup command as a subcommand of the root command
	rootCmd.AddCommand(backupCmd)

	// Define SSH flags
	backupCmd.PersistentFlags().StringVarP(&sshHost, "ssh-host", "", "", "ESXi host IP address or hostname")
	backupCmd.PersistentFlags().IntVarP(&sshPort, "ssh-port", "p", 22, "ESXi SSH port")
	backupCmd.PersistentFlags().StringVarP(&sshUsername, "ssh-username", "u", "root", "ESXi SSH username")
	backupCmd.PersistentFlags().StringVarP(&sshPassword, "ssh-password", "w", "", "ESXi SSH password")
	backupCmd.PersistentFlags().StringVarP(&sshKeyFile, "ssh-key-file", "i", "", "ESXi SSH private key file")
	backupCmd.PersistentFlags().StringVarP(&sshKeyPassphrase, "ssh-key-passphrase", "", "", "ESXi SSH private key passphrase")
	backupCmd.PersistentFlags().DurationVar(&sshTimeout, "ssh-timeout", 30*time.Second, "ESXi SSH connection timeout")

	// Define Backup Config flags (using long flags generally for clarity)
	backupCmd.PersistentFlags().StringVar(&diskBackupFormat, "disk-format", "thin", "Disk backup format (thin, zeroedthick, eagerzeroedthick)")
	backupCmd.PersistentFlags().BoolVar(&powerVMDownBeforeBackup, "power-down-before", false, "Power down VM before backup")
	backupCmd.PersistentFlags().BoolVar(&enableHardPowerOff, "hard-power-off", false, "Enable hard power off if graceful shutdown fails")
	backupCmd.PersistentFlags().IntVar(&iterToWaitShutdown, "wait-shutdown-iterations", 3, "Iterations to wait for VM shutdown")
	backupCmd.PersistentFlags().IntVar(&powerDownTimeout, "power-down-timeout", 5, "Timeout in minutes for VM power down")
	backupCmd.PersistentFlags().IntVar(&snapshotTimeout, "snapshot-timeout", 15, "Timeout in minutes for snapshot creation/removal")
	backupCmd.PersistentFlags().BoolVar(&vmSnapshotMemory, "snapshot-memory", false, "Include VM memory in snapshot")
	backupCmd.PersistentFlags().BoolVar(&vmSnapshotQuiesce, "snapshot-quiesce", false, "Quiesce guest file system (requires VMware Tools)")
	backupCmd.PersistentFlags().BoolVar(&allowVMsWithSnapshots, "allow-existing-snapshots", false, "Allow backing up VMs that already have snapshots")
	backupCmd.PersistentFlags().StringVar(&vmdkFilesToBackup, "vmdk-files", "all", "VMDK files to backup (all or specific comma-separated file names)")
	backupCmd.PersistentFlags().StringVar(&backupFilesChmod, "chmod", "", "Chmod permissions for backup files (e.g., 777)")

	// Define NFS Config flags
	backupCmd.PersistentFlags().StringVar(&nfsServer, "nfs-server", "", "NFS server address or hostname")
	backupCmd.PersistentFlags().StringVar(&nfsVersion, "nfs-version", "nfsv41", "NFS version (nfsv3, nfsv41)")
	backupCmd.PersistentFlags().StringVar(&nfsMount, "nfs-mount", "", "NFS mount path on the server (e.g., /mnt/backups)")
	backupCmd.PersistentFlags().StringVar(&nfsLocalName, "nfs-local-name", "", "Local name for the NFS mount on ESXi (e.g., datastore_backup)")
	backupCmd.PersistentFlags().StringVar(&nfsVMBackupDir, "nfs-vm-backup-dir", "", "Directory on the NFS share for VM backups (e.g., my_vm_backups)")

	// Define VM Ordering flags
	backupCmd.PersistentFlags().StringVar(&vmShutdownOrder, "vm-shutdown-order", "", "Comma-separated list of VM names for shutdown order")
	backupCmd.PersistentFlags().StringVar(&vmStartupOrder, "vm-startup-order", "", "Comma-separated list of VM names for startup order")

	// Define Advanced Options flags
	backupCmd.PersistentFlags().BoolVar(&rsyncLink, "rsync-link", false, "Enable rsync --link-dest for incremental backups (requires rsync on ESXi)")
	backupCmd.PersistentFlags().BoolVar(&workdirDebug, "workdir-debug", false, "Do not clean up the work directory after backup")
	backupCmd.PersistentFlags().IntVar(&nfsioHackLoopMax, "nfsio-hack-loops", 10, "Maximum loops for NFS I/O hack")
	backupCmd.PersistentFlags().IntVar(&nfsioHackSleepTimer, "nfsio-hack-sleep", 60, "Sleep timer in seconds for NFS I/O hack")
	backupCmd.PersistentFlags().IntVar(&nfsBackupDelay, "nfs-backup-delay", 0, "Delay in seconds before starting NFS backup")
	backupCmd.PersistentFlags().BoolVar(&enableNFSIOHack, "enable-nfsio-hack", false, "Enable NFS I/O hack")

	// Define Job flags
	backupCmd.Flags().StringSliceVarP(&vmNames, "vm", "v", []string{}, "Name(s) of the VM(s) to backup (comma-separated)")
	backupCmd.Flags().StringSliceVarP(&excludeVMs, "exclude-vm", "e", []string{}, "Name(s) of the VM(s) to exclude from backup (comma-separated)")
	backupCmd.Flags().BoolVarP(&backupAll, "all", "a", false, "Backup all VMs")
	backupCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Perform a dry run without actually backing up")
	backupCmd.Flags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	backupCmd.Flags().StringVar(&workDir, "work-dir", "", "Specify a work directory on ESXi (overrides default)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// Helper function to split comma-separated strings (if needed for other flags)
func splitCommaSeparated(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}
