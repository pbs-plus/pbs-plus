//go:build linux

package esxi

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// BackupConfig holds all configuration parameters for ghettoVCB
type BackupConfig struct {
	// Basic configuration
	VMBackupVolume          string
	DiskBackupFormat        string
	VMBackupRotationCount   int
	PowerVMDownBeforeBackup bool
	EnableHardPowerOff      bool
	IterToWaitShutdown      int
	PowerDownTimeout        int
	SnapshotTimeout         int
	EnableCompression       bool
	VMSnapshotMemory        bool
	VMSnapshotQuiesce       bool
	AllowVMsWithSnapshots   bool
	VMDKFilesToBackup       string
	BackupFilesChmod        string

	// NFS configuration
	EnableNonPersistentNFS bool
	UnmountNFS             bool
	NFSServer              string
	NFSVersion             string
	NFSMount               string
	NFSLocalName           string
	NFSVMBackupDir         string

	// Email configuration
	EmailAlert         bool
	EmailLog           bool
	EmailDelayInterval int
	EmailServer        string
	EmailServerPort    int
	EmailUserName      string
	EmailUserPassword  string
	EmailFrom          string
	EmailTo            string
	EmailErrorsTo      string

	// VM ordering
	VMShutdownOrder string
	VMStartupOrder  string

	// Advanced options
	RSyncLink              bool
	WorkdirDebug           bool
	NFSIOHackLoopMax       int
	NFSIOHackSleepTimer    int
	NFSBackupDelay         int
	EnableNFSIOHack        bool
	AdditionalRotationPath string
	VMBackupDirNamingConv  string
}

// SSHConfig holds SSH connection parameters for remote execution
type SSHConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	KeyFile  string
	Timeout  time.Duration
}

// BackupJob represents a single backup operation
type BackupJob struct {
	VMNames    []string
	ExcludeVMs []string
	JobName    string
	BackupAll  bool
	DryRun     bool
	LogLevel   string
	WorkDir    string
	ConfigDir  string
	GlobalConf string
}

// BackupResult contains the results of a backup operation
type BackupResult struct {
	Success      bool
	VMsOK        int
	VMsFailed    int
	VMDKsFailed  int
	Status       string
	ErrorMessage string
	LogOutput    string
	Duration     time.Duration
	StartTime    time.Time
	EndTime      time.Time
}

// VMInfo represents information about a virtual machine
type VMInfo struct {
	ID         string
	Name       string
	VMXPath    string
	VMXDir     string
	VMXConf    string
	Volume     string
	VMDKs      []VMDKInfo
	IndepVMDKs []VMDKInfo
	TotalSize  int64
}

// VMDKInfo represents information about a VMDK file
type VMDKInfo struct {
	Path string
	Size int64
}

// GhettoVCB is the main backup client
type GhettoVCB struct {
	config     *BackupConfig
	sshConfig  *SSHConfig
	sshClient  *ssh.Client
	logger     *Logger
	workDir    string
	vmwareCmd  string
	vmkfstools string
	version    int
	mutex      sync.RWMutex
}

// Logger handles logging with different levels
type Logger struct {
	level    string
	output   io.Writer
	logFile  *os.File
	emailLog *bytes.Buffer
}
