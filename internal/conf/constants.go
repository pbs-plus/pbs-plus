package conf

import "time"

const (
	ProxyTargetURL = "https://127.0.0.1:8007" // The target server URL

	ARPCServerPort   = ":8008"
	AgentAPIPort     = ":8018"
	ServerAPIExtPort = ":8017"

	ModifiedFilePath       = "/js/proxmox-backup-gui.js"     // The specific JS file to modify
	CertFile               = "/etc/proxmox-backup/proxy.pem" // Path to generated SSL certificate
	KeyFile                = "/etc/proxmox-backup/proxy.key" // Path to generated private key
	AgentTLSCACertFile     = "/etc/proxmox-backup/pbs-plus/certs/ca.crt"
	AgentTLSCAKeyFile      = "/etc/proxmox-backup/pbs-plus/certs/ca.key"
	AgentTLSPrevCACertFile = "/etc/proxmox-backup/pbs-plus/certs/ca-prev.crt"
	AgentTLSPrevCAKeyFile  = "/etc/proxmox-backup/pbs-plus/certs/ca-prev.key"
	TLSCARotationGraceDays = 30
	AuthTokenExpiration    = 24 * time.Hour

	HTTPReadTimeout    = 10 * time.Second
	HTTPWriteTimeout   = 5 * time.Minute
	HTTPIdleTimeout    = 5 * time.Minute
	HTTPMaxHeaderBytes = 1 << 20
	HTTPRateLimit      = 100.0
	HTTPRateBurst      = 200

	TimerBasePath        = "/lib/systemd/system"
	DbBasePath           = "/var/lib/proxmox-backup"
	AgentMountBasePath   = "/mnt/pbs-plus-mounts"
	RestoreMountBasePath = "/mnt/pbs-plus-restores"
	LogsBasePath         = "/var/log/proxmox-backup"
	TaskLogsBasePath     = LogsBasePath + "/tasks"
	ActiveLogsPath       = TaskLogsBasePath + "/active"
	ArchivedLogsPath     = TaskLogsBasePath + "/archive"
	PBSAuthKeyPath       = "/etc/proxmox-backup/authkey.key"
)

// These paths are variables so they can be overridden by build tags for agent vs server
var (
	StatePrefix         = "/var/lib/pbs-plus"
	ScriptsBasePath     = StatePrefix + "/scripts"
	SecretsKeyPath      = StatePrefix + "/.secret.key"
	BackupLogsBasePath  = "/var/log/pbs-plus"
	RestoreLogsBasePath = "/var/log/pbs-plus/restores"
	MountSocketPath     = "/var/run/pbs_agent_mount.sock"
	JobMutateSocketPath = "/var/run/pbs_agent_job_mutate.sock"
	RestoreSocketPath   = "/var/run/pbs_plus_restore"
	LockSocketPath      = "/var/run/pbs_plus_locker.sock"
	MemcachedSocketPath = "/var/run/pbs_plus_memcached"
)
