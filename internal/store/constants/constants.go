package constants

const (
	ProxyTargetURL      = "https://127.0.0.1:8007"        // The target server URL
	ModifiedFilePath    = "/js/proxmox-backup-gui.js"     // The specific JS file to modify
	CertFile            = "/etc/proxmox-backup/proxy.pem" // Path to generated SSL certificate
	KeyFile             = "/etc/proxmox-backup/proxy.key" // Path to generated private key
	TimerBasePath       = "/lib/systemd/system"
	DbBasePath          = "/var/lib/proxmox-backup"
	AgentMountBasePath  = "/mnt/pbs-plus-mounts"
	LogsBasePath        = "/var/log/proxmox-backup"
	TaskLogsBasePath    = LogsBasePath + "/tasks"
	JobLogsBasePath     = "/var/log/pbs-plus"
	ScriptsBasePath     = "/var/lib/pbs-plus/scripts"
	SecretsKeyPath      = "/var/lib/pbs-plus/.secret.key"
	MountSocketPath     = "/var/run/pbs_agent_mount.sock"
	JobMutateSocketPath = "/var/run/pbs_agent_job_mutate.sock"
	LockSocketPath      = "/var/run/pbs_plus_locker.sock"
	MemcachedSocketPath = "/var/run/pbs_plus_memcached.sock"
	PBSAuthKeyPath      = "/etc/proxmox-backup/authkey.key"
)
