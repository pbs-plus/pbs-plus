package constants

const (
	ProxyTargetURL     = "https://127.0.0.1:8007"        // The target server URL
	ModifiedFilePath   = "/js/proxmox-backup-gui.js"     // The specific JS file to modify
	CertFile           = "/etc/proxmox-backup/proxy.pem" // Path to generated SSL certificate
	KeyFile            = "/etc/proxmox-backup/proxy.key" // Path to generated private key
	TimerBasePath      = "/lib/systemd/system"
	DbBasePath         = "/var/lib/proxmox-backup"
	AgentMountBasePath = "/mnt/pbs-plus-mounts"
	LogsBasePath       = "/var/log/proxmox-backup"
	TaskLogsBasePath   = LogsBasePath + "/tasks"
	JobLogsBasePath    = "/var/log/pbs-plus"
	PBSCert            = "/etc/proxmox-backup/proxy.pem"
	PBSKey             = "/etc/proxmox-backup/proxy.key"
)
