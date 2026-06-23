//go:build linux

package proxmox

// PBS Plus service-account identity used to author pbs-plus-generated tasks
// (UPIDs) and to scope ownership of backups. The corresponding API token is
// materialized by the pbscli package (proxmox-backup-manager generate-token).
const (
	AUTH_USER  = "plus-user@pbs"
	AUTH_TOKEN = "server"
	AUTH_ID    = AUTH_USER + "!" + AUTH_TOKEN
)
