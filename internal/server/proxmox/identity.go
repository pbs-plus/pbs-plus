//go:build linux

package proxmox

// AuthUser is the PBS Plus service account. The corresponding API token is
// materialized by the cli package (proxmox-backup-manager generate-token).
const (
	// AuthUser is the PBS Plus service account (plus-user@pbs).
	AuthUser = "plus-user@pbs"
	// AuthToken is the API token name within that account.
	AuthToken = "server"
	// AuthID is the fully-qualified API token identifier used to author
	// pbs-plus-generated tasks (UPIDs) and to scope backup ownership.
	AuthID = AuthUser + "!" + AuthToken
)
