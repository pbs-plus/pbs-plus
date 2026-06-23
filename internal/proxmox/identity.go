//go:build linux

package proxmox

// AuthUser is the PBS Plus service account.
const (
	AuthUser  = "plus-user@pbs"
	AuthToken = "server"
	AuthID    = AuthUser + "!" + AuthToken
)
