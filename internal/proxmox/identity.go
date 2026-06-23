//go:build linux

package proxmox

const (
	AuthUser  = "plus-user@pbs"
	AuthToken = "server"
	AuthID    = AuthUser + "!" + AuthToken
)
