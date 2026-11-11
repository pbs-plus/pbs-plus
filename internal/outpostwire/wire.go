// Package outpostwire defines the RPC contract between the server and agents hosting an outpost listener.
package outpostwire

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
)

const (
	MethodStart = "outpost_start"
	MethodStop  = "outpost_stop"
	MethodReady = "outpost_ready"
	MethodConn  = "outpost_conn"

	Header = "X-PBS-Plus-OutpostID"

	ExitListenFailed = 3
)

type StartReq struct {
	Name       string `cbor:"name"`
	ListenAddr string `cbor:"listen_addr"`
}

type StopReq struct {
	Name string `cbor:"name"`
}

type ConnReq struct {
	Remote string `cbor:"remote"`
}

// StreamID is the agents-manager client id of an outpost child connection.
func StreamID(agent, name string) string { return agent + "|" + name + "|outpost" }

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

func ValidateName(name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid outpost name %q", name)
	}
	return nil
}

func ValidateListenAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid listen port %q", port)
	}
	return nil
}
