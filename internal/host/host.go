// Package host resolves and validates the agent hostname.
// It is a pure leaf package (stdlib only) so that low-level consumers such as
// syslog and mtls can depend on it without importing the agent package.
package host

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// AgentHostname returns the hostname from the PBS_PLUS_HOSTNAME environment
// variable, falling back to os.Hostname() when it is unset. The returned host
// is validated when sourced from the environment.
func AgentHostname() (string, error) {
	h := os.Getenv("PBS_PLUS_HOSTNAME")
	if h == "" {
		return os.Hostname()
	}
	if err := ValidateHostname(h); err != nil {
		return "", err
	}
	return h, nil
}

// ValidateHostname checks that host is a syntactically valid hostname or IP
// address: non-empty, at most 253 characters, with dot-separated labels of at
// most 63 characters drawn from letters, digits, and hyphens.
func ValidateHostname(host string) error {
	if host == "" {
		return fmt.Errorf("hostname cannot be empty")
	}
	if len(host) > 253 {
		return fmt.Errorf("hostname too long (%d chars)", len(host))
	}
	if ip := net.ParseIP(host); ip != nil {
		return nil
	}
	for part := range strings.SplitSeq(host, ".") {
		if part == "" {
			return fmt.Errorf("hostname segment cannot be empty")
		}
		if len(part) > 63 {
			return fmt.Errorf("hostname segment too long: %s", part)
		}
		for _, c := range part {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
				return fmt.Errorf("hostname segment %q contains invalid character %q", part, string(c))
			}
		}
	}
	return nil
}
