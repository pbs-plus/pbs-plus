package utils

import (
	"net"
	"net/http"
)

func IsLocalhost(r *http.Request) bool {
	// If these headers are present, the request went through a proxy.
	// Even if the proxy is local, the original user is NOT.
	if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("X-Real-IP") != "" {
		return false
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsLoopback()
}

