package utils

import (
	"net"
	"net/http"
)

func IsLocalhost(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	if ip.IsLoopback() {
		return true
	}

	if isLocalInterfaceIP(ip) {
		return true
	}

	return false
}

func isLocalInterfaceIP(ip net.IP) bool {
	ifaces, err := net.Interfaces()
	if err != nil {
		return false
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ipNet *net.IPNet
			switch v := a.(type) {
			case *net.IPNet:
				ipNet = v
			case *net.IPAddr:
				ipNet = &net.IPNet{IP: v.IP, Mask: net.CIDRMask(128, 128)} // exact match
			default:
				continue
			}
			if ipNet == nil || ipNet.IP == nil {
				continue
			}
			if (ip.To4() != nil && ipNet.IP.To4() == nil) || (ip.To4() == nil && ipNet.IP.To4() != nil) {
				continue
			}
			if ipNet.Contains(ip) {
				return true
			}
		}
	}
	return false
}
