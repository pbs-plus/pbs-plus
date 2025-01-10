package utils

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

func GetLocalIPs() ([]string, error) {
	var ips []string
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && ip.To4() != nil {
				ips = append(ips, ip.String())
			}
		}
	}
	return ips, nil
}

func IsRequestFromSelf(r *http.Request) bool {
	remoteIP := strings.Split(r.RemoteAddr, ":")[0] // Extract the IP part
	localIPs, err := GetLocalIPs()
	if err != nil {
		fmt.Println("Error fetching local IPs:", err)
		return false
	}

	for _, ip := range localIPs {
		if remoteIP == ip {
			return true
		}
	}
	return false
}
