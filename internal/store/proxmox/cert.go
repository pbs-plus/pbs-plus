//go:build linux

package proxmox

import (
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

type CertInfo struct {
	Subject           string
	DNSNames          []string
	IPAddresses       []string
	Issuer            string
	NotBefore         time.Time
	NotAfter          time.Time
	FingerprintSHA256 string
	PublicKeyType     string
	PublicKeyBits     int
}

func GetProxmoxCertInfo() (*CertInfo, error) {
	cmd := exec.Command("proxmox-backup-manager", "cert", "info")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w, stderr: %s", err, stderr.String())
	}

	output := out.String()
	certInfo := &CertInfo{}

	// Regex for parsing
	subjectRx := regexp.MustCompile(`Subject:\s*(.*)`)
	dnsRx := regexp.MustCompile(`DNS:(.*)`)
	ipRx := regexp.MustCompile(`IP:\[(.*?)\]`)
	issuerRx := regexp.MustCompile(`Issuer:\s*(.*)`)
	notBeforeRx := regexp.MustCompile(`Not Before:\s*(.*) GMT`)
	notAfterRx := regexp.MustCompile(`Not After\s*:\s*(.*) GMT`)
	fingerprintRx := regexp.MustCompile(`Fingerprint \(sha256\):\s*(.*)`)
	publicKeyTypeRx := regexp.MustCompile(`Public key type:\s*(.*)`)
	publicKeyBitsRx := regexp.MustCompile(`Public key bits:\s*(\d+)`)

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if matches := subjectRx.FindStringSubmatch(line); len(matches) > 1 {
			certInfo.Subject = strings.TrimSpace(matches[1])
		} else if matches := dnsRx.FindStringSubmatch(line); len(matches) > 1 {
			certInfo.DNSNames = append(certInfo.DNSNames, strings.TrimSpace(matches[1]))
		} else if matches := ipRx.FindStringSubmatch(line); len(matches) > 1 {
			// Clean up IP string (e.g., "127, 0, 0, 1" to "127.0.0.1")
			ipStr := strings.ReplaceAll(matches[1], ", ", ".")
			certInfo.IPAddresses = append(certInfo.IPAddresses, strings.TrimSpace(ipStr))
		} else if matches := issuerRx.FindStringSubmatch(line); len(matches) > 1 {
			certInfo.Issuer = strings.TrimSpace(matches[1])
		} else if matches := notBeforeRx.FindStringSubmatch(line); len(matches) > 1 {
			t, err := time.Parse("Jan _2 15:04:05 2006", matches[1])
			if err != nil {
				return nil, fmt.Errorf("failed to parse Not Before time: %w", err)
			}
			certInfo.NotBefore = t
		} else if matches := notAfterRx.FindStringSubmatch(line); len(matches) > 1 {
			t, err := time.Parse("Jan _2 15:04:05 2006", matches[1])
			if err != nil {
				return nil, fmt.Errorf("failed to parse Not After time: %w", err)
			}
			certInfo.NotAfter = t
		} else if matches := fingerprintRx.FindStringSubmatch(line); len(matches) > 1 {
			certInfo.FingerprintSHA256 = strings.TrimSpace(matches[1])
		} else if matches := publicKeyTypeRx.FindStringSubmatch(line); len(matches) > 1 {
			certInfo.PublicKeyType = strings.TrimSpace(matches[1])
		} else if matches := publicKeyBitsRx.FindStringSubmatch(line); len(matches) > 1 {
			fmt.Sscanf(matches[1], "%d", &certInfo.PublicKeyBits)
		}
	}

	return certInfo, nil
}
