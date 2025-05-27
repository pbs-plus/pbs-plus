//go:build linux

package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

var httpClient *http.Client

func NewPBSHTTPClient() (*http.Client, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Skip all default verification
		},
	}

	// Custom verification that checks certificate but skips hostname
	transport.TLSClientConfig.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("no certificates provided by server")
		}

		// Load our trusted certificate
		certPEM, err := os.ReadFile(constants.CertFile)
		if err != nil {
			return fmt.Errorf("failed to read trusted certificate: %w", err)
		}

		block, _ := pem.Decode(certPEM)
		if block == nil {
			return fmt.Errorf("failed to decode PEM block")
		}

		trustedCert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("failed to parse trusted certificate: %w", err)
		}

		// Verify the server certificate matches our trusted certificate
		serverCert := cs.PeerCertificates[0]
		if !serverCert.Equal(trustedCert) {
			return fmt.Errorf("server certificate does not match trusted certificate")
		}

		// Optionally verify certificate validity period
		now := time.Now()
		if now.Before(serverCert.NotBefore) || now.After(serverCert.NotAfter) {
			return fmt.Errorf("server certificate is not valid at current time")
		}

		return nil
	}

	return &http.Client{
		Timeout:   time.Second * 30,
		Transport: transport,
	}, nil
}
