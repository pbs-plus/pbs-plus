package utils

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

func IsProxyCertValid(hostname string) bool {
	certPEM, err := os.ReadFile(constants.CertFile)
	if err != nil {
		return false
	}

	keyPEM, err := os.ReadFile(constants.KeyFile)
	if err != nil {
		return false
	}

	_, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return false
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}

	now := time.Now()
	if now.Before(cert.NotBefore) {
		return false
	}
	if now.After(cert.NotAfter) {
		return false
	}

	roots, err := x509.SystemCertPool()
	if err != nil {
		return false
	}

	opts := x509.VerifyOptions{
		Roots:       roots,
		DNSName:     hostname,
		CurrentTime: now,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	_, err = cert.Verify(opts)
	if err != nil {
		return false
	}

	return true
}
