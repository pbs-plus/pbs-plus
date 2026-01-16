package utils

import (
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

	roots, err := x509.SystemCertPool()
	if err != nil {
		return false
	}

	intermediates := x509.NewCertPool()
	var leaf *x509.Certificate

	rest := certPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}

		if leaf == nil {
			leaf = cert
		} else {
			intermediates.AddCert(cert)
		}
	}

	if leaf == nil {
		return false
	}

	opts := x509.VerifyOptions{
		DNSName:       hostname,
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   time.Now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	_, err = leaf.Verify(opts)
	return err == nil
}
