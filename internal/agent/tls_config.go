//go:build windows

package agent

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
)

func GetTLSConfig() (*tls.Config, error) {
	serverCertReg, err := registry.GetEntry(registry.AUTH, "ServerCA", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: server cert not found -> %w", err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM([]byte(serverCertReg.Value)); !ok {
		return nil, fmt.Errorf("failed to append CA certificate: %s", serverCertReg.Value)
	}

	certReg, err := registry.GetEntry(registry.AUTH, "Cert", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: cert not found -> %w", err)
	}

	keyReg, err := registry.GetEntry(registry.AUTH, "Priv", true)
	if err != nil {
		return nil, fmt.Errorf("GetTLSConfig: key not found -> %w", err)
	}

	certPEM := []byte(certReg.Value)
	keyPEM := []byte(keyReg.Value)

	// Configure TLS client
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}, nil
}
