//go:build linux

package mtls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// It handles CA and server certificate generation, loading, and TLS config building.
type CertManager struct {
	mu sync.Mutex

	ServerCertPath string
	ServerCertPEM  []byte
	ServerKeyPath  string
	ServerKeyPEM   []byte
	CACertPath     string
	CACertPEM      []byte
	CAKeyPath      string
	CAKeyPEM       []byte
}

// Call Validate() to generate or load certificates.
func NewCertManager() *CertManager {
	return &CertManager{}
}

func (c *CertManager) Validate() error {
	serverCertPath, serverKeyPath, caCertPath, caKeyPath, err := EnsureLocalCAAndServerCert(
		filepath.Dir(conf.AgentTLSCACertFile),
		"PBS Plus",
		"PBS Plus CA",
		2048,
		365,
	)
	if err != nil {
		return err
	}

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return fmt.Errorf("read ca cert: %w", err)
	}
	caKey, err := os.ReadFile(caKeyPath)
	if err != nil {
		return fmt.Errorf("read ca key: %w", err)
	}
	serverCert, err := os.ReadFile(serverCertPath)
	if err != nil {
		return fmt.Errorf("read server cert: %w", err)
	}
	serverKey, err := os.ReadFile(serverKeyPath)
	if err != nil {
		return fmt.Errorf("read server key: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.ServerCertPEM = serverCert
	c.ServerCertPath = serverCertPath
	c.ServerKeyPEM = serverKey
	c.ServerKeyPath = serverKeyPath
	c.CACertPEM = caCert
	c.CACertPath = caCertPath
	c.CAKeyPEM = caKey
	c.CAKeyPath = caKeyPath

	return nil
}

func (c *CertManager) SignCSR(csr []byte) (cert []byte, ca []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cert, err = SignCSR(
		c.CACertPEM,
		c.CAKeyPEM,
		csr,
		365,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	if err != nil {
		return nil, nil, err
	}
	return cert, c.CACertPEM, nil
}

func (c *CertManager) ServeTLS(server *http.Server) error {
	c.mu.Lock()
	serverCert := c.ServerCertPath
	serverKey := c.ServerKeyPath
	c.mu.Unlock()
	return server.ListenAndServeTLS(serverCert, serverKey)
}

func (c *CertManager) APIServerTLSConfig() (*tls.Config, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return BuildServerTLS(c.ServerCertPath, c.ServerKeyPath, c.CACertPath,
		conf.AgentTLSPrevCACertFile, nil, tls.VerifyClientCertIfGiven, false)
}

func (c *CertManager) ARPCServerTLSConfig() (*tls.Config, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return BuildServerTLS(c.ServerCertPath, c.ServerKeyPath, c.CACertPath,
		conf.AgentTLSPrevCACertFile, []string{"pbsarpc"}, tls.VerifyClientCertIfGiven, true)
}

func (c *CertManager) CAFingerprint() (string, error) {
	c.mu.Lock()
	caPEM := c.CACertPEM
	c.mu.Unlock()

	if len(caPEM) == 0 {
		return "", fmt.Errorf("CA certificate not loaded")
	}

	block, _ := pem.Decode(caPEM)
	if block == nil {
		return "", fmt.Errorf("failed to decode CA certificate PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse CA certificate: %w", err)
	}

	digest := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(digest[:]), nil
}
