//go:build linux

package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// CertManager manages TLS certificates for the PBS Plus server.
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

// NewCertManager creates an uninitialized CertManager.
// Call Validate() to generate or load certificates.
func NewCertManager() *CertManager {
	return &CertManager{}
}

// Validate generates or loads the local CA and server certificate.
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

// SignCSR signs an agent CSR with the local CA.
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

// ServeTLS calls ListenAndServeTLS on the server using the managed cert/key paths.
func (c *CertManager) ServeTLS(server *http.Server) error {
	c.mu.Lock()
	serverCert := c.ServerCertPath
	serverKey := c.ServerKeyPath
	c.mu.Unlock()
	return server.ListenAndServeTLS(serverCert, serverKey)
}

// APIServerTLSConfig builds the TLS config for the agent API server.
func (c *CertManager) APIServerTLSConfig() (*tls.Config, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return BuildServerTLS(c.ServerCertPath, c.ServerKeyPath, c.CACertPath,
		conf.AgentTLSPrevCACertFile, nil, tls.VerifyClientCertIfGiven, false)
}

// ARPCServerTLSConfig builds the TLS config for the ARPC server.
func (c *CertManager) ARPCServerTLSConfig() (*tls.Config, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return BuildServerTLS(c.ServerCertPath, c.ServerKeyPath, c.CACertPath,
		conf.AgentTLSPrevCACertFile, []string{"pbsarpc"}, tls.VerifyClientCertIfGiven, true)
}
