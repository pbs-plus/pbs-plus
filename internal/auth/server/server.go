package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	authErrors "github.com/pbs-plus/pbs-plus/internal/auth/errors"
)

// Config represents the server configuration
type Config struct {
	// Server TLS configuration
	CertFile string
	KeyFile  string
	CAFile   string
	CAKey    string

	// Token configuration
	TokenExpiration time.Duration
	TokenSecret     string

	// Server configuration
	Address        string
	AgentAddress   string
	ReadTimeout    time.Duration
	IdleTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxHeaderBytes int

	// Rate limiting
	RateLimit float64 // Requests per second
	RateBurst int     // Maximum burst size
}

const ProxyCert = "/etc/proxmox-backup/proxy.pem"
const ProxyKey = "/etc/proxmox-backup/proxy.key"

// DefaultConfig returns a default server configuration
func DefaultConfig() *Config {
	return &Config{
		Address:        ":8017",
		AgentAddress:   ":8008",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   5 * time.Minute,
		IdleTimeout:    5 * time.Minute,
		MaxHeaderBytes: 1 << 20, // 1MB

		TokenExpiration: 24 * time.Hour,

		RateLimit: 100.0,
		RateBurst: 200,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		return authErrors.ErrCertificateRequired
	}

	// Check if certificate files exist
	files := []string{c.CertFile, c.KeyFile, c.CAFile}
	for _, file := range files {
		if _, err := os.Stat(file); err != nil {
			return authErrors.WrapError("validate_config", err)
		}
	}

	return nil
}

// LoadTLSConfig creates a TLS configuration from the server config
func (c *Config) LoadTLSConfig() (*tls.Config, error) {
	// Load server certificate
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, authErrors.WrapError("load_tls_config", err)
	}

	// Load CA cert
	caCert, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, authErrors.WrapError("load_tls_config", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, authErrors.WrapError("load_tls_config",
			errors.New("failed to append CA certificate"))
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
		MinVersion:   tls.VersionTLS12,
		// TLS 1.3 ciphers are fixed; AES-GCM will be preferred on AES-NI.
		// For TLS 1.2, force AES-GCM and prefer server order:
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			// Omit ChaCha to ensure AES-GCM is chosen for TLS 1.2.
		},
	}, nil
}
