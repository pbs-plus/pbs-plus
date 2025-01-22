package agent

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	authErrors "github.com/sonroyaalmerol/pbs-plus/internal/auth/errors"
)

// Config represents the agent configuration
type Config struct {
	// Agent identification
	AgentID string

	// TLS configuration
	CertFile string
	KeyFile  string
	CAFile   string

	// Server connection
	ServerURL string
	Timeout   time.Duration

	// Retry configuration
	MaxRetries    int
	RetryInterval time.Duration
	BackoffFactor float64
	MaxBackoff    time.Duration

	// Keep-alive configuration
	KeepAlive         bool
	KeepAliveInterval time.Duration
}

// DefaultConfig returns a default agent configuration
func DefaultConfig() *Config {
	return &Config{
		Timeout:           30 * time.Second,
		MaxRetries:        5,
		RetryInterval:     time.Second,
		BackoffFactor:     2.0,
		MaxBackoff:        1 * time.Minute,
		KeepAlive:         true,
		KeepAliveInterval: 5 * time.Minute,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.AgentID == "" {
		return authErrors.WrapError("validate_config",
			errors.New("agent ID is required"))
	}

	if c.ServerURL == "" {
		return authErrors.WrapError("validate_config",
			errors.New("server URL is required"))
	}

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

// LoadTLSConfig creates a TLS configuration from the agent config
func (c *Config) LoadTLSConfig() (*tls.Config, error) {
	// Load client certificate
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
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}, nil
}

