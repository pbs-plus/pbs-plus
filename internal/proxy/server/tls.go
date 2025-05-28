package server

import (
	"crypto/tls"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

func createServerTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(constants.CertFile, constants.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
