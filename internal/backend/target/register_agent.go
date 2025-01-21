//go:build linux

package target

import (
	"encoding/base64"
	"fmt"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
)

func RegisterAgent(storeInstance *store.Store, hostname, clientIP, csrPEM string, drives []string) (string, error) {
	certPEM, err := storeInstance.CertGenerator.GenerateCert(csrPEM)
	if err != nil {
		return "", fmt.Errorf("RegisterAgent: error generating cert \"%s\" -> %w", hostname, err)
	}

	for _, drive := range drives {
		encoded := base64.StdEncoding.EncodeToString(certPEM)

		newTarget := store.Target{
			Name: fmt.Sprintf("%s - %s", hostname, drive),
			Path: fmt.Sprintf("agent://%s/%s", clientIP, drive),
			Auth: encoded,
		}

		err = storeInstance.CreateTarget(newTarget)
		if err != nil {
			return "", fmt.Errorf("RegisterAgent: error creating target in database -> %w", err)
		}
	}

	return string(certPEM), nil
}
