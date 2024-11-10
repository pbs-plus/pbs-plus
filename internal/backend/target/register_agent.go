//go:build linux

package target

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func RegisterAgent(storeInstance *store.Store, hostname, clientIP, basePath string) (string, error) {
	privKey, pubKey, err := utils.GenerateKeyPair(4096)
	privKeyDir := filepath.Join(store.DbBasePath, "agent_keys")

	err = os.MkdirAll(privKeyDir, 0700)
	if err != nil {
		return "", fmt.Errorf("RegisterAgent: error creating directory \"%s\" -> %w", privKeyDir, err)
	}

	newTarget := store.Target{
		Name: fmt.Sprintf("%s - %s", hostname, basePath),
		Path: fmt.Sprintf("agent://%s/%s", clientIP, basePath),
	}

	privKeyFilePath := filepath.Join(
		privKeyDir,
		strings.ReplaceAll(fmt.Sprintf("%s.key", newTarget.Name), " ", "-"),
	)

	privKeyFile, err := os.OpenFile(privKeyFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return "", fmt.Errorf("RegisterAgent: error opening private key file \"%s\" -> %w", privKeyFilePath, err)
	}
	defer privKeyFile.Close()

	_, err = privKeyFile.Write(privKey)
	if err != nil {
		return "", fmt.Errorf("RegisterAgent: error writing private key content -> %w", err)
	}

	err = storeInstance.CreateTarget(newTarget)
	if err != nil {
		return "", fmt.Errorf("RegisterAgent: error creating target in database -> %w", err)
	}

	return string(pubKey), nil
}
