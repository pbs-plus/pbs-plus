//go:build linux

package plugins

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const healthCheckTimeout = 30 * time.Second

// AddRepositoryRequest carries the trust decision an administrator made out of band.
type AddRepositoryRequest struct {
	ID           string
	Name         string
	URL          string
	PublicKeyPEM string
	Fingerprint  string
}

// AddRepository pins a repository key only when its fingerprint matches the confirmed value.
func AddRepository(ctx context.Context, db *coredb.Store, request AddRepositoryRequest) (string, error) {
	if request.ID == "" || request.Name == "" {
		return "", errors.New("plugin repository identity and name are required")
	}
	if err := targetplugin.ValidateRepositoryURL(request.URL); err != nil {
		return "", err
	}
	if request.Fingerprint == "" {
		return "", errors.New("plugin repository key fingerprint must be confirmed")
	}
	der, publicKey, err := decodePublicKeyPEM(request.PublicKeyPEM)
	if err != nil {
		return "", err
	}
	fingerprint, err := targetplugin.PublicKeyFingerprint(publicKey)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(strings.TrimSpace(request.Fingerprint), fingerprint) {
		return "", fmt.Errorf("plugin repository key fingerprint is %s, not the confirmed value", fingerprint)
	}
	if err := db.CreatePluginRepository(ctx, coredb.PluginRepository{
		ID:        request.ID,
		Name:      request.Name,
		URL:       request.URL,
		PublicKey: der,
		Enabled:   true,
	}); err != nil {
		return "", err
	}
	return fingerprint, nil
}

// RemoveRepository refuses to orphan plugins that were installed from it.
func RemoveRepository(ctx context.Context, db *coredb.Store, repositoryID string) error {
	installed, err := db.ListInstalledPlugins(ctx)
	if err != nil {
		return err
	}
	for _, plugin := range installed {
		if plugin.RepositoryID == repositoryID {
			return fmt.Errorf("plugin %q is installed from repository %q", plugin.PluginID, repositoryID)
		}
	}
	deleted, err := db.DeletePluginRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("plugin repository %q was not found", repositoryID)
	}
	return nil
}

// CheckHealth probes one installed version and records the outcome for operators.
func CheckHealth(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, pluginID, version string) (bool, error) {
	installed, err := db.GetInstalledPluginVersion(ctx, pluginID, version)
	if err != nil {
		return false, err
	}
	var health targetplugin.PluginHealthResponse
	runErr := supervisor.Run(ctx, pluginID, installed.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		deadline, ok := runCtx.Deadline()
		if !ok {
			deadline = time.Now().Add(healthCheckTimeout)
		}
		request := targetplugin.PluginHealthRequest{Operation: targetplugin.Operation{
			ProtocolVersion:   targetplugin.CurrentProtocolVersion,
			ID:                "health-" + version,
			IdempotencyKey:    pluginID + "@" + version,
			DeadlineUnixMilli: deadline.UnixMilli(),
			PluginVersion:     version,
		}}
		return process.Invoke(runCtx, targetplugin.MethodPluginHealth, request, &health)
	})

	state := coredb.PluginHealthHealthy
	message := health.Message
	if runErr != nil {
		state = coredb.PluginHealthUnhealthy
		message = runErr.Error()
	} else if !health.Healthy {
		state = coredb.PluginHealthUnhealthy
	}
	if _, err := db.UpdatePluginVersionHealth(ctx, pluginID, version, state, message, time.Now()); err != nil {
		return false, err
	}
	if runErr != nil {
		return false, runErr
	}
	return state == coredb.PluginHealthHealthy, nil
}

func UninstallVersion(ctx context.Context, db *coredb.Store, root, pluginID, version string) error {
	plugin, err := db.GetInstalledPlugin(ctx, pluginID)
	if err != nil {
		return err
	}
	if plugin.ActiveVersion == version {
		return fmt.Errorf("plugin %q version %q is active", pluginID, version)
	}
	installed, err := db.GetInstalledPluginVersion(ctx, pluginID, version)
	if err != nil {
		return err
	}
	directory, err := installDirectory(root, installed.InstallPath)
	if err != nil {
		return err
	}
	deleted, err := db.DeleteInactivePluginVersion(ctx, pluginID, version)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("plugin %q version %q was not removed", pluginID, version)
	}
	if err := os.RemoveAll(directory); err != nil {
		return fmt.Errorf("removing plugin version directory: %w", err)
	}
	if _, err := db.DeleteEmptyPlugin(ctx, pluginID); err != nil {
		return err
	}
	return nil
}

func installDirectory(root, installPath string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", errors.New("plugin install root must be absolute")
	}
	directory := filepath.Dir(filepath.Clean(installPath))
	if !strings.HasPrefix(directory, filepath.Clean(root)+string(filepath.Separator)) {
		return "", fmt.Errorf("plugin install path %q is outside %q", installPath, root)
	}
	return directory, nil
}

func decodePublicKeyPEM(encoded string) ([]byte, *ecdsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(encoded))
	if block == nil || block.Type != "PUBLIC KEY" {
		return nil, nil, errors.New("plugin repository key must be a PEM public key")
	}
	publicKey, err := parsePublisherKey(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding plugin repository key: %w", err)
	}
	return der, publicKey, nil
}
