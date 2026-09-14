//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin/filesystem"
)

// ImportLocalTargets creates plugin target rows for every legacy local filesystem target.
// Existing rows are left untouched, so the import is idempotent and reversible by flag.
func ImportLocalTargets(ctx context.Context, db *coredb.Store) (int, error) {
	plugin, err := db.GetInstalledPlugin(ctx, filesystem.PluginID)
	if err != nil {
		return 0, fmt.Errorf("local filesystem plugin is not installed: %w", err)
	}
	if !plugin.Enabled || plugin.ActiveVersion == "" {
		return 0, errors.New("local filesystem plugin is not enabled")
	}
	manifest, _, err := loadActiveManifest(ctx, db, filesystem.PluginID)
	if err != nil {
		return 0, err
	}

	targets, err := db.GetAllTargets()
	if err != nil {
		return 0, err
	}
	imported := 0
	for _, target := range targets {
		if !target.IsLocal() {
			continue
		}
		if _, err := db.GetPluginTarget(ctx, target.Name); err == nil {
			continue
		} else if !errors.Is(err, coredb.ErrTargetNotFound) {
			return imported, err
		}
		config, err := targetplugin.MarshalProtocol(targetplugin.Values{
			"path": targetplugin.NewStringScalar(target.Path),
		})
		if err != nil {
			return imported, err
		}
		if err := db.AttachPluginTarget(ctx, coredb.PluginTarget{
			Name:          target.Name,
			PluginID:      filesystem.PluginID,
			PluginVersion: plugin.ActiveVersion,
			TargetType:    filesystem.TargetTypeLocal,
			SchemaVersion: manifest.TargetSchema.Version,
			Config:        config,
			SecretFields:  nil,
		}, nil); err != nil {
			return imported, err
		}
		imported++
	}
	return imported, nil
}
