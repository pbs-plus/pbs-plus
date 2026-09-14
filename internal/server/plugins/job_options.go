//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const PluginJobOptionPrefix = "plugin-options."

func ParseBackupJobOptions(ctx context.Context, db *coredb.Store, targetName string, submitted map[string][]string) (*coredb.PluginJobOptions, error) {
	return parsePluginJobOptions(ctx, db, targetName, submitted, func(manifest targetplugin.PluginManifest) targetplugin.FormSchema {
		return manifest.BackupSchema
	})
}

func ParseRestoreJobOptions(ctx context.Context, db *coredb.Store, targetName string, submitted map[string][]string) (*coredb.PluginJobOptions, error) {
	return parsePluginJobOptions(ctx, db, targetName, submitted, func(manifest targetplugin.PluginManifest) targetplugin.FormSchema {
		return manifest.RestoreSchema
	})
}

func PluginJobOptionFormData(options *coredb.PluginJobOptions) (map[string]any, error) {
	result := make(map[string]any)
	if options == nil {
		return result, nil
	}
	var values targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(options.Options, &values); err != nil {
		return nil, fmt.Errorf("decode plugin job options: %w", err)
	}
	for key, value := range values {
		result[PluginJobOptionPrefix+key] = pluginJobScalarValue(value)
	}
	return result, nil
}

func parsePluginJobOptions(ctx context.Context, db *coredb.Store, targetName string, submitted map[string][]string, schema func(targetplugin.PluginManifest) targetplugin.FormSchema) (*coredb.PluginJobOptions, error) {
	form := pluginJobForm(submitted)
	target, err := db.GetPluginTarget(ctx, targetName)
	if errors.Is(err, coredb.ErrTargetNotFound) {
		if len(form) != 0 {
			return nil, errors.New("plugin job options require a plugin target")
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	manifest, installed, err := loadActiveManifest(ctx, db, target.PluginID)
	if err != nil {
		return nil, err
	}
	if target.PluginVersion != installed.Version {
		return nil, fmt.Errorf("target %q uses inactive plugin version %q", targetName, target.PluginVersion)
	}
	jobSchema := schema(manifest)
	encoded, err := encodePluginJobForm(jobSchema, form)
	if err != nil {
		return nil, err
	}
	return &coredb.PluginJobOptions{
		PluginID:      target.PluginID,
		PluginVersion: target.PluginVersion,
		SchemaVersion: jobSchema.Version,
		Options:       encoded,
	}, nil
}

func pluginJobForm(submitted map[string][]string) map[string][]string {
	form := make(map[string][]string)
	for key, values := range submitted {
		if after, ok := strings.CutPrefix(key, PluginJobOptionPrefix); ok {
			form[after] = values
		}
	}
	return form
}

func encodePluginJobForm(schema targetplugin.FormSchema, form map[string][]string) ([]byte, error) {
	values, secrets, err := targetplugin.ParseFormValues(schema, form, nil)
	if err != nil {
		return nil, err
	}
	if len(secrets) != 0 {
		return nil, errors.New("plugin job option schemas cannot contain secret fields")
	}
	encoded, err := targetplugin.MarshalProtocol(values)
	if err != nil {
		return nil, fmt.Errorf("encode plugin job options: %w", err)
	}
	return encoded, nil
}

func pluginJobScalarValue(value targetplugin.Scalar) any {
	switch value.Kind() {
	case targetplugin.ScalarString:
		result, _ := value.StringValue()
		return result
	case targetplugin.ScalarInteger:
		result, _ := value.IntegerValue()
		return result
	case targetplugin.ScalarBoolean:
		result, _ := value.BooleanValue()
		return result
	default:
		return nil
	}
}
