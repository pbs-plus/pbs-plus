package plugins

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const targetOperationTimeout = 30 * time.Second

type TargetType struct {
	PluginID      string
	PluginVersion string
	TargetType    string
	Schema        targetplugin.FormSchema
	BackupSchema  targetplugin.FormSchema
	RestoreSchema targetplugin.FormSchema
}

func ListTargetTypes(ctx context.Context, db *coredb.Store) ([]TargetType, error) {
	installed, err := db.ListInstalledPlugins(ctx)
	if err != nil {
		return nil, err
	}
	var result []TargetType
	for _, plugin := range installed {
		if !plugin.Enabled || plugin.ActiveVersion == "" {
			continue
		}
		manifest, _, err := loadActiveManifest(ctx, db, plugin.PluginID)
		if err != nil {
			return nil, err
		}
		for _, targetType := range manifest.TargetTypes {
			result = append(result, TargetType{
				PluginID:      plugin.PluginID,
				PluginVersion: plugin.ActiveVersion,
				TargetType:    targetType,
				Schema:        manifest.TargetSchema,
				BackupSchema:  manifest.BackupSchema,
				RestoreSchema: manifest.RestoreSchema,
			})
		}
	}
	return result, nil
}

func CreateTarget(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, name, pluginID, targetType string, submitted map[string][]string) error {
	if name == "" {
		return errors.New("target name is required")
	}
	manifest, installed, err := loadActiveManifest(ctx, db, pluginID)
	if err != nil {
		return err
	}
	if !slices.Contains(manifest.TargetTypes, targetType) {
		return fmt.Errorf("plugin %q does not provide target type %q", pluginID, targetType)
	}
	config, secrets, err := targetplugin.ParseFormValues(manifest.TargetSchema, submitted, nil)
	if err != nil {
		return err
	}
	normalized, err := validateTarget(ctx, supervisor, installed, targetType, manifest.TargetSchema.Version, config, secrets)
	if err != nil {
		return err
	}
	secretPresent := make(map[string]bool, len(secrets))
	for key := range secrets {
		secretPresent[key] = true
	}
	if err := targetplugin.ValidateFormConfig(manifest.TargetSchema, normalized, secretPresent); err != nil {
		return fmt.Errorf("validate normalized target: %w", err)
	}
	encoded, err := targetplugin.MarshalProtocol(normalized)
	if err != nil {
		return fmt.Errorf("encode target config: %w", err)
	}
	return db.CreatePluginTarget(ctx, coredb.PluginTarget{
		Name:          name,
		PluginID:      pluginID,
		PluginVersion: installed.Version,
		TargetType:    targetType,
		SchemaVersion: manifest.TargetSchema.Version,
		Config:        encoded,
	}, secrets)
}

func UpdateTarget(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, name string, submitted map[string][]string, deleteSecrets []string) error {
	target, err := db.GetPluginTarget(ctx, name)
	if err != nil {
		return err
	}
	manifest, installed, err := loadActiveManifest(ctx, db, target.PluginID)
	if err != nil {
		return err
	}
	if installed.Version != target.PluginVersion || manifest.TargetSchema.Version != target.SchemaVersion {
		return errors.New("plugin target requires schema migration before update")
	}
	existingSecrets, err := db.ResolvePluginTargetSecrets(ctx, name)
	if err != nil {
		return err
	}
	seenDeletes := make(map[string]struct{}, len(deleteSecrets))
	for _, key := range deleteSecrets {
		if !secretFieldExists(manifest.TargetSchema.Fields, key) {
			return fmt.Errorf("unknown secret field %q", key)
		}
		if _, exists := seenDeletes[key]; exists {
			return fmt.Errorf("secret field %q is deleted more than once", key)
		}
		seenDeletes[key] = struct{}{}
		delete(existingSecrets, key)
	}
	secretPresent := make(map[string]bool, len(existingSecrets))
	for key := range existingSecrets {
		secretPresent[key] = true
	}
	config, newSecrets, err := targetplugin.ParseFormValues(manifest.TargetSchema, submitted, secretPresent)
	if err != nil {
		return err
	}
	for key, value := range newSecrets {
		existingSecrets[key] = value
		secretPresent[key] = true
	}
	normalized, err := validateTarget(ctx, supervisor, installed, target.TargetType, target.SchemaVersion, config, existingSecrets)
	if err != nil {
		return err
	}
	if err := targetplugin.ValidateFormConfig(manifest.TargetSchema, normalized, secretPresent); err != nil {
		return fmt.Errorf("validate normalized target: %w", err)
	}
	encoded, err := targetplugin.MarshalProtocol(normalized)
	if err != nil {
		return fmt.Errorf("encode target config: %w", err)
	}
	target.Config = encoded
	return db.UpdatePluginTarget(ctx, target, newSecrets, deleteSecrets)
}

// ProbeTarget runs the active plugin against one persisted target.
func ProbeTarget(ctx context.Context, db *coredb.Store, supervisor *targetplugin.Supervisor, name string) (targetplugin.TargetProbeResponse, error) {
	target, err := db.GetPluginTarget(ctx, name)
	if err != nil {
		return targetplugin.TargetProbeResponse{}, err
	}
	manifest, installed, err := loadActiveManifest(ctx, db, target.PluginID)
	if err != nil {
		return targetplugin.TargetProbeResponse{}, err
	}
	if installed.Version != target.PluginVersion || manifest.TargetSchema.Version != target.SchemaVersion {
		return targetplugin.TargetProbeResponse{}, errors.New("plugin target requires schema migration before probe")
	}
	var config targetplugin.Values
	if err := targetplugin.UnmarshalProtocol(target.Config, &config); err != nil {
		return targetplugin.TargetProbeResponse{}, fmt.Errorf("decode target config: %w", err)
	}
	secrets, err := db.ResolvePluginTargetSecrets(ctx, name)
	if err != nil {
		return targetplugin.TargetProbeResponse{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, targetOperationTimeout)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	operationID := uuid.NewString()
	request := targetplugin.TargetProbeRequest{
		Operation: targetplugin.Operation{
			ProtocolVersion:   targetplugin.CurrentProtocolVersion,
			ID:                operationID,
			IdempotencyKey:    "target-probe:" + operationID,
			DeadlineUnixMilli: deadline.UnixMilli(),
			PluginVersion:     installed.Version,
			TargetType:        target.TargetType,
			SchemaVersion:     target.SchemaVersion,
		},
		Target: targetplugin.TargetInput{Config: config, Secrets: secrets},
	}
	var response targetplugin.TargetProbeResponse
	err = supervisor.Run(ctx, installed.PluginID, installed.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		request.Operation.BrokerToken = process.BrokerToken()
		return process.Invoke(runCtx, targetplugin.MethodTargetProbe, request, &response)
	})
	return response, err
}

func secretFieldExists(fields []targetplugin.FormField, key string) bool {
	for _, field := range fields {
		if field.Key == key {
			return field.Control == targetplugin.ControlSecret
		}
		if secretFieldExists(field.Fields, key) {
			return true
		}
	}
	return false
}

func loadActiveManifest(ctx context.Context, db *coredb.Store, pluginID string) (targetplugin.PluginManifest, coredb.InstalledPluginVersion, error) {
	plugin, err := db.GetInstalledPlugin(ctx, pluginID)
	if err != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, err
	}
	if !plugin.Enabled {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, fmt.Errorf("plugin %q is disabled", pluginID)
	}
	if plugin.ActiveVersion == "" {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, fmt.Errorf("plugin %q has no active version", pluginID)
	}
	return loadInstalledManifest(ctx, db, pluginID, plugin.ActiveVersion)
}

func loadInstalledManifest(ctx context.Context, db *coredb.Store, pluginID, version string) (targetplugin.PluginManifest, coredb.InstalledPluginVersion, error) {
	installed, err := db.GetInstalledPluginVersion(ctx, pluginID, version)
	if err != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, err
	}
	digest := sha256.Sum256(installed.Manifest)
	manifest, err := targetplugin.ParsePluginManifest(installed.Manifest, hex.EncodeToString(digest[:]))
	if err != nil {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, fmt.Errorf("load plugin %q manifest: %w", pluginID, err)
	}
	if manifest.PluginID != pluginID || manifest.Version != installed.Version {
		return targetplugin.PluginManifest{}, coredb.InstalledPluginVersion{}, errors.New("installed plugin manifest identity does not match registry")
	}
	return manifest, installed, nil
}

func validateTarget(ctx context.Context, supervisor *targetplugin.Supervisor, installed coredb.InstalledPluginVersion, targetType string, schemaVersion uint32, config targetplugin.Values, secrets targetplugin.Secrets) (targetplugin.Values, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, targetOperationTimeout)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	operationID := uuid.NewString()
	request := targetplugin.TargetValidateRequest{
		Operation: targetplugin.Operation{
			ProtocolVersion:   targetplugin.CurrentProtocolVersion,
			ID:                operationID,
			IdempotencyKey:    "target-validate:" + operationID,
			DeadlineUnixMilli: deadline.UnixMilli(),
			PluginVersion:     installed.Version,
			TargetType:        targetType,
			SchemaVersion:     schemaVersion,
		},
		Target: targetplugin.TargetInput{Config: config, Secrets: secrets},
	}
	var response targetplugin.TargetValidateResponse
	if err := supervisor.Run(ctx, installed.PluginID, installed.InstallPath, func(runCtx context.Context, process *targetplugin.Process) error {
		return process.Invoke(runCtx, targetplugin.MethodTargetValidate, request, &response)
	}); err != nil {
		return nil, err
	}
	return response.Config, nil
}
