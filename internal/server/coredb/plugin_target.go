package coredb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
)

type PluginTarget struct {
	Name          string
	PluginID      string
	PluginVersion string
	TargetType    string
	SchemaVersion uint32
	Config        []byte
	SecretFields  []string
	UpdatedAt     time.Time
}

type PluginTargetConfigHistory struct {
	ID            int64
	TargetName    string
	PluginID      string
	PluginVersion string
	SchemaVersion uint32
	Config        []byte
	MigratedAt    time.Time
}

func (db *Store) CreatePluginTarget(ctx context.Context, target PluginTarget, secrets map[string][]byte) error {
	if err := validatePluginTarget(target); err != nil {
		return err
	}
	encrypted, err := encryptPluginTargetSecrets(secrets)
	if err != nil {
		return err
	}
	ctx = db.pluginContext(ctx)
	return db.RunInTransaction(ctx, func(_ *Transaction, queries *corequery.Queries) error {
		if err := queries.CreateTarget(ctx, corequery.CreateTargetParams{
			Name:       target.Name,
			TargetType: target.TargetType,
		}); err != nil {
			return fmt.Errorf("create plugin target: %w", err)
		}
		if err := queries.CreatePluginTargetConfig(ctx, pluginTargetParams(target)); err != nil {
			return fmt.Errorf("create plugin target config: %w", err)
		}
		return writePluginTargetSecrets(ctx, queries, target.Name, encrypted, nil)
	})
}

// AttachPluginTarget adds a plugin config to an existing target row, leaving the
// legacy kind and detail tables untouched for the compatibility read path.
func (db *Store) AttachPluginTarget(ctx context.Context, target PluginTarget, secrets map[string][]byte) error {
	if err := validatePluginTarget(target); err != nil {
		return err
	}
	encrypted, err := encryptPluginTargetSecrets(secrets)
	if err != nil {
		return err
	}
	ctx = db.pluginContext(ctx)
	return db.RunInTransaction(ctx, func(_ *Transaction, queries *corequery.Queries) error {
		if _, err := queries.GetTarget(ctx, target.Name); err != nil {
			return fmt.Errorf("attach plugin target: target %q does not exist: %w", target.Name, err)
		}
		if err := queries.CreatePluginTargetConfig(ctx, pluginTargetParams(target)); err != nil {
			return fmt.Errorf("create plugin target config: %w", err)
		}
		return writePluginTargetSecrets(ctx, queries, target.Name, encrypted, nil)
	})
}

func (db *Store) UpdatePluginTarget(ctx context.Context, target PluginTarget, secrets map[string][]byte, deleteSecrets []string) error {
	if err := validatePluginTarget(target); err != nil {
		return err
	}
	encrypted, err := encryptPluginTargetSecrets(secrets)
	if err != nil {
		return err
	}
	ctx = db.pluginContext(ctx)
	return db.RunInTransaction(ctx, func(_ *Transaction, queries *corequery.Queries) error {
		rows, err := queries.UpdatePluginTargetConfig(ctx, corequery.UpdatePluginTargetConfigParams{
			PluginVersion: target.PluginVersion,
			TargetType:    target.TargetType,
			SchemaVersion: int64(target.SchemaVersion),
			Config:        target.Config,
			UpdatedAt:     pluginTargetUpdatedAt(target).Unix(),
			TargetName:    target.Name,
			PluginID:      target.PluginID,
		})
		if err != nil {
			return fmt.Errorf("update plugin target config: %w", err)
		}
		if rows != 1 {
			return ErrTargetNotFound
		}
		if err := queries.UpdateTarget(ctx, corequery.UpdateTargetParams{
			TargetType: target.TargetType,
			Name:       target.Name,
		}); err != nil {
			return fmt.Errorf("update plugin target: %w", err)
		}
		return writePluginTargetSecrets(ctx, queries, target.Name, encrypted, deleteSecrets)
	})
}

// SyncAttachedPluginTarget updates plugin data without replacing the legacy target kind used by compatibility APIs.
func (db *Store) SyncAttachedPluginTarget(ctx context.Context, target PluginTarget, secrets map[string][]byte, deleteSecrets []string) error {
	if err := validatePluginTarget(target); err != nil {
		return err
	}
	encrypted, err := encryptPluginTargetSecrets(secrets)
	if err != nil {
		return err
	}
	ctx = db.pluginContext(ctx)
	return db.RunInTransaction(ctx, func(_ *Transaction, queries *corequery.Queries) error {
		rows, err := queries.UpdatePluginTargetConfig(ctx, corequery.UpdatePluginTargetConfigParams{
			PluginVersion: target.PluginVersion,
			TargetType:    target.TargetType,
			SchemaVersion: int64(target.SchemaVersion),
			Config:        target.Config,
			UpdatedAt:     pluginTargetUpdatedAt(target).Unix(),
			TargetName:    target.Name,
			PluginID:      target.PluginID,
		})
		if err != nil {
			return fmt.Errorf("sync attached plugin target config: %w", err)
		}
		if rows != 1 {
			return ErrTargetNotFound
		}
		return writePluginTargetSecrets(ctx, queries, target.Name, encrypted, deleteSecrets)
	})
}

func (db *Store) GetPluginTarget(ctx context.Context, name string) (PluginTarget, error) {
	ctx = db.pluginContext(ctx)
	row, err := db.readQueries.GetPluginTargetConfig(ctx, name)
	if errors.Is(err, sql.ErrNoRows) {
		return PluginTarget{}, ErrTargetNotFound
	}
	if err != nil {
		return PluginTarget{}, fmt.Errorf("get plugin target: %w", err)
	}
	secrets, err := db.readQueries.ListPluginTargetSecrets(ctx, name)
	if err != nil {
		return PluginTarget{}, fmt.Errorf("list plugin target secrets: %w", err)
	}
	result := pluginTargetFromRow(row)
	result.SecretFields = make([]string, len(secrets))
	for index := range secrets {
		result.SecretFields[index] = secrets[index].FieldKey
	}
	return result, nil
}

func (db *Store) ResolvePluginTargetSecrets(ctx context.Context, name string) (map[string][]byte, error) {
	rows, err := db.readQueries.ListPluginTargetSecrets(db.pluginContext(ctx), name)
	if err != nil {
		return nil, fmt.Errorf("list plugin target secrets: %w", err)
	}
	secrets := make(map[string][]byte, len(rows))
	for _, row := range rows {
		plaintext, err := Decrypt(row.EncryptedValue)
		if err != nil {
			return nil, fmt.Errorf("decrypt plugin target secret %q: %w", row.FieldKey, err)
		}
		secrets[row.FieldKey] = []byte(plaintext)
	}
	return secrets, nil
}

func (db *Store) ListPluginTargets(ctx context.Context, pluginID string) ([]PluginTarget, error) {
	rows, err := db.readQueries.ListPluginTargetConfigsByPlugin(db.pluginContext(ctx), pluginID)
	if err != nil {
		return nil, fmt.Errorf("list plugin targets: %w", err)
	}
	targets := make([]PluginTarget, len(rows))
	for index := range rows {
		targets[index] = pluginTargetFromRow(rows[index])
	}
	return targets, nil
}

func (db *Store) ListPluginTargetConfigHistory(ctx context.Context, name string) ([]PluginTargetConfigHistory, error) {
	rows, err := db.readQueries.ListPluginTargetConfigHistory(db.pluginContext(ctx), name)
	if err != nil {
		return nil, fmt.Errorf("list plugin target config history: %w", err)
	}
	history := make([]PluginTargetConfigHistory, len(rows))
	for index, row := range rows {
		history[index] = PluginTargetConfigHistory{
			ID:            row.ID,
			TargetName:    row.TargetName,
			PluginID:      row.PluginID,
			PluginVersion: row.PluginVersion,
			SchemaVersion: uint32(row.SchemaVersion),
			Config:        row.Config,
			MigratedAt:    time.Unix(row.MigratedAt, 0),
		}
	}
	return history, nil
}

func validatePluginTarget(target PluginTarget) error {
	if target.Name == "" || target.PluginID == "" || target.PluginVersion == "" || target.TargetType == "" {
		return errors.New("plugin target identity is incomplete")
	}
	if target.SchemaVersion == 0 {
		return errors.New("plugin target schema version is zero")
	}
	if len(target.Config) == 0 || len(target.Config) > 4<<20 {
		return errors.New("plugin target config size is invalid")
	}
	return nil
}

func pluginTargetParams(target PluginTarget) corequery.CreatePluginTargetConfigParams {
	return corequery.CreatePluginTargetConfigParams{
		TargetName:    target.Name,
		PluginID:      target.PluginID,
		PluginVersion: target.PluginVersion,
		TargetType:    target.TargetType,
		SchemaVersion: int64(target.SchemaVersion),
		Config:        target.Config,
		UpdatedAt:     pluginTargetUpdatedAt(target).Unix(),
	}
}

func pluginTargetUpdatedAt(target PluginTarget) time.Time {
	if target.UpdatedAt.IsZero() {
		return time.Now()
	}
	return target.UpdatedAt
}

func encryptPluginTargetSecrets(secrets map[string][]byte) (map[string]string, error) {
	encrypted := make(map[string]string, len(secrets))
	for key, value := range secrets {
		if key == "" || len(key) > 128 {
			return nil, fmt.Errorf("invalid plugin target secret key %q", key)
		}
		ciphertext, err := Encrypt(string(value))
		if err != nil {
			return nil, fmt.Errorf("encrypt plugin target secret %q: %w", key, err)
		}
		encrypted[key] = ciphertext
	}
	return encrypted, nil
}

func writePluginTargetSecrets(ctx context.Context, queries *corequery.Queries, targetName string, encrypted map[string]string, deleteSecrets []string) error {
	keys := make([]string, 0, len(encrypted))
	for key := range encrypted {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := queries.UpsertPluginTargetSecret(ctx, corequery.UpsertPluginTargetSecretParams{
			TargetName:     targetName,
			FieldKey:       key,
			EncryptedValue: encrypted[key],
		}); err != nil {
			return fmt.Errorf("store plugin target secret %q: %w", key, err)
		}
	}
	for _, key := range deleteSecrets {
		if _, err := queries.DeletePluginTargetSecret(ctx, corequery.DeletePluginTargetSecretParams{
			TargetName: targetName,
			FieldKey:   key,
		}); err != nil {
			return fmt.Errorf("delete plugin target secret %q: %w", key, err)
		}
	}
	return nil
}

func pluginTargetFromRow(row corequery.PluginTargetConfig) PluginTarget {
	return PluginTarget{
		Name:          row.TargetName,
		PluginID:      row.PluginID,
		PluginVersion: row.PluginVersion,
		TargetType:    row.TargetType,
		SchemaVersion: uint32(row.SchemaVersion),
		Config:        row.Config,
		UpdatedAt:     time.Unix(row.UpdatedAt, 0),
	}
}
