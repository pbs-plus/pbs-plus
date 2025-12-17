//go:build windows

package registry

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/billgraziano/dpapi"
	"golang.org/x/sys/windows/registry"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

var (
	// legacy file-based paths (from the previous implementation)
	legacySecretFilePath string // "secrets.json"
	legacyKeyFilePath    string // "master.key" (no longer used, but removed if present)

	initOnce sync.Once
	initErr  error
)

func ensureInitialized() error {
	initOnce.Do(func() {
		// C:\PBS Plus Agent\secrets.json and C:\PBS Plus Agent\master.key
		serviceDataDir := filepath.Join("C:\\", "PBS Plus Agent")
		legacySecretFilePath = filepath.Join(serviceDataDir, "secrets.json")
		legacyKeyFilePath = filepath.Join(serviceDataDir, "master.key")

		// Attempt migration; ignore non-fatal issues but record a fatal initErr.
		if err := migrateFromFileSecretsToRegistry(); err != nil {
			// Non-fatal: we continue operating with DPAPI+registry regardless.
			// Only set initErr if you want to surface migration failure.
			// Here, we log it via wrapping but do not fail init.
			_ = err
		}
	})
	return initErr
}

func migrateFromFileSecretsToRegistry() error {
	type secretsMap = map[string]string

	legacySecrets, err := readLegacySecrets(legacySecretFilePath)
	if err != nil {
		return nil
	}
	if len(legacySecrets) == 0 {
		cleanupLegacyFiles()
		return nil
	}

	for b64k, val := range legacySecrets {
		raw, err := base64.StdEncoding.DecodeString(b64k)
		if err != nil {
			// Skip malformed entries
			continue
		}
		parts := strings.SplitN(string(raw), "|", 2)
		if len(parts) != 2 {
			continue
		}
		path := parts[0]
		key := parts[1]

		if err := writeSecretDPAPIToRegistry(path, key, val); err != nil {
			continue
		}
	}

	cleanupLegacyFiles()
	return nil
}

func readLegacySecrets(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func writeSecretDPAPIToRegistry(path, key, plaintext string) error {
	enc, err := dpapi.EncryptMachineLocal(plaintext)
	if err != nil {
		return fmt.Errorf("dpapi encrypt (LocalMachine) failed: %w", err)
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		baseKey, _, err = registry.CreateKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
		if err != nil {
			return fmt.Errorf("failed creating/opening registry key: %w", err)
		}
	}
	defer baseKey.Close()

	if err := baseKey.SetStringValue(key, enc); err != nil {
		return fmt.Errorf("failed setting registry value: %w", err)
	}
	return nil
}

func migrateFromRegistryDPAPI(path, key string) (*RegistryEntry, error) {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, err
	}
	defer baseKey.Close()

	val, _, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, err
	}

	plain, err := dpapi.Decrypt(val)
	if err != nil {
		return nil, fmt.Errorf("DPAPI decryption failed: %w", err)
	}

	return &RegistryEntry{Path: path, Key: key, Value: plain, IsSecret: true}, nil
}

func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	if err := ensureInitialized(); err != nil {
		return nil, err
	}

	if isSecret {
		if entry, err := migrateFromRegistryDPAPI(path, key); err == nil {
			return entry, nil
		}
		return nil, errors.New("GetEntry error: secret not found")
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error opening key: %w", err)
	}
	defer baseKey.Close()

	value, _, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error reading value: %w", err)
	}

	return &RegistryEntry{Path: path, Key: key, Value: value, IsSecret: false}, nil
}

func CreateEntry(entry *RegistryEntry) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	if entry.IsSecret {
		return writeSecretDPAPIToRegistry(entry.Path, entry.Key, entry.Value)
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		baseKey, _, err = registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
		if err != nil {
			return fmt.Errorf("CreateEntry error creating registry key: %w", err)
		}
	}
	defer baseKey.Close()

	if err := baseKey.SetStringValue(entry.Key, entry.Value); err != nil {
		return fmt.Errorf("CreateEntry error setting value: %w", err)
	}

	return nil
}

func CreateEntryIfNotExists(entry *RegistryEntry) error {
	_, err := GetEntry(entry.Path, entry.Key, entry.IsSecret)
	if err == nil {
		return errors.New("CreateEntryIfNotExists error: entry already exists")
	}
	return CreateEntry(entry)
}

func UpdateEntry(entry *RegistryEntry) error {
	_, err := GetEntry(entry.Path, entry.Key, entry.IsSecret)
	if err != nil {
		return fmt.Errorf("UpdateEntry error: entry does not exist: %w", err)
	}
	return CreateEntry(entry)
}

func DeleteEntry(path string, key string) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("DeleteEntry error opening key: %w", err)
	}
	defer baseKey.Close()

	if err := baseKey.DeleteValue(key); err != nil {
		return fmt.Errorf("DeleteEntry error deleting value: %w", err)
	}

	return nil
}

func DeleteKey(path string) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	// Best-effort cleanup of legacy secrets.json if present.
	legacySecrets, _ := readLegacySecrets(legacySecretFilePath)
	if len(legacySecrets) > 0 {
		changed := false
		for k := range legacySecrets {
			if decoded, decErr := base64.StdEncoding.DecodeString(k); decErr == nil {
				if strings.HasPrefix(string(decoded), path+"|") {
					delete(legacySecrets, k)
					changed = true
				}
			}
		}
		if changed {
			_ = os.WriteFile(legacySecretFilePath, mustJSON(legacySecrets), 0o600)
		}
	}

	if err := registry.DeleteKey(registry.LOCAL_MACHINE, path); err != nil {
		return fmt.Errorf("DeleteKey error deleting registry key: %w", err)
	}

	return nil
}

func ListEntries(path string) ([]string, error) {
	if err := ensureInitialized(); err != nil {
		return nil, err
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error opening key: %w", err)
	}
	defer baseKey.Close()

	names, err := baseKey.ReadValueNames(0)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error reading value names: %w", err)
	}

	return names, nil
}

func mustJSON(v any) []byte {
	b, _ := json.MarshalIndent(v, "", "  ")
	return b
}

func cleanupLegacyFiles() {
	_ = os.Remove(legacySecretFilePath)
	_ = os.Remove(legacyKeyFilePath)
}
