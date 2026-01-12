//go:build windows

package registry

import (
	"encoding/base64"
	"encoding/json"
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
	legacySecretFilePath string
	legacyKeyFilePath    string

	initOnce sync.Once
	initErr  error
)

func init() {
	_ = ensureInitialized()
}

func ensureInitialized() error {
	initOnce.Do(func() {
		serviceDataDir := filepath.Join("C:\\", "PBS Plus Agent")
		legacySecretFilePath = filepath.Join(serviceDataDir, "secrets.json")
		legacyKeyFilePath = filepath.Join(serviceDataDir, "master.key")

		_ = migrateFromFileSecretsToRegistry()
	})
	return initErr
}

func migrateFromFileSecretsToRegistry() error {
	data, err := os.ReadFile(legacySecretFilePath)
	if err != nil {
		return nil
	}
	var legacySecrets map[string]string
	if err := json.Unmarshal(data, &legacySecrets); err != nil {
		return nil
	}

	for b64k, val := range legacySecrets {
		raw, err := base64.StdEncoding.DecodeString(b64k)
		if err != nil {
			continue
		}
		parts := strings.SplitN(string(raw), "|", 2)
		if len(parts) != 2 {
			continue
		}
		_ = writeSecretDPAPIToRegistry(parts[0], parts[1], val)
	}

	_ = os.Remove(legacySecretFilePath)
	_ = os.Remove(legacyKeyFilePath)
	return nil
}

func writeSecretDPAPIToRegistry(path, key, plaintext string) error {
	enc, err := dpapi.EncryptMachineLocal(plaintext)
	if err != nil {
		return fmt.Errorf("dpapi encrypt failed: %w", err)
	}

	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("failed opening registry key: %w", err)
	}
	defer baseKey.Close()

	return baseKey.SetStringValue(key, enc)
}

func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	if err := ensureInitialized(); err != nil {
		return nil, err
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}
	defer baseKey.Close()

	val, _, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: key not found")
	}

	if isSecret {
		plain, err := dpapi.Decrypt(val)
		if err != nil {
			return nil, fmt.Errorf("GetEntry decryption error: %w", err)
		}
		val = plain
	}

	return &RegistryEntry{
		Path:     path,
		Key:      key,
		Value:    val,
		IsSecret: isSecret,
	}, nil
}

func CreateEntry(entry *RegistryEntry) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	// Check existence first
	if _, err := GetEntry(entry.Path, entry.Key, entry.IsSecret); err == nil {
		return fmt.Errorf("CreateEntry error: key already exists")
	}

	if entry.IsSecret {
		return writeSecretDPAPIToRegistry(entry.Path, entry.Key, entry.Value)
	}

	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("CreateEntry error: %w", err)
	}
	defer baseKey.Close()

	return baseKey.SetStringValue(entry.Key, entry.Value)
}

func UpdateEntry(entry *RegistryEntry) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	if _, err := GetEntry(entry.Path, entry.Key, entry.IsSecret); err != nil {
		return fmt.Errorf("UpdateEntry error: entry does not exist")
	}

	if entry.IsSecret {
		return writeSecretDPAPIToRegistry(entry.Path, entry.Key, entry.Value)
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		return err
	}
	defer baseKey.Close()

	return baseKey.SetStringValue(entry.Key, entry.Value)
}

func CreateEntryIfNotExists(entry *RegistryEntry) error {
	if _, err := GetEntry(entry.Path, entry.Key, entry.IsSecret); err == nil {
		return nil
	}
	return CreateEntry(entry)
}

func DeleteEntry(path string, key string) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		return nil // Path doesn't exist, effectively deleted
	}
	defer baseKey.Close()

	return baseKey.DeleteValue(key)
}

func DeleteKey(path string) error {
	if err := ensureInitialized(); err != nil {
		return err
	}
	return registry.DeleteKey(registry.LOCAL_MACHINE, path)
}

func ListEntries(path string) ([]string, error) {
	if err := ensureInitialized(); err != nil {
		return nil, err
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return []string{}, nil
	}
	defer baseKey.Close()

	return baseKey.ReadValueNames(0)
}
