//go:build windows

package registry

import (
	"fmt"

	"github.com/zalando/go-keyring"
	"golang.org/x/sys/windows/registry"
)

const (
	keyringService = "PBSPlusAgent"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

// GetEntry retrieves a registry entry or secret (auto-migrates if needed)
func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	if isSecret {
		// Try to get from keyring first
		value, err := keyring.Get(keyringService, path+"|"+key)
		if err == nil {
			return &RegistryEntry{
				Path:     path,
				Key:      key,
				Value:    value,
				IsSecret: true,
			}, nil
		}

		// Fallback: try to get from registry (old DPAPI way)
		baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
		if err == nil {
			defer baseKey.Close()
			regValue, _, regErr := baseKey.GetStringValue(key)
			if regErr == nil {
				// Migrate: store in keyring, then delete from registry
				_ = keyring.Set(keyringService, path+"|"+key, regValue)
				_ = baseKey.DeleteValue(key)
				return &RegistryEntry{
					Path:     path,
					Key:      key,
					Value:    regValue,
					IsSecret: true,
				}, nil
			}
		}
		return nil, fmt.Errorf("GetEntry error: secret not found")
	}

	// Non-secret: regular registry
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}
	defer baseKey.Close()

	value, _, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}

	return &RegistryEntry{
		Path:     path,
		Key:      key,
		Value:    value,
		IsSecret: false,
	}, nil
}

// CreateEntry creates a new registry entry or secret
func CreateEntry(entry *RegistryEntry) error {
	if entry.IsSecret {
		return keyring.Set(keyringService, entry.Path+"|"+entry.Key, entry.Value)
	}

	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		baseKey, _, err = registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
		if err != nil {
			return fmt.Errorf("CreateEntry error creating key: %w", err)
		}
	}
	defer baseKey.Close()

	err = baseKey.SetStringValue(entry.Key, entry.Value)
	if err != nil {
		return fmt.Errorf("CreateEntry error setting value: %w", err)
	}
	return nil
}

func CreateEntryIfNotExists(entry *RegistryEntry) error {
	_, err := GetEntry(entry.Path, entry.Key, entry.IsSecret)
	if err == nil {
		return fmt.Errorf("CreateEntryIfNotExists error: entry already exists")
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
	// Try both keyring and registry
	_ = keyring.Delete(keyringService, path+"|"+key)
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		return nil // If not found, treat as deleted
	}
	defer baseKey.Close()
	_ = baseKey.DeleteValue(key)
	return nil
}

func DeleteKey(path string) error {
	// No keyring equivalent for deleting all secrets under a path, so just delete registry key
	return registry.DeleteKey(registry.LOCAL_MACHINE, path)
}

func ListEntries(path string) ([]string, error) {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error opening key: %w", err)
	}
	defer baseKey.Close()

	valueNames, err := baseKey.ReadValueNames(0)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error reading values: %w", err)
	}
	return valueNames, nil
}
