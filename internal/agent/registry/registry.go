//go:build windows

package registry

import (
	"fmt"

	"github.com/billgraziano/dpapi"
	"golang.org/x/sys/windows/registry"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

// GetEntry retrieves a registry entry
func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}
	defer baseKey.Close()

	value, _, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}

	if isSecret {
		value, err = dpapi.Decrypt(value)
		if err != nil {
			return nil, fmt.Errorf("GetEntry error: %w", err)
		}
	}

	return &RegistryEntry{
		Path:     path,
		Key:      key,
		Value:    value,
		IsSecret: isSecret,
	}, nil
}

// CreateEntry creates a new registry entry
func CreateEntry(entry *RegistryEntry) error {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		// If the key doesn't exist, create it
		baseKey, _, err = registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
		if err != nil {
			return fmt.Errorf("CreateEntry error creating key: %w", err)
		}
	}
	defer baseKey.Close()

	value := entry.Value
	if entry.IsSecret {
		encrypted, err := dpapi.Encrypt(value)
		if err != nil {
			return fmt.Errorf("CreateEntry error encrypting: %w", err)
		}
		value = encrypted
	}

	err = baseKey.SetStringValue(entry.Key, value)
	if err != nil {
		return fmt.Errorf("CreateEntry error setting value: %w", err)
	}

	return nil
}

// UpdateEntry updates an existing registry entry
func UpdateEntry(entry *RegistryEntry) error {
	// First check if the entry exists
	_, err := GetEntry(entry.Path, entry.Key, entry.IsSecret)
	if err != nil {
		return fmt.Errorf("UpdateEntry error: entry does not exist: %w", err)
	}

	// Reuse CreateEntry logic for the update
	return CreateEntry(entry)
}

// DeleteEntry deletes a registry entry
func DeleteEntry(path string, key string) error {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("DeleteEntry error opening key: %w", err)
	}
	defer baseKey.Close()

	err = baseKey.DeleteValue(key)
	if err != nil {
		return fmt.Errorf("DeleteEntry error deleting value: %w", err)
	}

	return nil
}

// DeleteKey deletes an entire registry key and all its values
func DeleteKey(path string) error {
	err := registry.DeleteKey(registry.LOCAL_MACHINE, path)
	if err != nil {
		return fmt.Errorf("DeleteKey error: %w", err)
	}
	return nil
}

// ListEntries lists all values in a registry key
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
