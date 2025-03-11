//go:build linux

package registry

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/nacl/secretbox"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

const (
	baseRegistryPath = "/etc/pbs-plus-agent/registry" // Base directory for the "registry"
	secretKeyFile    = "secret.key"                   // File to store the secret key
)

func normalizePath(path string) string {
	return strings.ReplaceAll(path, "\\", "/")
}

var secretKey [32]byte // Global secret key for encryption/decryption

// Initialize ensures the base registry path exists and loads or generates the secret key
func init() {
	// Ensure the base registry path exists
	if err := os.MkdirAll(baseRegistryPath, 0755); err != nil {
		return
	}

	// Load or generate the secret key
	keyPath := filepath.Join(baseRegistryPath, secretKeyFile)
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		// Generate a new secret key
		if err := generateAndStoreSecretKey(keyPath); err != nil {
			return
		}
	} else {
		// Load the existing secret key
		if err := loadSecretKey(keyPath); err != nil {
			return
		}
	}

	return
}

// GetEntry retrieves a registry entry
func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	path = normalizePath(path)
	fullPath := filepath.Join(baseRegistryPath, path, key)

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}

	value := string(data)
	if isSecret {
		decrypted, err := decrypt(value)
		if err != nil {
			return nil, fmt.Errorf("GetEntry error decrypting: %w", err)
		}
		value = decrypted
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
	fullPath := filepath.Join(baseRegistryPath, normalizePath(entry.Path))
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return fmt.Errorf("CreateEntry error creating path: %w", err)
	}

	value := entry.Value
	if entry.IsSecret {
		encrypted, err := encrypt(value)
		if err != nil {
			return fmt.Errorf("CreateEntry error encrypting: %w", err)
		}
		value = encrypted
	}

	filePath := filepath.Join(fullPath, entry.Key)
	if err := os.WriteFile(filePath, []byte(value), 0644); err != nil {
		return fmt.Errorf("CreateEntry error writing file: %w", err)
	}

	return nil
}

// UpdateEntry updates an existing registry entry
func UpdateEntry(entry *RegistryEntry) error {
	// First check if the entry exists
	_, err := GetEntry(normalizePath(entry.Path), entry.Key, entry.IsSecret)
	if err != nil {
		return fmt.Errorf("UpdateEntry error: entry does not exist: %w", err)
	}

	// Reuse CreateEntry logic for the update
	return CreateEntry(entry)
}

// DeleteEntry deletes a registry entry
func DeleteEntry(path string, key string) error {
	path = normalizePath(path)
	filePath := filepath.Join(baseRegistryPath, path, key)
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("DeleteEntry error deleting file: %w", err)
	}

	return nil
}

// DeleteKey deletes an entire registry key and all its values
func DeleteKey(path string) error {
	path = normalizePath(path)
	fullPath := filepath.Join(baseRegistryPath, path)
	if err := os.RemoveAll(fullPath); err != nil {
		return fmt.Errorf("DeleteKey error: %w", err)
	}
	return nil
}

// ListEntries lists all values in a registry key
func ListEntries(path string) ([]string, error) {
	path = normalizePath(path)
	fullPath := filepath.Join(baseRegistryPath, path)
	files, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error reading directory: %w", err)
	}

	var valueNames []string
	for _, file := range files {
		if !file.IsDir() {
			valueNames = append(valueNames, file.Name())
		}
	}

	return valueNames, nil
}

// Helper functions for encryption and decryption

func encrypt(value string) (string, error) {
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	encrypted := secretbox.Seal(nonce[:], []byte(value), &nonce, &secretKey)
	return hex.EncodeToString(encrypted), nil
}

func decrypt(value string) (string, error) {
	data, err := hex.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("failed to decode encrypted value: %w", err)
	}

	if len(data) < 24 {
		return "", errors.New("invalid encrypted value")
	}

	var nonce [24]byte
	copy(nonce[:], data[:24])

	decrypted, ok := secretbox.Open(nil, data[24:], &nonce, &secretKey)
	if !ok {
		return "", errors.New("decryption failed")
	}

	return string(decrypted), nil
}

// Secret key management

func generateAndStoreSecretKey(keyPath string) error {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return fmt.Errorf("failed to generate secret key: %w", err)
	}

	keyHex := hex.EncodeToString(key[:])
	if err := os.WriteFile(keyPath, []byte(keyHex), 0600); err != nil {
		return fmt.Errorf("failed to store secret key: %w", err)
	}

	secretKey = key
	return nil
}

func loadSecretKey(keyPath string) error {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("failed to read secret key: %w", err)
	}

	keyBytes, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf("failed to decode secret key: %w", err)
	}

	if len(keyBytes) != 32 {
		return errors.New("invalid secret key length")
	}

	copy(secretKey[:], keyBytes)
	return nil
}
