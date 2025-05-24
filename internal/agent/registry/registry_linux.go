//go:build linux

package registry

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	registryBasePath = "/etc/pbs-plus-agent/registry"
	keyFile          = "/etc/pbs-plus-agent/registry/.key"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

type registryData struct {
	Values map[string]string `json:"values"`
}

// ensureRegistryDir creates the registry directory if it doesn't exist
func ensureRegistryDir() error {
	return os.MkdirAll(registryBasePath, 0755)
}

// getRegistryFilePath converts a Windows registry path to a Linux file path
func getRegistryFilePath(path string) string {
	// Convert Windows registry path to file path with all directories lowercase
	cleanPath := strings.ReplaceAll(path, "\\", "/")
	cleanPath = strings.ToLower(cleanPath)
	return filepath.Join(registryBasePath, cleanPath+".json")
}

// getEncryptionKey gets or creates an encryption key for secrets
func getEncryptionKey() ([]byte, error) {
	if err := ensureRegistryDir(); err != nil {
		return nil, err
	}

	// Try to read existing key
	if keyData, err := os.ReadFile(keyFile); err == nil {
		return keyData, nil
	}

	// Generate new key
	key := make([]byte, 32) // 256-bit key
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate encryption key: %w", err)
	}

	// Save key with restricted permissions
	if err := os.WriteFile(keyFile, key, 0600); err != nil {
		return nil, fmt.Errorf("failed to save encryption key: %w", err)
	}

	return key, nil
}

// encrypt encrypts a value using AES-GCM
func encrypt(plaintext string) (string, error) {
	key, err := getEncryptionKey()
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decrypt decrypts a value using AES-GCM
func decrypt(ciphertext string) (string, error) {
	key, err := getEncryptionKey()
	if err != nil {
		return "", err
	}

	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext_bytes := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext_bytes, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// loadRegistryFile loads registry data from file
func loadRegistryFile(filePath string) (*registryData, error) {
	data := &registryData{Values: make(map[string]string)}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return data, nil
	}

	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(fileData, data); err != nil {
		return nil, err
	}

	return data, nil
}

// saveRegistryFile saves registry data to file
func saveRegistryFile(filePath string, data *registryData) error {
	// Ensure all directory components are lowercase
	dir := filepath.Dir(filePath)
	dirParts := strings.Split(dir, "/")
	for i, part := range dirParts {
		dirParts[i] = strings.ToLower(part)
	}
	lowercaseDir := strings.Join(dirParts, "/")

	if err := os.MkdirAll(lowercaseDir, 0755); err != nil {
		return err
	}

	fileData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, fileData, 0644)
}

// GetEntry retrieves a registry entry
func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	filePath := getRegistryFilePath(path)
	data, err := loadRegistryFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}

	value, exists := data.Values[key]
	if !exists {
		return nil, fmt.Errorf("GetEntry error: key not found")
	}

	if isSecret {
		decrypted, err := decrypt(value)
		if err != nil {
			return nil, fmt.Errorf("GetEntry error: %w", err)
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
	filePath := getRegistryFilePath(entry.Path)
	data, err := loadRegistryFile(filePath)
	if err != nil {
		return fmt.Errorf("CreateEntry error: %w", err)
	}

	value := entry.Value
	if entry.IsSecret {
		encrypted, err := encrypt(value)
		if err != nil {
			return fmt.Errorf("CreateEntry error encrypting: %w", err)
		}
		value = encrypted
	}

	data.Values[entry.Key] = value

	if err := saveRegistryFile(filePath, data); err != nil {
		return fmt.Errorf("CreateEntry error saving: %w", err)
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
	filePath := getRegistryFilePath(path)
	data, err := loadRegistryFile(filePath)
	if err != nil {
		return fmt.Errorf("DeleteEntry error: %w", err)
	}

	if _, exists := data.Values[key]; !exists {
		return fmt.Errorf("DeleteEntry error: key not found")
	}

	delete(data.Values, key)

	if err := saveRegistryFile(filePath, data); err != nil {
		return fmt.Errorf("DeleteEntry error saving: %w", err)
	}

	return nil
}

// DeleteKey deletes an entire registry key and all its values
func DeleteKey(path string) error {
	filePath := getRegistryFilePath(path)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("DeleteKey error: %w", err)
	}
	return nil
}

// ListEntries lists all values in a registry key
func ListEntries(path string) ([]string, error) {
	filePath := getRegistryFilePath(path)
	data, err := loadRegistryFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error: %w", err)
	}

	var keys []string
	for key := range data.Values {
		keys = append(keys, key)
	}

	return keys, nil
}
