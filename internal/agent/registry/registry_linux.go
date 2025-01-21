//go:build linux

package registry

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

const (
	baseStoragePath = "/tmp"                               // Change myapp to your application name
	encryptionKey   = "your-32-byte-encryption-key-here!!" // Change this in production
)

func init() {
	// Ensure storage directory exists with correct permissions
	if err := os.MkdirAll(baseStoragePath, 0750); err != nil {
		panic(fmt.Sprintf("Failed to create storage directory: %v", err))
	}
}

func getFilePath(path string) string {
	// Convert Windows-style path to filesystem path
	return filepath.Join(baseStoragePath, filepath.Clean(path))
}

func encrypt(text string) (string, error) {
	block, err := aes.NewCipher([]byte(encryptionKey))
	if err != nil {
		return "", err
	}

	plaintext := []byte(text)
	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := rand.Read(iv); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], plaintext)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func decrypt(cryptoText string) (string, error) {
	block, err := aes.NewCipher([]byte(encryptionKey))
	if err != nil {
		return "", err
	}

	ciphertext, err := base64.StdEncoding.DecodeString(cryptoText)
	if err != nil {
		return "", err
	}

	if len(ciphertext) < aes.BlockSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(ciphertext, ciphertext)

	return string(ciphertext), nil
}

func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	filePath := getFilePath(path)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error reading file: %w", err)
	}

	var entries map[string]string
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("GetEntry error parsing JSON: %w", err)
	}

	value, ok := entries[key]
	if !ok {
		return nil, fmt.Errorf("GetEntry error: key not found")
	}

	if isSecret {
		value, err = decrypt(value)
		if err != nil {
			return nil, fmt.Errorf("GetEntry error decrypting: %w", err)
		}
	}

	return &RegistryEntry{
		Path:     path,
		Key:      key,
		Value:    value,
		IsSecret: isSecret,
	}, nil
}

func CreateEntry(entry *RegistryEntry) error {
	dirPath := getFilePath(entry.Path)
	if err := os.MkdirAll(filepath.Dir(dirPath), 0750); err != nil {
		return fmt.Errorf("CreateEntry error creating directory: %w", err)
	}

	var entries map[string]string
	data, err := os.ReadFile(dirPath)
	if err == nil {
		if err := json.Unmarshal(data, &entries); err != nil {
			return fmt.Errorf("CreateEntry error parsing JSON: %w", err)
		}
	} else {
		entries = make(map[string]string)
	}

	value := entry.Value
	if entry.IsSecret {
		value, err = encrypt(value)
		if err != nil {
			return fmt.Errorf("CreateEntry error encrypting: %w", err)
		}
	}

	entries[entry.Key] = value

	data, err = json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("CreateEntry error marshaling JSON: %w", err)
	}

	if err := os.WriteFile(dirPath, data, 0640); err != nil {
		return fmt.Errorf("CreateEntry error writing file: %w", err)
	}

	return nil
}

func UpdateEntry(entry *RegistryEntry) error {
	// First check if the entry exists
	_, err := GetEntry(entry.Path, entry.Key, entry.IsSecret)
	if err != nil {
		return fmt.Errorf("UpdateEntry error: entry does not exist: %w", err)
	}
	// Reuse CreateEntry logic for the update
	return CreateEntry(entry)
}

func DeleteEntry(path string, key string) error {
	filePath := getFilePath(path)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("DeleteEntry error reading file: %w", err)
	}

	var entries map[string]string
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("DeleteEntry error parsing JSON: %w", err)
	}

	delete(entries, key)

	data, err = json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("DeleteEntry error marshaling JSON: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0640); err != nil {
		return fmt.Errorf("DeleteEntry error writing file: %w", err)
	}

	return nil
}

func DeleteKey(path string) error {
	filePath := getFilePath(path)
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("DeleteKey error: %w", err)
	}
	return nil
}

func ListEntries(path string) ([]string, error) {
	filePath := getFilePath(path)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error reading file: %w", err)
	}

	var entries map[string]string
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("ListEntries error parsing JSON: %w", err)
	}

	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}

	return keys, nil
}
