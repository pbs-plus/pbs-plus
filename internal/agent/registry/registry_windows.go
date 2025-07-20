//go:build windows

package registry

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/billgraziano/dpapi"
	"golang.org/x/crypto/nacl/secretbox"
	"golang.org/x/sys/windows/registry"
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

var (
	secretFilePath string
	secretKey      = getOrCreateSecretKey()
	secretsMu      sync.Mutex
)

func init() {
	ex, err := os.Executable()
	if err != nil {
		return
	}

	secretFilePath = filepath.Join(filepath.Dir(ex), "secret.json")
}

func getOrCreateSecretKey() *[32]byte {
	ex, err := os.Executable()
	if err != nil {
		return nil
	}

	keyPath := filepath.Join(filepath.Dir(ex), "secret.key")
	var key [32]byte
	if data, err := os.ReadFile(keyPath); err == nil && len(data) == 32 {
		copy(key[:], data)
		return &key
	}
	_, _ = rand.Read(key[:])
	_ = os.MkdirAll(filepath.Dir(keyPath), 0700)
	_ = os.WriteFile(keyPath, key[:], 0600)
	return &key
}

func loadSecrets() (map[string]string, error) {
	secretsMu.Lock()
	defer secretsMu.Unlock()
	m := make(map[string]string)
	data, err := os.ReadFile(secretFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return m, nil
		}
		return nil, err
	}
	return m, json.Unmarshal(data, &m)
}

func saveSecrets(m map[string]string) error {
	secretsMu.Lock()
	defer secretsMu.Unlock()
	_ = os.MkdirAll(filepath.Dir(secretFilePath), 0700)
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(secretFilePath, data, 0600)
}

func secretKeyFor(path, key string) string {
	return base64.StdEncoding.EncodeToString([]byte(path + "|" + key))
}

func encryptSecret(plaintext string) (string, error) {
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	encrypted := secretbox.Seal(nonce[:], []byte(plaintext), &nonce, secretKey)
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func decryptSecret(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil || len(data) < 24 {
		return "", errors.New("invalid ciphertext")
	}
	var nonce [24]byte
	copy(nonce[:], data[:24])
	plaintext, ok := secretbox.Open(nil, data[24:], &nonce, secretKey)
	if !ok {
		return "", errors.New("decryption failed")
	}
	return string(plaintext), nil
}

func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	if isSecret {
		// Try new file-based secret store first
		secrets, err := loadSecrets()
		if err != nil {
			return nil, fmt.Errorf("GetEntry error loading secrets: %w", err)
		}
		sk := secretKeyFor(path, key)
		if enc, ok := secrets[sk]; ok {
			val, err := decryptSecret(enc)
			if err != nil {
				return nil, fmt.Errorf("GetEntry error decrypting secret: %w", err)
			}
			return &RegistryEntry{Path: path, Key: key, Value: val, IsSecret: true}, nil
		}

		// Fallback: try to migrate from registry (old DPAPI)
		baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
		if err == nil {
			defer baseKey.Close()
			if val, _, err := baseKey.GetStringValue(key); err == nil {
				plain, err := dpapi.Decrypt(val)
				if err == nil {
					// Migrate to new store
					enc, _ := encryptSecret(plain)
					secrets[sk] = enc
					_ = saveSecrets(secrets)
					_ = baseKey.DeleteValue(key)
					return &RegistryEntry{Path: path, Key: key, Value: plain, IsSecret: true}, nil
				}
			}
		}
		return nil, fmt.Errorf("GetEntry error: secret not found")
	}

	// Non-secret: registry as before
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}
	defer baseKey.Close()
	value, _, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}
	return &RegistryEntry{Path: path, Key: key, Value: value, IsSecret: false}, nil
}

func CreateEntry(entry *RegistryEntry) error {
	if entry.IsSecret {
		secrets, err := loadSecrets()
		if err != nil {
			return fmt.Errorf("CreateEntry error loading secrets: %w", err)
		}
		enc, err := encryptSecret(entry.Value)
		if err != nil {
			return fmt.Errorf("CreateEntry error encrypting: %w", err)
		}
		secrets[secretKeyFor(entry.Path, entry.Key)] = enc
		return saveSecrets(secrets)
	}

	// Non-secret: registry as before
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		baseKey, _, err = registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
		if err != nil {
			return fmt.Errorf("CreateEntry error creating key: %w", err)
		}
	}
	defer baseKey.Close()
	return baseKey.SetStringValue(entry.Key, entry.Value)
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
	// Try both stores
	secrets, err := loadSecrets()
	if err == nil {
		sk := secretKeyFor(path, key)
		if _, ok := secrets[sk]; ok {
			delete(secrets, sk)
			return saveSecrets(secrets)
		}
	}
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("DeleteEntry error opening key: %w", err)
	}
	defer baseKey.Close()
	return baseKey.DeleteValue(key)
}

func DeleteKey(path string) error {
	// Remove all secrets for this path
	secrets, err := loadSecrets()
	if err == nil {
		changed := false
		for k := range secrets {
			if len(k) > 0 && string(k[:len(path)]) == path {
				delete(secrets, k)
				changed = true
			}
		}
		if changed {
			_ = saveSecrets(secrets)
		}
	}
	return registry.DeleteKey(registry.LOCAL_MACHINE, path)
}

func ListEntries(path string) ([]string, error) {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("ListEntries error opening key: %w", err)
	}
	defer baseKey.Close()
	return baseKey.ReadValueNames(0)
}
