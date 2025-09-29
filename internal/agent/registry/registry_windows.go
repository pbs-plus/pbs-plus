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
	"strings"
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
	secretFilePath    string
	keyFilePath       string
	legacySecretsPath string
	legacyKeyPath     string
	secretKey         *[32]byte
	secretsMu         sync.Mutex
	initOnce          sync.Once
	initErr           error
)

func init() {
	systemRoot := os.Getenv("SYSTEMROOT")
	if systemRoot == "" {
		systemRoot = "C:\\"
	}
	serviceDataDir := filepath.Join(systemRoot, "PBS Plus Agent")
	secretFilePath = filepath.Join(serviceDataDir, "secrets.json")
	keyFilePath = filepath.Join(serviceDataDir, "master.key")

	if ex, err := os.Executable(); err == nil {
		execDir := filepath.Dir(ex)
		legacySecretsPath = filepath.Join(execDir, "secret.json")
		legacyKeyPath = filepath.Join(execDir, "secret.key")
	}
}

func ensureInitialized() error {
	initOnce.Do(func() {
		dir := filepath.Dir(secretFilePath)
		if err := os.MkdirAll(dir, 0700); err != nil {
			initErr = fmt.Errorf("failed to create service data directory: %w", err)
			return
		}

		if err := migrateLegacyData(); err != nil {
			_ = err
		}

		secretKey = getOrCreateSecretKey()
		if secretKey == nil {
			initErr = errors.New("failed to initialize secret key")
		}
	})
	return initErr
}

func migrateLegacyData() error {
	if legacySecretsPath == "" || legacyKeyPath == "" {
		return nil
	}

	if _, err := os.Stat(secretFilePath); err == nil {
		if _, err := os.Stat(keyFilePath); err == nil {
			cleanupLegacyFiles()
			return nil
		}
	}

	if err := migrateLegacyKey(); err != nil {
		return fmt.Errorf("failed to migrate legacy key: %w", err)
	}

	if err := migrateLegacySecrets(); err != nil {
		return fmt.Errorf("failed to migrate legacy secrets: %w", err)
	}

	cleanupLegacyFiles()
	return nil
}

func migrateLegacyKey() error {
	legacyData, err := os.ReadFile(legacyKeyPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	if len(legacyData) != 32 {
		return fmt.Errorf("invalid legacy key length: %d", len(legacyData))
	}

	if err := os.WriteFile(keyFilePath, legacyData, 0600); err != nil {
		return fmt.Errorf("failed to write new key file: %w", err)
	}

	return nil
}

func migrateLegacySecrets() error {
	legacyData, err := os.ReadFile(legacySecretsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var legacySecrets map[string]string
	if err := json.Unmarshal(legacyData, &legacySecrets); err != nil {
		return fmt.Errorf("invalid legacy secrets format: %w", err)
	}

	if err := os.WriteFile(secretFilePath, legacyData, 0600); err != nil {
		return fmt.Errorf("failed to write new secrets file: %w", err)
	}

	return nil
}

func cleanupLegacyFiles() {
	if legacySecretsPath != "" {
		_ = os.Remove(legacySecretsPath)
	}
	if legacyKeyPath != "" {
		_ = os.Remove(legacyKeyPath)
	}
}

func getOrCreateSecretKey() *[32]byte {
	var key [32]byte

	if data, err := os.ReadFile(keyFilePath); err == nil && len(data) == 32 {
		copy(key[:], data)
		return &key
	}

	if _, err := rand.Read(key[:]); err != nil {
		return nil
	}

	if err := os.WriteFile(keyFilePath, key[:], 0600); err != nil {
		return nil
	}

	return &key
}

func loadSecrets() (map[string]string, error) {
	if err := ensureInitialized(); err != nil {
		return nil, err
	}

	secretsMu.Lock()
	defer secretsMu.Unlock()

	m := make(map[string]string)
	data, err := os.ReadFile(secretFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return m, nil
		}
		return nil, fmt.Errorf("failed to read secrets file: %w", err)
	}

	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse secrets file: %w", err)
	}
	return m, nil
}

func saveSecrets(m map[string]string) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	secretsMu.Lock()
	defer secretsMu.Unlock()

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal secrets: %w", err)
	}

	if err := os.WriteFile(secretFilePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write secrets file: %w", err)
	}

	return nil
}

func secretKeyFor(path, key string) string {
	return base64.StdEncoding.EncodeToString([]byte(path + "|" + key))
}

func encryptSecret(plaintext string) (string, error) {
	if err := ensureInitialized(); err != nil {
		return "", fmt.Errorf("encryption initialization failed: %w", err)
	}

	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	encrypted := secretbox.Seal(nonce[:], []byte(plaintext), &nonce, secretKey)
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func decryptSecret(ciphertext string) (string, error) {
	if err := ensureInitialized(); err != nil {
		return "", fmt.Errorf("decryption initialization failed: %w", err)
	}

	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("invalid base64: %w", err)
	}
	if len(data) < 24 {
		return "", errors.New("ciphertext too short")
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

		if entry, err := migrateFromRegistryDPAPI(path, key); err == nil {
			enc, encErr := encryptSecret(entry.Value)
			if encErr == nil {
				secrets[sk] = enc
				if saveErr := saveSecrets(secrets); saveErr == nil {
					if baseKey, regErr := registry.OpenKey(registry.LOCAL_MACHINE, path, registry.SET_VALUE); regErr == nil {
						_ = baseKey.DeleteValue(key)
						baseKey.Close()
					}
				}
			}
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

	if err := baseKey.DeleteValue(key); err != nil {
		return fmt.Errorf("DeleteEntry error deleting value: %w", err)
	}

	return nil
}

func DeleteKey(path string) error {
	secrets, err := loadSecrets()
	if err == nil {
		changed := false

		for k := range secrets {
			if decoded, decErr := base64.StdEncoding.DecodeString(k); decErr == nil {
				if strings.HasPrefix(string(decoded), path+"|") {
					delete(secrets, k)
					changed = true
				}
			}
		}

		if changed {
			if saveErr := saveSecrets(secrets); saveErr != nil {
				return fmt.Errorf("DeleteKey error saving secrets: %w", saveErr)
			}
		}
	}

	if err := registry.DeleteKey(registry.LOCAL_MACHINE, path); err != nil {
		return fmt.Errorf("DeleteKey error deleting registry key: %w", err)
	}

	return nil
}

func ListEntries(path string) ([]string, error) {
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
