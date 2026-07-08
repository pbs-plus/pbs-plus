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
	"github.com/pbs-plus/pbs-plus/internal/log"
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

func isPEMData(value string) bool {
	return strings.Contains(value, "-----BEGIN") && strings.Contains(value, "-----END")
}

func normalizePEMData(pemData string) string {
	pemData = strings.ReplaceAll(pemData, "\r\n", "\n")
	pemData = strings.ReplaceAll(pemData, "\r", "\n")
	lines := strings.Split(pemData, "\n")
	var normalized []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	res := strings.Join(normalized, "\n")
	if !strings.HasSuffix(res, "\n") {
		res += "\n"
	}
	return res
}

func preprocessValue(value string, isSecret bool) string {
	if isSecret && isPEMData(value) {
		return normalizePEMData(value)
	}
	return value
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

	val, valType, err := baseKey.GetStringValue(key)
	if err != nil {
		return nil, fmt.Errorf("GetEntry error: key not found")
	}

	if isSecret {
		diagLen := len(val)
		if diagLen > 40 {
			log.Debug("GetEntry: raw registry value", "path", path, "key", key, "type", valType, "len", diagLen, "prefix", val[:40])
		} else {
			log.Debug("GetEntry: raw registry value", "path", path, "key", key, "type", valType, "len", diagLen, "prefix", val)
		}

		plain, err := dpapi.Decrypt(val)
		if err != nil {
			return nil, fmt.Errorf("GetEntry decryption error: %w", err)
		}
		val = plain

		plainLen := len(val)
		startsPEM := strings.HasPrefix(val, "-----BEGIN")
		if plainLen > 40 {
			log.Debug("GetEntry: decrypted value", "path", path, "key", key, "len", plainLen, "startsPEM", startsPEM, "prefix", fmt.Sprintf("%q", val[:40]))
		} else {
			log.Debug("GetEntry: decrypted value", "path", path, "key", key, "len", plainLen, "startsPEM", startsPEM, "value", fmt.Sprintf("%q", val))
		}
	}

	if isSecret && isPEMData(val) {
		val = normalizePEMData(val)
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

	if _, err := GetEntry(entry.Path, entry.Key, entry.IsSecret); err == nil {
		return fmt.Errorf("CreateEntry error: key already exists")
	}

	value := preprocessValue(entry.Value, entry.IsSecret)

	if entry.IsSecret {
		return writeSecretDPAPIToRegistry(entry.Path, entry.Key, value)
	}

	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("CreateEntry error: %w", err)
	}
	defer baseKey.Close()

	return baseKey.SetStringValue(entry.Key, value)
}

func UpsertEntry(entry *RegistryEntry) error {
	if err := ensureInitialized(); err != nil {
		return err
	}

	value := preprocessValue(entry.Value, entry.IsSecret)

	if entry.IsSecret {
		return writeSecretDPAPIToRegistry(entry.Path, entry.Key, value)
	}

	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, entry.Path, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("UpsertEntry error: %w", err)
	}
	defer baseKey.Close()

	return baseKey.SetStringValue(entry.Key, value)
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
		return nil
	}
	defer baseKey.Close()

	return baseKey.DeleteValue(key)
}
