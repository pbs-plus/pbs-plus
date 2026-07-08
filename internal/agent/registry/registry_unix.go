//go:build unix

package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gofrs/flock"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	registryDir      = "/etc/pbs-plus-agent"
	registryFilePath = "/etc/pbs-plus-agent/registry.toml"
	lockFilePath     = "/etc/pbs-plus-agent/registry.lock"
	keyFile          = "/etc/pbs-plus-agent/.registry.key"

	legacyRegistryBasePath = "/etc/pbs-plus-agent/registry"
	legacyKeyFile          = "/etc/pbs-plus-agent/registry/.key"
	valueFileSuffix        = ".value"
	metaFileName           = ".index.json"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

type RegistryEntry struct {
	Path     string
	Key      string
	Value    string
	IsSecret bool
}

type fullRegistry map[string]map[string]string

type legacyData struct {
	Values map[string]string `json:"values"`
}

func init() {
	if err := os.MkdirAll(registryDir, 0o755); err != nil {
		log.Error(err, "")
	}
	if err := migrateLegacy(); err != nil {
		log.Error(err, "")
	}
}

func withLock(readOnly bool, fn func() error) error {
	f := flock.New(lockFilePath)
	if readOnly {
		if err := f.RLock(); err != nil {
			return err
		}
	} else {
		if err := f.Lock(); err != nil {
			return err
		}
	}
	defer func() {
		if err := f.Unlock(); err != nil {
			log.Error(err, "")
		}
	}()
	return fn()
}

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
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

func lcPath(p string) string {
	if p == "" {
		return "root"
	}
	// Replace Windows backslashes with forward slashes for a unified Linux feel
	p = strings.ReplaceAll(p, "\\", "/")
	return strings.ToLower(strings.Trim(p, "/"))
}

func toTomlKey(k string) string {
	return toSnakeCase(k)
}

var registryKeyMgr = crypto.NewKeyManager(keyFile)

func initKeyManager() {
	registryKeyMgr = crypto.NewKeyManager(keyFile)
}

func getEncryptionKey() ([]byte, error) {
	if keyData, err := os.ReadFile(keyFile); err == nil && len(keyData) == 32 {
		return keyData, nil
	}
	if keyData, err := os.ReadFile(legacyKeyFile); err == nil && len(keyData) == 32 {
		if err := writeFileAtomic(keyFile, keyData, 0o600); err != nil {
			log.Error(err, "")
		}
		return keyData, nil
	}
	return registryKeyMgr.GetOrCreate()
}

func encrypt(plaintext string) (string, error) {
	key, err := getEncryptionKey()
	if err != nil {
		return "", err
	}
	return crypto.EncryptWithKey(plaintext, key)
}

func decrypt(ciphertext string) (string, error) {
	key, err := getEncryptionKey()
	if err != nil {
		return "", err
	}
	return crypto.DecryptWithKey(ciphertext, key)
}

func loadRegistry() (fullRegistry, error) {
	reg := make(fullRegistry)
	_, err := toml.DecodeFile(registryFilePath, &reg)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return reg, nil
		}
		return nil, err
	}
	return reg, nil
}

func saveRegistry(reg fullRegistry) error {
	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(reg); err != nil {
		return err
	}
	return writeFileAtomic(registryFilePath, buf.Bytes(), 0o600)
}

func writeFileAtomic(dst string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Error(err, "")
	}
	tmp, err := os.CreateTemp(dir, ".tmp-reg-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	success := false
	defer func() {
		if err := tmp.Close(); err != nil {
			log.Error(err, "")
		}
		if !success {
			if err := os.Remove(tmpName); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
			}
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		log.Error(err, "")
	}
	if err := tmp.Sync(); err != nil {
		log.Error(err, "")
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return err
	}
	success = true
	return nil
}

func migrateLegacy() error {
	if _, err := os.Stat(registryFilePath); err == nil {
		return nil
	}
	if _, err := os.Stat(legacyRegistryBasePath); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return withLock(false, func() error {
		if _, err := os.Stat(registryFilePath); err == nil {
			return nil
		}
		reg := make(fullRegistry)
		if err := filepath.Walk(legacyRegistryBasePath, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if strings.HasSuffix(info.Name(), ".json") && info.Name() != metaFileName {
				p := strings.TrimSuffix(info.Name(), ".json")
				b, err := os.ReadFile(path)
				if err != nil {
					log.Error(err, "")
				}
				var ld legacyData
				if err := json.Unmarshal(b, &ld); err == nil {
					pathKey := lcPath(p)
					if reg[pathKey] == nil {
						reg[pathKey] = make(map[string]string)
					}
					for k, v := range ld.Values {
						reg[pathKey][toTomlKey(k)] = v
					}
				}
			}
			if strings.HasSuffix(info.Name(), valueFileSuffix) {
				rel, err := filepath.Rel(legacyRegistryBasePath, filepath.Dir(path))
				if err != nil {
					log.Error(err, "")
				}
				pathKey := lcPath(rel)
				keyName := strings.TrimSuffix(info.Name(), valueFileSuffix)
				if reg[pathKey] == nil {
					reg[pathKey] = make(map[string]string)
				}
				b, err := os.ReadFile(path)
				if err != nil {
					log.Error(err, "")
				}
				reg[pathKey][toTomlKey(keyName)] = string(b)
			}
			return nil
		}); err != nil {
			log.Error(err, "")
		}
		if len(reg) > 0 {
			return saveRegistry(reg)
		}
		return nil
	})
}

func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	var entry *RegistryEntry
	err := withLock(true, func() error {
		reg, err := loadRegistry()
		if err != nil {
			return err
		}
		p := lcPath(path)
		k := toTomlKey(key)
		section, ok := reg[p]
		if !ok {
			return fmt.Errorf("GetEntry error: path not found")
		}
		val, ok := section[k]
		if !ok {
			return fmt.Errorf("GetEntry error: key not found")
		}
		if isSecret {
			decrypted, err := decrypt(val)
			if err != nil {
				return fmt.Errorf("GetEntry error: %w", err)
			}
			val = decrypted
		}
		entry = &RegistryEntry{
			Path:     path,
			Key:      key,
			Value:    val,
			IsSecret: isSecret,
		}
		return nil
	})
	return entry, err
}

func CreateEntry(entry *RegistryEntry) error {
	return withLock(false, func() error {
		reg, err := loadRegistry()
		if err != nil {
			return err
		}
		p := lcPath(entry.Path)
		k := toTomlKey(entry.Key)
		if section, ok := reg[p]; ok {
			if _, exists := section[k]; exists {
				return fmt.Errorf("CreateEntry error: key already exists")
			}
		}
		value := preprocessValue(entry.Value, entry.IsSecret)
		if entry.IsSecret {
			enc, err := encrypt(value)
			if err != nil {
				return fmt.Errorf("CreateEntry error encrypting: %w", err)
			}
			value = enc
		}
		if reg[p] == nil {
			reg[p] = make(map[string]string)
		}
		reg[p][k] = value
		return saveRegistry(reg)
	})
}

func UpsertEntry(entry *RegistryEntry) error {
	return withLock(false, func() error {
		reg, err := loadRegistry()
		if err != nil {
			return err
		}
		p := lcPath(entry.Path)
		k := toTomlKey(entry.Key)
		value := preprocessValue(entry.Value, entry.IsSecret)
		if entry.IsSecret {
			enc, err := encrypt(value)
			if err != nil {
				return fmt.Errorf("UpsertEntry error encrypting: %w", err)
			}
			value = enc
		}
		if reg[p] == nil {
			reg[p] = make(map[string]string)
		}
		reg[p][k] = value
		return saveRegistry(reg)
	})
}

func CreateEntryIfNotExists(entry *RegistryEntry) error {
	return withLock(false, func() error {
		reg, err := loadRegistry()
		if err != nil {
			return err
		}
		p := lcPath(entry.Path)
		k := toTomlKey(entry.Key)
		if reg[p] != nil {
			if _, ok := reg[p][k]; ok {
				return nil
			}
		}
		value := preprocessValue(entry.Value, entry.IsSecret)
		if entry.IsSecret {
			enc, err := encrypt(value)
			if err != nil {
				return err
			}
			value = enc
		}
		if reg[p] == nil {
			reg[p] = make(map[string]string)
		}
		reg[p][k] = value
		return saveRegistry(reg)
	})
}

func DeleteEntry(path string, key string) error {
	return withLock(false, func() error {
		reg, err := loadRegistry()
		if err != nil {
			return err
		}
		p := lcPath(path)
		k := toTomlKey(key)
		if reg[p] != nil {
			delete(reg[p], k)
			if len(reg[p]) == 0 {
				delete(reg, p)
			}
		}
		return saveRegistry(reg)
	})
}
