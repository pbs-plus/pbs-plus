//go:build unix

package registry

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	registryBasePath = "/etc/pbs-plus-agent/registry"
	keyFile          = "/etc/pbs-plus-agent/registry/.key"

	valueFileSuffix = ".value"
	metaFileName    = ".index.json"
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

func init() {
	_ = ensureRegistryDir()
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

func ensureRegistryDir() error {
	return os.MkdirAll(registryBasePath, 0o755)
}

// Lowercase helpers

func lcPath(p string) string {
	if p == "" {
		return ""
	}
	p = strings.ReplaceAll(p, "\\", "/")
	return strings.ToLower(p)
}

func lcKey(k string) string {
	return strings.ToLower(k)
}

func getRegistryDirPath(path string) string {
	return filepath.Join(registryBasePath, lcPath(path))
}

func getLegacyRegistryFilePath(path string) string {
	return filepath.Join(registryBasePath, lcPath(path)+".json")
}

func getValueFilePath(path, key string) string {
	return filepath.Join(getRegistryDirPath(path), lcKey(key)+valueFileSuffix)
}

func getMetaFilePath(path string) string {
	return filepath.Join(getRegistryDirPath(path), metaFileName)
}

func getEncryptionKey() ([]byte, error) {
	if err := ensureRegistryDir(); err != nil {
		return nil, err
	}
	if keyData, err := os.ReadFile(keyFile); err == nil && len(keyData) == 32 {
		return keyData, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate encryption key: %w", err)
	}
	if err := writeFileAtomic(keyFile, key, 0o600); err != nil {
		return nil, fmt.Errorf("failed to save encryption key: %w", err)
	}
	return key, nil
}

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
	ns := gcm.NonceSize()
	if len(data) < ns {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce, ct := data[:ns], data[ns:]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

func loadLegacyRegistryFile(filePath string) (*registryData, error) {
	data := &registryData{Values: make(map[string]string)}
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return data, nil
	}
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	if len(fileData) == 0 {
		return data, nil
	}
	if err := json.Unmarshal(fileData, data); err != nil {
		return nil, err
	}
	return data, nil
}

func saveLegacyRegistryFile(filePath string, data *registryData) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	fileData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filePath, fileData, 0o644)
}

func writeFileAtomic(dst string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Migration: legacy JSON -> lowercase directory + lowercase key filenames
func migrateIfNeeded(path string) error {
	dir := getRegistryDirPath(path)
	// If index present, assume migrated
	if _, err := os.Stat(getMetaFilePath(path)); err == nil {
		return nil
	}
	legacy := getLegacyRegistryFilePath(path)
	legacyData, err := loadLegacyRegistryFile(legacy)
	if err != nil {
		return err
	}
	if len(legacyData.Values) == 0 {
		// No legacy content; ensure dir exists for new layout
		return os.MkdirAll(dir, 0o755)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Write each value to lowercase key file
	for k, v := range legacyData.Values {
		fp := getValueFilePath(path, k) // lcKey happens inside
		if err := writeFileAtomic(fp, []byte(v), 0o640); err != nil {
			return fmt.Errorf("migrate write %s: %w", k, err)
		}
	}
	// Write index with lowercase keys
	index := &registryData{Values: make(map[string]string, len(legacyData.Values))}
	for k := range legacyData.Values {
		index.Values[lcKey(k)] = ""
	}
	indexBytes, _ := json.MarshalIndent(index, "", "  ")
	if err := writeFileAtomic(getMetaFilePath(path), indexBytes, 0o644); err != nil {
		return err
	}
	// Keep legacy file for backward readability
	return nil
}

func readValueFile(path, key string) (string, error) {
	fp := getValueFilePath(path, key) // lc key inside
	b, err := os.ReadFile(fp)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func writeValueFile(path, key, value string) error {
	fp := getValueFilePath(path, key)
	return writeFileAtomic(fp, []byte(value), 0o640)
}

func deleteValueFile(path, key string) error {
	fp := getValueFilePath(path, key)
	err := os.Remove(fp)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func listKeysNewLayout(path string) ([]string, error) {
	dir := getRegistryDirPath(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, err
	}
	var keys []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, valueFileSuffix) && name != metaFileName {
			keys = append(keys, strings.TrimSuffix(name, valueFileSuffix))
		}
	}
	return keys, nil
}

func readValueWithFallback(path, key string) (string, bool, error) {
	// Normalize key to lowercase
	key = lcKey(key)
	// Try new layout first
	v, err := readValueFile(path, key)
	if err == nil {
		return v, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	// Legacy JSON fallback (key lookup is case-insensitive by lowercasing keys)
	legacy := getLegacyRegistryFilePath(path)
	data, lerr := loadLegacyRegistryFile(legacy)
	if lerr != nil {
		return "", false, lerr
	}
	// Build lowercase map view
	for k, v := range data.Values {
		if lcKey(k) == key {
			return v, false, nil
		}
	}
	return "", false, os.ErrNotExist
}

func GetEntry(path string, key string, isSecret bool) (*RegistryEntry, error) {
	path = lcPath(path)
	keyOrig := key
	key = lcKey(key)

	if err := migrateIfNeeded(path); err != nil {
		// continue best-effort
	}

	value, fromNew, err := readValueWithFallback(path, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("GetEntry error: key not found")
		}
		return nil, fmt.Errorf("GetEntry error: %w", err)
	}

	if isSecret {
		decrypted, derr := decrypt(value)
		if derr != nil {
			return nil, fmt.Errorf("GetEntry error: %w", derr)
		}
		value = decrypted
	}

	// Opportunistic per-key write if came from legacy
	if !fromNew {
		_ = writeValueFile(path, key, value)
		_ = updateIndexForKey(path, key, true)
	}

	return &RegistryEntry{
		Path:     path,    // path is already lowercase
		Key:      keyOrig, // preserve caller's key in the return struct
		Value:    value,
		IsSecret: isSecret,
	}, nil
}

func CreateEntry(entry *RegistryEntry) error {
	path := lcPath(entry.Path)
	key := lcKey(entry.Key)

	value := preprocessValue(entry.Value, entry.IsSecret)
	if entry.IsSecret {
		enc, err := encrypt(value)
		if err != nil {
			return fmt.Errorf("CreateEntry error encrypting: %w", err)
		}
		value = enc
	}

	if _, _, err := readValueWithFallback(path, key); err == nil {
		return fmt.Errorf("CreateEntry error: key already exists")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("CreateEntry error: %w", err)
	}

	if err := writeValueFile(path, key, value); err != nil {
		return fmt.Errorf("CreateEntry error saving: %w", err)
	}
	if err := updateIndexForKey(path, key, true); err != nil {
		return fmt.Errorf("CreateEntry error indexing: %w", err)
	}

	// Keep legacy JSON in sync (with original or lowercase? We'll write lowercase keys)
	if err := legacyUpsert(path, key, value); err != nil {
		return fmt.Errorf("CreateEntry error legacy save: %w", err)
	}

	return nil
}

func UpdateEntry(entry *RegistryEntry) error {
	path := lcPath(entry.Path)
	key := lcKey(entry.Key)

	if _, _, err := readValueWithFallback(path, key); err != nil {
		return fmt.Errorf("UpdateEntry error: entry does not exist: %w", err)
	}

	value := preprocessValue(entry.Value, entry.IsSecret)
	if entry.IsSecret {
		enc, err := encrypt(value)
		if err != nil {
			return fmt.Errorf("UpdateEntry error encrypting: %w", err)
		}
		value = enc
	}

	if err := writeValueFile(path, key, value); err != nil {
		return fmt.Errorf("UpdateEntry error saving: %w", err)
	}
	if err := updateIndexForKey(path, key, true); err != nil {
		return fmt.Errorf("UpdateEntry error indexing: %w", err)
	}

	if err := legacyUpsert(path, key, value); err != nil {
		return fmt.Errorf("UpdateEntry error legacy save: %w", err)
	}

	return nil
}

func CreateEntryIfNotExists(entry *RegistryEntry) error {
	path := lcPath(entry.Path)
	key := lcKey(entry.Key)

	if _, _, err := readValueWithFallback(path, key); err == nil {
		return fmt.Errorf("CreateEntryIfNotExists error: entry already exists")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("CreateEntryIfNotExists error: %w", err)
	}
	return CreateEntry(&RegistryEntry{
		Path:     path,
		Key:      key,
		Value:    entry.Value,
		IsSecret: entry.IsSecret,
	})
}

func DeleteEntry(path string, key string) error {
	path = lcPath(path)
	key = lcKey(key)

	if err := deleteValueFile(path, key); err != nil {
		return fmt.Errorf("DeleteEntry error: %w", err)
	}
	if err := updateIndexForKey(path, key, false); err != nil {
		return fmt.Errorf("DeleteEntry error indexing: %w", err)
	}
	if err := legacyDelete(path, key); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("DeleteEntry error legacy: %w", err)
	}
	return nil
}

func DeleteKey(path string) error {
	path = lcPath(path)
	dir := getRegistryDirPath(path)
	if err := os.RemoveAll(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("DeleteKey error: %w", err)
	}
	legacy := getLegacyRegistryFilePath(path)
	if err := os.Remove(legacy); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("DeleteKey error: %w", err)
	}
	return nil
}

func ListEntries(path string) ([]string, error) {
	path = lcPath(path)
	if err := migrateIfNeeded(path); err != nil {
		// best-effort
	}
	keys, err := listKeysNewLayout(path)
	if err == nil && len(keys) > 0 {
		return keys, nil
	}
	legacy := getLegacyRegistryFilePath(path)
	data, lerr := loadLegacyRegistryFile(legacy)
	if lerr != nil {
		return nil, fmt.Errorf("ListEntries error: %w", lerr)
	}
	// Return lowercase keys
	var out []string
	for k := range data.Values {
		out = append(out, lcKey(k))
	}
	return out, nil
}

func updateIndexForKey(path, key string, present bool) error {
	path = lcPath(path)
	key = lcKey(key)

	idxPath := getMetaFilePath(path)
	data := &registryData{Values: map[string]string{}}
	if b, err := os.ReadFile(idxPath); err == nil && len(b) > 0 {
		_ = json.Unmarshal(b, data)
	}
	if present {
		data.Values[key] = ""
	} else {
		delete(data.Values, key)
	}
	blob, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(idxPath, blob, 0o644)
}

func legacyUpsert(path, key, value string) error {
	filePath := getLegacyRegistryFilePath(path)
	data, err := loadLegacyRegistryFile(filePath)
	if err != nil {
		return err
	}
	if data.Values == nil {
		data.Values = map[string]string{}
	}
	// store by lowercase key
	data.Values[lcKey(key)] = value
	return saveLegacyRegistryFile(filePath, data)
}

func legacyDelete(path, key string) error {
	filePath := getLegacyRegistryFilePath(path)
	data, err := loadLegacyRegistryFile(filePath)
	if err != nil {
		return err
	}
	if data.Values == nil {
		return os.ErrNotExist
	}
	lk := lcKey(key)
	if _, ok := data.Values[lk]; !ok {
		// try to find any variant and delete it
		found := false
		for k := range data.Values {
			if lcKey(k) == lk {
				delete(data.Values, k)
				found = true
				break
			}
		}
		if !found {
			return os.ErrNotExist
		}
	} else {
		delete(data.Values, lk)
	}
	if len(data.Values) == 0 {
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return saveLegacyRegistryFile(filePath, data)
}
