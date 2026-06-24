package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const aes256KeySize = 32
const aesGCMNonceSize = 12

type KeyManager struct {
	mu      sync.Mutex
	keyFile string
}

func NewKeyManager(keyFile string) *KeyManager {
	return &KeyManager{keyFile: keyFile}
}

func (km *KeyManager) GetOrCreate() ([]byte, error) {
	km.mu.Lock()
	defer km.mu.Unlock()
	return km.loadOrGenerate()
}

func (km *KeyManager) loadOrGenerate() ([]byte, error) {
	if data, err := os.ReadFile(km.keyFile); err == nil && len(data) == aes256KeySize {
		return data, nil
	}
	key := make([]byte, aes256KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("crypto: generate key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(km.keyFile), 0o700); err != nil {
		return nil, fmt.Errorf("crypto: create key dir: %w", err)
	}
	if err := os.WriteFile(km.keyFile, key, 0o600); err != nil {
		return nil, fmt.Errorf("crypto: write key: %w", err)
	}
	return key, nil
}

func EncryptWithKey(plaintext string, key []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("crypto: create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("crypto: create gcm: %w", err)
	}
	nonce := make([]byte, aesGCMNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("crypto: generate nonce: %w", err)
	}
	ct := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ct), nil
}

func DecryptWithKey(ciphertext string, key []byte) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("crypto: decode base64: %w", err)
	}
	if len(data) < aesGCMNonceSize {
		return "", fmt.Errorf("crypto: ciphertext too short")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("crypto: create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("crypto: create gcm: %w", err)
	}
	nonce, ct := data[:aesGCMNonceSize], data[aesGCMNonceSize:]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", fmt.Errorf("crypto: decrypt: %w", err)
	}
	return string(pt), nil
}
