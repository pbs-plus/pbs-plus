package database

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/crypto/nacl/box"
)

var (
	publicKey  *[32]byte
	privateKey *[32]byte
	once       sync.Once
	initErr    error
)

// ensureInitialized performs one-time initialization of the secret keys.
// Call this at the start of any exported function that needs the keys.
func ensureInitialized() {
	once.Do(func() {
		initErr = loadOrCreateKey()
		if initErr != nil {
			syslog.L.Error(initErr).WithMessage("failed to initialize database secret store").Write()
		}
	})
}

func loadOrCreateKey() error {
	if err := os.MkdirAll(filepath.Dir(conf.SecretsKeyPath), 0700); err != nil {
		return err
	}

	if data, err := os.ReadFile(conf.SecretsKeyPath); err == nil {
		if len(data) != 64 {
			return errors.New("invalid key file")
		}
		publicKey = new([32]byte)
		privateKey = new([32]byte)
		copy(publicKey[:], data[:32])
		copy(privateKey[:], data[32:])
		return nil
	}

	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}

	publicKey = pub
	privateKey = priv

	keyData := append(pub[:], priv[:]...)
	if err := os.WriteFile(conf.SecretsKeyPath, keyData, 0600); err != nil {
		return err
	}

	return nil
}

// Encrypt takes a plaintext string and returns a base64 ciphertext
func Encrypt(plaintext string) (string, error) {
	ensureInitialized()
	if initErr != nil {
		return "", initErr
	}
	if privateKey == nil || publicKey == nil {
		return "", errors.New("failed to acquire database private and public keys")
	}

	var nonce [24]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return "", err
	}

	// Using our own public key as both sender and receiver
	encrypted := box.Seal(nonce[:], []byte(plaintext), &nonce, publicKey, privateKey)

	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func Decrypt(ciphertext string) (string, error) {
	ensureInitialized()
	if initErr != nil {
		return "", initErr
	}
	if privateKey == nil || publicKey == nil {
		return "", errors.New("failed to acquire database private and public keys")
	}

	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	if len(data) < 24 {
		return "", errors.New("ciphertext too short")
	}

	var nonce [24]byte
	copy(nonce[:], data[:24])

	decrypted, ok := box.Open(nil, data[24:], &nonce, publicKey, privateKey)
	if !ok {
		return "", errors.New("decryption failed")
	}

	return string(decrypted), nil
}
