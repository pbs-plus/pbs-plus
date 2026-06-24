package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/crypto/nacl/box"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	naclKeySize   = 64
	aesKeySize    = 32
	naclNonceSize = 24
	aesNonceSize  = 12
	naclLegacyExt = ".nacl.key"
	migratedExt   = ".migrated"
)

var sealMu sync.Mutex
var sealKeyFile string

func SetSealKeyPath(p string) {
	sealKeyFile = p
}

func getSealKeyPath() string {
	if sealKeyFile != "" {
		return sealKeyFile
	}
	return "/var/lib/pbs-plus/.seal.key"
}

func naclKeyPath() string {
	return getSealKeyPath() + naclLegacyExt
}

func migratedMarkerPath() string {
	return getSealKeyPath() + migratedExt
}

func getOrCreateSealKey() ([]byte, error) {
	path := getSealKeyPath()
	if data, err := os.ReadFile(path); err == nil && len(data) == aesKeySize {
		return data, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("crypto: create seal key dir: %w", err)
	}

	key := make([]byte, aesKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("crypto: generate seal key: %w", err)
	}
	if err := os.WriteFile(path, key, 0o600); err != nil {
		return nil, fmt.Errorf("crypto: write seal key: %w", err)
	}
	return key, nil
}

func Seal(plaintext string) (string, error) {
	sealMu.Lock()
	defer sealMu.Unlock()

	key, err := getOrCreateSealKey()
	if err != nil {
		return "", err
	}
	return EncryptWithKey(plaintext, key)
}

func Unseal(ciphertext string) (string, error) {
	sealMu.Lock()
	defer sealMu.Unlock()

	aesKey, err := getOrCreateSealKey()
	if err != nil {
		return "", err
	}

	pt, err := DecryptWithKey(ciphertext, aesKey)
	if err == nil {
		return pt, nil
	}

	naclPub, naclPriv, naclErr := loadNaclKeys()
	if naclErr != nil {
		return "", fmt.Errorf("crypto: aes decrypt failed: %w; nacl fallback unavailable: %v", err, naclErr)
	}

	pt, naclDecryptErr := naclBoxDecrypt(ciphertext, naclPub, naclPriv)
	if naclDecryptErr != nil {
		return "", fmt.Errorf("crypto: aes decrypt: %w; nacl decrypt: %v", err, naclDecryptErr)
	}

	reencrypted, reencErr := EncryptWithKey(pt, aesKey)
	if reencErr != nil {
		syslog.L.Error(reencErr).WithMessage("crypto: failed to re-encrypt migrated secret").Write()
		return pt, nil
	}

	_ = reencrypted
	syslog.L.Info().WithMessage("crypto: migrated secret from nacl-box to aes-256-gcm").Write()
	return pt, nil
}

func loadNaclKeys() (*[32]byte, *[32]byte, error) {
	nkPath := naclKeyPath()
	data, err := os.ReadFile(nkPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read nacl keys: %w", err)
	}
	if len(data) != naclKeySize {
		return nil, nil, fmt.Errorf("nacl key file is %d bytes, expected %d", len(data), naclKeySize)
	}
	pub := new([32]byte)
	priv := new([32]byte)
	copy(pub[:], data[:32])
	copy(priv[:], data[32:])
	return pub, priv, nil
}

func naclBoxDecrypt(ciphertext string, pub, priv *[32]byte) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("crypto: nacl base64 decode: %w", err)
	}
	if len(data) < naclNonceSize {
		return "", errors.New("crypto: nacl ciphertext too short")
	}
	var nonce [24]byte
	copy(nonce[:], data[:naclNonceSize])
	decrypted, ok := box.Open(nil, data[naclNonceSize:], &nonce, pub, priv)
	if !ok {
		return "", errors.New("crypto: nacl box open failed")
	}
	return string(decrypted), nil
}

func IsMigrated() bool {
	_, err := os.Stat(migratedMarkerPath())
	return err == nil
}

func NaclKeyExists() bool {
	_, err := os.Stat(naclKeyPath())
	return err == nil
}

func MigrateNaclKeyIfExists() error {
	sealPath := getSealKeyPath()
	nkPath := naclKeyPath()

	if _, err := os.Stat(nkPath); err == nil {
		return nil
	}

	data, err := os.ReadFile(sealPath)
	if err != nil {
		return nil
	}

	if len(data) != naclKeySize {
		return nil
	}

	if err := os.WriteFile(nkPath, data, 0o600); err != nil {
		return fmt.Errorf("crypto: backup nacl key: %w", err)
	}
	syslog.L.Info().WithMessage("crypto: backed up nacl-box key for migration").Write()

	key := make([]byte, aesKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return fmt.Errorf("crypto: generate new aes key: %w", err)
	}
	if err := os.WriteFile(sealPath, key, 0o600); err != nil {
		return fmt.Errorf("crypto: write new aes key: %w", err)
	}
	syslog.L.Info().WithMessage("crypto: replaced nacl-box key with aes-256-gcm key").Write()
	return nil
}

func MarkMigrated() error {
	markerPath := migratedMarkerPath()
	if err := os.WriteFile(markerPath, []byte("migrated"), 0o600); err != nil {
		return fmt.Errorf("crypto: write migration marker: %w", err)
	}
	return nil
}

func TryDecryptNacl(ciphertext string) (string, error) {
	naclPub, naclPriv, err := loadNaclKeys()
	if err != nil {
		return "", err
	}
	return naclBoxDecrypt(ciphertext, naclPub, naclPriv)
}
