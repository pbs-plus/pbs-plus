package crypto

import (
	"crypto/tls"
	"testing"
)

func TestSealUnseal(t *testing.T) {
	tmpDir := t.TempDir()
	SetSealKeyPath(tmpDir + "/test-seal.key")

	plaintext := "hello world, this is a secret!"
	sealed, err := Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	unsealed, err := Unseal(sealed)
	if err != nil {
		t.Fatalf("Unseal: %v", err)
	}

	if unsealed != plaintext {
		t.Errorf("expected %q, got %q", plaintext, unsealed)
	}
}

func TestSealProducesDifferentCiphertexts(t *testing.T) {
	tmpDir := t.TempDir()
	SetSealKeyPath(tmpDir + "/test-seal.key")

	plaintext := "same input"
	c1, err := Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal 1: %v", err)
	}
	c2, err := Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal 2: %v", err)
	}
	if c1 == c2 {
		t.Error("encrypting the same plaintext twice should produce different ciphertexts due to random nonce")
	}
}

func TestUnsealWrongKey(t *testing.T) {
	dir1 := t.TempDir()
	SetSealKeyPath(dir1 + "/key1.key")
	sealed, err := Seal("secret data")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	dir2 := t.TempDir()
	SetSealKeyPath(dir2 + "/key2.key")
	_, err = Unseal(sealed)
	if err == nil {
		t.Error("expected error when unsealing with wrong key")
	}
}

func TestEncryptDecryptWithKey(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	plaintext := "test data 12345"
	ciphertext, err := EncryptWithKey(plaintext, key)
	if err != nil {
		t.Fatalf("EncryptWithKey: %v", err)
	}

	decrypted, err := DecryptWithKey(ciphertext, key)
	if err != nil {
		t.Fatalf("DecryptWithKey: %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("expected %q, got %q", plaintext, decrypted)
	}
}

func TestEncryptWithKeyDifferentNonces(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	c1, _ := EncryptWithKey("test", key)
	c2, _ := EncryptWithKey("test", key)
	if c1 == c2 {
		t.Error("same plaintext should produce different ciphertexts")
	}
}

func TestDecryptWithKeyCorrupted(t *testing.T) {
	key := make([]byte, 32)
	ciphertext, err := EncryptWithKey("hello", key)
	if err != nil {
		t.Fatalf("EncryptWithKey: %v", err)
	}

	corrupted := ciphertext[:len(ciphertext)-4] + "XXXX"
	_, err = DecryptWithKey(corrupted, key)
	if err == nil {
		t.Error("expected error decrypting corrupted data")
	}
}

func TestHMACSHA256(t *testing.T) {
	key := []byte("test-key")
	data := []byte("test-data")

	mac := HMACSHA256(key, data)
	if len(mac) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(mac))
	}

	if !VerifyHMACSHA256(key, data, mac) {
		t.Error("HMAC verification failed")
	}

	wrongKey := []byte("wrong-key")
	if VerifyHMACSHA256(wrongKey, data, mac) {
		t.Error("HMAC should not verify with wrong key")
	}
}

func TestSHA256(t *testing.T) {
	data := []byte("hello world")
	sum := SHA256(data)
	if len(sum) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(sum))
	}

	hex := SHA256Hex(data)
	if len(hex) != 64 {
		t.Errorf("expected 64 hex chars, got %d", len(hex))
	}
}

func TestSecureRandomBytes(t *testing.T) {
	b1, err := SecureRandomBytes(32)
	if err != nil {
		t.Fatalf("SecureRandomBytes: %v", err)
	}
	if len(b1) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(b1))
	}

	b2, err := SecureRandomBytes(32)
	if err != nil {
		t.Fatalf("SecureRandomBytes: %v", err)
	}

	allSame := true
	for i := range b1 {
		if b1[i] != b2[i] {
			allSame = false
			break
		}
	}
	if allSame {
		t.Error("two random byte sequences should differ")
	}
}

func TestSecureRandomString(t *testing.T) {
	s, err := SecureRandomString(32)
	if err != nil {
		t.Fatalf("SecureRandomString: %v", err)
	}
	if len(s) == 0 {
		t.Error("expected non-empty string")
	}
}

func TestTokenManager(t *testing.T) {
	tm, err := NewTokenManager(TokenConfig{
		SecretKey:       "test-secret-key-that-is-long-enough",
		TokenExpiration: 0,
	})
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}

	token, err := tm.GenerateToken(0)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if token == "" {
		t.Error("expected non-empty token")
	}

	if err := tm.ValidateToken(token); err != nil {
		t.Errorf("ValidateToken: %v", err)
	}

	remaining := tm.GetTokenRemainingDuration(token)
	if remaining != -1 {
		t.Errorf("expected -1 for token without expiry, got %v", remaining)
	}
}

func TestTokenManagerWithExpiry(t *testing.T) {
	tm, err := NewTokenManager(TokenConfig{
		SecretKey:       "test-secret-key-that-is-long-enough",
		TokenExpiration: 3600000000000,
	})
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}

	token, err := tm.GenerateToken(3600000000000)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	if err := tm.ValidateToken(token); err != nil {
		t.Errorf("ValidateToken: %v", err)
	}

	remaining := tm.GetTokenRemainingDuration(token)
	if remaining <= 0 {
		t.Error("expected positive remaining duration")
	}
}

func TestTokenManagerInvalidToken(t *testing.T) {
	tm, err := NewTokenManager(TokenConfig{
		SecretKey: "test-secret-key-that-is-long-enough",
	})
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}

	if err := tm.ValidateToken("invalid-token-data"); err == nil {
		t.Error("expected error validating invalid token")
	}
}

func TestTokenManagerWrongKey(t *testing.T) {
	tm1, _ := NewTokenManager(TokenConfig{SecretKey: "key-one-is-here-now"})
	tm2, _ := NewTokenManager(TokenConfig{SecretKey: "key-two-is-different"})

	token, _ := tm1.GenerateToken(0)
	if err := tm2.ValidateToken(token); err == nil {
		t.Error("expected validation failure with wrong key")
	}
}

func TestKeyManager(t *testing.T) {
	tmpDir := t.TempDir()
	km := NewKeyManager(tmpDir + "/test.key")

	key1, err := km.GetOrCreate()
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if len(key1) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key1))
	}

	key2, err := km.GetOrCreate()
	if err != nil {
		t.Fatalf("GetOrCreate 2: %v", err)
	}

	allSame := true
	for i := range key1 {
		if key1[i] != key2[i] {
			allSame = false
			break
		}
	}
	if !allSame {
		t.Error("key should be deterministic across calls")
	}
}

func TestFIPSServerTLSConfig(t *testing.T) {
	cfg := FIPSServerTLSConfig()
	if cfg.MinVersion != uint16(tls.VersionTLS12) {
		t.Errorf("expected MinVersion TLS 1.2, got %d", cfg.MinVersion)
	}
	if !cfg.PreferServerCipherSuites {
		t.Error("expected PreferServerCipherSuites to be true")
	}
	if len(cfg.CipherSuites) == 0 {
		t.Error("expected non-empty cipher suites")
	}
}

func TestFIPSClientTLSConfig(t *testing.T) {
	cfg := FIPSClientTLSConfig(uint16(tls.VersionTLS12))
	if cfg.MinVersion != uint16(tls.VersionTLS12) {
		t.Errorf("expected MinVersion TLS 1.2, got %d", cfg.MinVersion)
	}
	if len(cfg.CipherSuites) == 0 {
		t.Error("expected non-empty cipher suites")
	}
}

func TestNaclMigrationFallback(t *testing.T) {
	tmpDir := t.TempDir()
	sealPath := tmpDir + "/seal.key"

	origSealKeyFile := sealKeyFile
	sealKeyFile = sealPath
	defer func() { sealKeyFile = origSealKeyFile }()

	_ = origSealKeyFile

	aesKey, err := getOrCreateSealKey()
	if err != nil {
		t.Fatalf("getOrCreateSealKey: %v", err)
	}
	if len(aesKey) != 32 {
		t.Fatalf("expected 32-byte key, got %d", len(aesKey))
	}

	plaintext := "migrated nacl secret"
	aesSealed, err := EncryptWithKey(plaintext, aesKey)
	if err != nil {
		t.Fatalf("EncryptWithKey: %v", err)
	}

	result, err := Unseal(aesSealed)
	if err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if result != plaintext {
		t.Errorf("expected %q, got %q", plaintext, result)
	}
}
