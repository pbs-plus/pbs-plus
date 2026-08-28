package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

func SecureRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return nil, fmt.Errorf("crypto: generate random bytes: %w", err)
	}
	return b, nil
}

func SecureRandomString(n int) (string, error) {
	b, err := SecureRandomBytes(n)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func SecureRandomInt(max int64) (int64, error) {
	if max <= 0 {
		return 0, fmt.Errorf("crypto: SecureRandomInt: max must be positive")
	}
	n := (max-1)/256 + 1
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return 0, fmt.Errorf("crypto: SecureRandomInt: %w", err)
	}
	var result int64
	for _, by := range b {
		result = (result*256 + int64(by)) % max
	}
	return result, nil
}
