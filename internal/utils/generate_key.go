package utils

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func GenerateSecretKey(length int) (string, error) {
	keyBytes := make([]byte, length)
	_, err := rand.Read(keyBytes)
	if err != nil {
		return "", fmt.Errorf("failed to read random bytes: %w", err)
	}

	encodedKey := base64.URLEncoding.EncodeToString(keyBytes)
	return encodedKey, nil
}
