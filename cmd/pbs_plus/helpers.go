package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func GenerateSecretKey(length int) (string, error) {
	keyBytes := make([]byte, length)
	if _, err := rand.Read(keyBytes); err != nil {
		return "", fmt.Errorf("failed to read random bytes: %w", err)
	}
	return base64.URLEncoding.EncodeToString(keyBytes), nil
}
