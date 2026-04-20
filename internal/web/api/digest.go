//go:build linux

package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// calculateDigest returns the SHA-256 hex digest of the JSON encoding of data.
func calculateDigest(data any) (string, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("calculateDigest: failed to marshal data to JSON -> %w", err)
	}

	if string(jsonData) == "[]" || string(jsonData) == "{}" {
		jsonData = []byte{}
	}

	hash := sha256.Sum256(jsonData)
	return hex.EncodeToString(hash[:]), nil
}
