package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

func CalculateDigest(data any) (string, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("CalculateDigest: failed to marshal data to JSON -> %w", err)
	}

	if string(jsonData) == "[]" || string(jsonData) == "{}" {
		jsonData = []byte{}
	}

	hash := sha256.New()

	hash.Write(jsonData)

	digest := hash.Sum(nil)

	return hex.EncodeToString(digest), nil
}
