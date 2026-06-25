package api

import (
	"encoding/json"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
)

func calculateDigest(data any) (string, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("calculateDigest: failed to marshal data to JSON -> %w", err)
	}

	if string(jsonData) == "[]" || string(jsonData) == "{}" {
		jsonData = []byte{}
	}

	return crypto.SHA256Hex(jsonData), nil
}
