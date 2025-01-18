package controllers

import (
	"encoding/base64"
	"strings"
)

func EncodePath(path string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(path))
	encoded = strings.ReplaceAll(encoded, "+", "-")
	encoded = strings.ReplaceAll(encoded, "/", "_")
	encoded = strings.TrimRight(encoded, "=")
	return encoded
}

func DecodePath(orig string) string {
	encoded := strings.ReplaceAll(orig, "-", "+")
	encoded = strings.ReplaceAll(orig, "_", "/")

	padding := len(encoded) % 4
	if padding != 0 {
		encoded += strings.Repeat("=", 4-padding)
	}

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return orig
	}
	return string(decoded)
}
