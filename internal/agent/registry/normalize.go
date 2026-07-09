package registry

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"strings"
)

func isPEMData(value string) bool {
	return strings.Contains(value, "-----BEGIN") && strings.Contains(value, "-----END")
}

func normalizePEMData(pemData string) string {
	pemData = strings.ReplaceAll(pemData, "\r\n", "\n")
	pemData = strings.ReplaceAll(pemData, "\r", "\n")
	lines := strings.Split(pemData, "\n")
	var normalized []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	result := strings.Join(normalized, "\n")
	if !strings.HasSuffix(result, "\n") {
		result += "\n"
	}
	return result
}

func preprocessValue(value string, isSecret bool) string {
	if isSecret && isPEMData(value) {
		return normalizePEMData(value)
	}
	return value
}

func unwrapBase64Layers(val string, key string) (string, error) {
	if isPEMData(val) {
		return val, nil
	}

	for attempt := 1; attempt <= 4; attempt++ {
		decoded, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return val, fmt.Errorf("unwrapBase64Layers: value is not PEM and not valid base64 (attempt %d): %w", attempt, err)
		}
		if isPEMData(string(decoded)) {
			return string(decoded), nil
		}
		if len(decoded) > 0 && decoded[0] == 0x30 {
			return pemEncodeDER(decoded, key), nil
		}
		val = string(decoded)
	}

	return val, fmt.Errorf("unwrapBase64Layers: exhausted base64 decode attempts, value is not PEM or DER")
}

func pemEncodeDER(der []byte, key string) string {
	var blockType string
	switch strings.ToLower(key) {
	case "cert":
		blockType = "CERTIFICATE"
	case "priv", "key":
		if _, err := x509.ParsePKCS8PrivateKey(der); err == nil {
			blockType = "PRIVATE KEY"
		} else if _, err := x509.ParsePKCS1PrivateKey(der); err == nil {
			blockType = "RSA PRIVATE KEY"
		} else if _, err := x509.ParseECPrivateKey(der); err == nil {
			blockType = "EC PRIVATE KEY"
		} else {
			blockType = "PRIVATE KEY"
		}
	case "serverca", "legacyserverca":
		blockType = "CERTIFICATE"
	default:
		blockType = "CERTIFICATE"
	}
	var buf strings.Builder
	if err := pem.Encode(&buf, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		return ""
	}
	return buf.String()
}
