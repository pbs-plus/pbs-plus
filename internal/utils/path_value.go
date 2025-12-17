package utils

import (
	"encoding/base64"
	"net/url"
	"strings"
)

func DecodePath(encoded string) string {
	decoded, err := url.QueryUnescape(encoded)
	if err != nil {
		return encoded
	}

	decoded = strings.ReplaceAll(decoded, "-", "+")
	decoded = strings.ReplaceAll(decoded, "_", "/")

	switch len(decoded) % 4 {
	case 2:
		decoded += "=="
	case 3:
		decoded += "="
	}

	data, err := base64.StdEncoding.DecodeString(decoded)
	if err != nil {
		return encoded
	}

	return string(data)
}
