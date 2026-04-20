package system

import (
	"encoding/base64"
	"net/url"
	"strings"
)

func EncodePath(decoded string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(decoded))
	encoded = strings.TrimRight(encoded, "=")
	encoded = strings.ReplaceAll(encoded, "+", "-")
	encoded = strings.ReplaceAll(encoded, "/", "_")
	return url.QueryEscape(encoded)
}
