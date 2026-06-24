package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func HMACSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func HMACSHA256Hex(key, data []byte) string {
	return hex.EncodeToString(HMACSHA256(key, data))
}

func VerifyHMACSHA256(key, data, expected []byte) bool {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return hmac.Equal(m.Sum(nil), expected)
}

func SHA256(data []byte) [32]byte {
	return sha256.Sum256(data)
}

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func DeriveKey(secret []byte, context string, length int) ([]byte, error) {
	if length > 64 {
		return nil, fmt.Errorf("crypto: derive key: length %d exceeds sha256 output", length)
	}
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(context))
	out := h.Sum(nil)
	if length <= 32 {
		return out[:length], nil
	}
	h2 := hmac.New(sha256.New, secret)
	h2.Write(out)
	out2 := h2.Sum(nil)
	return append(out[:32], out2[:length-32]...), nil
}
