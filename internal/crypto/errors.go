package crypto

import "errors"

var (
	ErrTokenInvalidVersion = errors.New("crypto: unsupported token version")
	ErrTokenInvalidFormat  = errors.New("crypto: invalid token format")
	ErrTokenBadSignature   = errors.New("crypto: token signature invalid")
	ErrTokenExpired        = errors.New("crypto: token expired")

	ErrSealCiphertextTooShort = errors.New("crypto: nacl ciphertext too short")
	ErrSealBoxOpenFailed      = errors.New("crypto: nacl box open failed")

	ErrNoPeerCerts = errors.New("crypto: no peer certificates")
)
