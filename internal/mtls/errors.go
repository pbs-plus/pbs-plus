package mtls

import "errors"

var (
	ErrInvalidToken        = errors.New("invalid or expired token")
	ErrUnauthorized        = errors.New("unauthorized")
	ErrCertificateRequired = errors.New("valid certificates are required")
	ErrInvalidConfig       = errors.New("invalid configuration")
)
