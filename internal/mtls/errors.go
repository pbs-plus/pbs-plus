package mtls

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidToken        = errors.New("invalid or expired token")
	ErrUnauthorized        = errors.New("unauthorized")
	ErrCertificateRequired = errors.New("valid certificates are required")
	ErrInvalidConfig       = errors.New("invalid configuration")
)

type AuthError struct {
	Op  string
	Err error
}

func (e *AuthError) Error() string {
	if e.Op != "" {
		return fmt.Sprintf("%s: %v", e.Op, e.Err)
	}
	return e.Err.Error()
}

func (e *AuthError) Unwrap() error {
	return e.Err
}

func WrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &AuthError{
		Op:  op,
		Err: err,
	}
}
