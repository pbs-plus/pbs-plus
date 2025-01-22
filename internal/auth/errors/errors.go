package errors

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidToken indicates the provided token is invalid or expired
	ErrInvalidToken = errors.New("invalid or expired token")

	// ErrUnauthorized indicates the client is not authorized
	ErrUnauthorized = errors.New("unauthorized")

	// ErrCertificateRequired indicates missing or invalid certificates
	ErrCertificateRequired = errors.New("valid certificates are required")

	// ErrInvalidConfig indicates invalid configuration
	ErrInvalidConfig = errors.New("invalid configuration")
)

// AuthError represents a custom error type for authentication errors
type AuthError struct {
	Op  string // Operation that failed
	Err error  // Underlying error
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

// WrapError wraps an error with additional operation context
func WrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &AuthError{
		Op:  op,
		Err: err,
	}
}
