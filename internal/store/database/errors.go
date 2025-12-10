package sqlite

import (
	"errors"
)

var (
	ErrTargetNotFound = errors.New("target not found; check targets list to verify")
	ErrJobNotFound    = errors.New("job not found")
	ErrTokenNotFound  = errors.New("token not found")
	ErrSecretNotFound = errors.New("secret not found")
)
