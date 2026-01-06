package sqlite

import (
	"errors"
)

var (
	ErrTargetNotFound  = errors.New("target not found; check targets list to verify")
	ErrBackupNotFound  = errors.New("backup job not found")
	ErrRestoreNotFound = errors.New("restore job not found")
	ErrTokenNotFound   = errors.New("token not found")
	ErrSecretNotFound  = errors.New("secret not found")
)
