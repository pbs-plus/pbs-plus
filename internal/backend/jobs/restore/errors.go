package restore

import "errors"

var (
	ErrTargetUnreachable = errors.New("target unreachable")
	ErrTargetNotFound    = errors.New("target does not exist")
	ErrMountEmpty        = errors.New("target directory is empty, skipping restore")
)
