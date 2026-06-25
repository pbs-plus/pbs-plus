package jobs

import "errors"

var (
	ErrCanceled      = errors.New("operation canceled")
	ErrOneInstance   = errors.New("a job is still running; only one instance allowed")
	ErrManagerClosed = errors.New("manager is closed")

	ErrTargetUnreachable = errors.New("target unreachable")
	ErrTargetNotFound    = errors.New("target not found")
	ErrMountEmpty        = errors.New("target directory is empty")
	ErrSubpathNotFound   = errors.New("subpath does not exist")
	ErrUnexpected        = errors.New("unknown error, view logs for details")
)
