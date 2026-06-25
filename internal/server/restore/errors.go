package restore

import "github.com/pbs-plus/pbs-plus/internal/server/jobs"

var (
	ErrTargetUnreachable = jobs.ErrTargetUnreachable
	ErrTargetNotFound    = jobs.ErrTargetNotFound
	ErrMountEmpty        = jobs.ErrMountEmpty
)
