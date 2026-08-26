//go:build linux

package jobs

import (
	"context"
	"errors"
)

// Job is a unit of durable work: PreExec prepares, Execute runs, callbacks fire once per outcome.
type Job struct {
	ID        string
	PreExec   func(ctx context.Context) error
	Execute   func(ctx context.Context) error
	OnSuccess func()
	OnError   func(err error)
	Cleanup   func()
}

func RunJob(ctx context.Context, job *Job) error {
	defer func() {
		if job.Cleanup != nil {
			job.Cleanup()
		}
	}()
	if err := ctx.Err(); err != nil {
		return runJobError(job, ErrCanceled)
	}
	if job.PreExec != nil {
		if err := job.PreExec(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return runJobError(job, ErrCanceled)
			}
			return runJobError(job, err)
		}
	}
	if job.Execute != nil {
		if err := job.Execute(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return runJobError(job, ErrCanceled)
			}
			return runJobError(job, err)
		}
	}
	if job.OnSuccess != nil {
		job.OnSuccess()
	}
	return nil
}

func runJobError(job *Job, err error) error {
	if job.OnError != nil {
		job.OnError(err)
	}
	return err
}
