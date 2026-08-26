//go:build linux

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database/sqlc"
)

const (
	WorkflowExecutionPending   = "pending"
	WorkflowExecutionRunning   = "running"
	WorkflowExecutionSucceeded = "succeeded"
	WorkflowExecutionFailed    = "failed"
	WorkflowExecutionCanceled  = "canceled"
)

var ErrWorkflowExecutionNotFound = errors.New("workflow execution not found")

type WorkflowSubmit struct {
	ID                string
	Kind              string
	DefinitionID      string
	Trigger           string
	DedupeKey         string
	Payload           json.RawMessage
	Resources         []string
	MaxAttempts       int
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	RunAt             time.Time
	ParentExecutionID string
}

type WorkflowExecution struct {
	ID                string
	Kind              string
	DefinitionID      string
	Trigger           string
	DedupeKey         string
	Payload           json.RawMessage
	State             string
	Attempt           int
	MaxAttempts       int
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	RunAt             time.Time
	LeaseOwner        string
	LeaseUntil        time.Time
	CancelRequested   bool
	LastError         string
	ParentExecutionID string
	CreatedAt         time.Time
	StartedAt         time.Time
	FinishedAt        time.Time
}

type WorkflowActivity struct {
	ExecutionID string
	Name        string
	InputHash   string
	State       string
	Attempt     int
	Result      json.RawMessage
	Checkpoint  json.RawMessage
	LastError   string
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
}

type WorkflowEvent struct {
	Sequence    int64
	ExecutionID string
	Type        string
	Data        json.RawMessage
	CreatedAt   time.Time
}

func (d *Database) SubmitWorkflow(ctx context.Context, submit WorkflowSubmit) (WorkflowExecution, bool, error) {
	if err := validateWorkflowSubmit(submit); err != nil {
		return WorkflowExecution{}, false, err
	}

	var execution WorkflowExecution
	created := false
	err := d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		err := q.CreateWorkflowExecution(ctx, sqlc.CreateWorkflowExecutionParams{
			ID:                  submit.ID,
			Kind:                submit.Kind,
			DefinitionID:        submit.DefinitionID,
			Trigger:             submit.Trigger,
			DedupeKey:           submit.DedupeKey,
			Payload:             string(submit.Payload),
			MaxAttempts:         int64(submit.MaxAttempts),
			RetryInitialSeconds: int64(submit.RetryInitialDelay / time.Second),
			RetryMaxSeconds:     int64(submit.RetryMaxDelay / time.Second),
			RunAt:               submit.RunAt.Unix(),
			CreatedAt:           time.Now().Unix(),
			ParentExecutionID:   nullString(submit.ParentExecutionID),
		})
		if err != nil {
			row, getErr := q.GetWorkflowExecutionByDedupeKey(ctx, submit.DedupeKey)
			if getErr != nil {
				return fmt.Errorf("creating workflow execution: %w", err)
			}
			execution = workflowExecutionFromSQLC(row)
			return nil
		}

		for _, resource := range normalizedWorkflowResources(submit.Resources) {
			if err := q.CreateWorkflowExecutionResource(ctx, sqlc.CreateWorkflowExecutionResourceParams{
				ExecutionID: submit.ID,
				ResourceKey: resource,
			}); err != nil {
				return fmt.Errorf("creating workflow resource: %w", err)
			}
		}
		if err := createWorkflowEvent(ctx, q, submit.ID, "workflow.submitted", "{}"); err != nil {
			return err
		}
		row, err := q.GetWorkflowExecution(ctx, submit.ID)
		if err != nil {
			return fmt.Errorf("getting created workflow execution: %w", err)
		}
		execution = workflowExecutionFromSQLC(row)
		created = true
		return nil
	})
	if err != nil {
		return WorkflowExecution{}, false, err
	}
	return execution, created, nil
}

func (d *Database) GetWorkflowExecution(ctx context.Context, id string) (WorkflowExecution, error) {
	row, err := d.readQueries.GetWorkflowExecution(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkflowExecution{}, ErrWorkflowExecutionNotFound
	}
	if err != nil {
		return WorkflowExecution{}, fmt.Errorf("getting workflow execution: %w", err)
	}
	return workflowExecutionFromSQLC(row), nil
}

func (d *Database) GetActiveWorkflowExecution(ctx context.Context, kind, definitionID string) (WorkflowExecution, error) {
	row, err := d.readQueries.GetActiveWorkflowExecutionByDefinition(ctx, sqlc.GetActiveWorkflowExecutionByDefinitionParams{
		Kind:         kind,
		DefinitionID: definitionID,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return WorkflowExecution{}, ErrWorkflowExecutionNotFound
	}
	if err != nil {
		return WorkflowExecution{}, fmt.Errorf("getting active workflow execution: %w", err)
	}
	return workflowExecutionFromSQLC(row), nil
}

func (d *Database) ClaimWorkflowExecution(ctx context.Context, owner string, now, leaseUntil time.Time) (WorkflowExecution, bool, error) {
	if owner == "" || !leaseUntil.After(now) {
		return WorkflowExecution{}, false, errors.New("invalid workflow lease")
	}

	var claimed WorkflowExecution
	claimedOK := false
	err := d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		if err := q.RequeueExpiredWorkflowExecutions(ctx, sqlc.RequeueExpiredWorkflowExecutionsParams{
			RunAt:      now.Unix(),
			LeaseUntil: nullInt64(now.Unix()),
		}); err != nil {
			return fmt.Errorf("recovering expired workflow executions: %w", err)
		}
		if err := q.DeleteExpiredWorkflowResourceLocks(ctx, now.Unix()); err != nil {
			return fmt.Errorf("deleting expired workflow locks: %w", err)
		}
		ids, err := q.ListClaimableWorkflowExecutionIDs(ctx, now.Unix())
		if err != nil {
			return fmt.Errorf("listing claimable workflow executions: %w", err)
		}
		for _, id := range ids {
			updated, err := q.ClaimWorkflowExecution(ctx, sqlc.ClaimWorkflowExecutionParams{
				LeaseOwner: nullString(owner),
				LeaseUntil: nullInt64(leaseUntil.Unix()),
				StartedAt:  nullInt64(now.Unix()),
				ID:         id,
				RunAt:      now.Unix(),
			})
			if err != nil {
				return fmt.Errorf("claiming workflow execution: %w", err)
			}
			if updated == 0 {
				continue
			}

			resources, err := q.ListWorkflowExecutionResources(ctx, id)
			if err != nil {
				return fmt.Errorf("listing workflow resources: %w", err)
			}
			locked := true
			for _, resource := range resources {
				updated, err := q.CreateWorkflowResourceLock(ctx, sqlc.CreateWorkflowResourceLockParams{
					ResourceKey: resource,
					ExecutionID: id,
					LeaseUntil:  leaseUntil.Unix(),
				})
				if err != nil {
					return fmt.Errorf("locking workflow resource: %w", err)
				}
				if updated == 0 {
					locked = false
					break
				}
			}
			if !locked {
				if err := q.DeleteWorkflowResourceLocks(ctx, id); err != nil {
					return fmt.Errorf("releasing workflow resources: %w", err)
				}
				if err := q.ReleaseWorkflowExecutionClaim(ctx, sqlc.ReleaseWorkflowExecutionClaimParams{
					RunAt:      now.Add(time.Second).Unix(),
					ID:         id,
					LeaseOwner: nullString(owner),
				}); err != nil {
					return fmt.Errorf("releasing workflow execution claim: %w", err)
				}
				continue
			}
			if err := createWorkflowEvent(ctx, q, id, "workflow.started", "{}"); err != nil {
				return err
			}
			row, err := q.GetWorkflowExecution(ctx, id)
			if err != nil {
				return fmt.Errorf("getting claimed workflow execution: %w", err)
			}
			claimed = workflowExecutionFromSQLC(row)
			claimedOK = true
			return nil
		}
		return nil
	})
	if err != nil {
		return WorkflowExecution{}, false, err
	}
	return claimed, claimedOK, nil
}

func (d *Database) RenewWorkflowExecutionLease(ctx context.Context, id, owner string, leaseUntil time.Time) error {
	return d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		updated, err := q.RenewWorkflowExecutionLease(ctx, sqlc.RenewWorkflowExecutionLeaseParams{
			LeaseUntil: nullInt64(leaseUntil.Unix()),
			ID:         id,
			LeaseOwner: nullString(owner),
		})
		if err != nil {
			return fmt.Errorf("renewing workflow lease: %w", err)
		}
		if updated == 0 {
			return ErrWorkflowExecutionNotFound
		}
		if err := q.RenewWorkflowResourceLocks(ctx, sqlc.RenewWorkflowResourceLocksParams{
			LeaseUntil:  leaseUntil.Unix(),
			ExecutionID: id,
		}); err != nil {
			return fmt.Errorf("renewing workflow resource locks: %w", err)
		}
		return nil
	})
}

func (d *Database) CancelWorkflowExecution(ctx context.Context, id string, now time.Time) (WorkflowExecution, error) {
	var execution WorkflowExecution
	err := d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		updated, err := q.RequestWorkflowExecutionCancellation(ctx, id)
		if err != nil {
			return fmt.Errorf("requesting workflow cancellation: %w", err)
		}
		if updated == 0 {
			return ErrWorkflowExecutionNotFound
		}
		row, err := q.GetWorkflowExecution(ctx, id)
		if err != nil {
			return fmt.Errorf("getting workflow execution for cancellation: %w", err)
		}
		if row.State == WorkflowExecutionPending {
			if _, err := q.CancelPendingWorkflowExecution(ctx, sqlc.CancelPendingWorkflowExecutionParams{
				FinishedAt: nullInt64(now.Unix()),
				ID:         id,
			}); err != nil {
				return fmt.Errorf("canceling pending workflow execution: %w", err)
			}
			if err := q.DeleteWorkflowResourceLocks(ctx, id); err != nil {
				return fmt.Errorf("releasing canceled workflow resources: %w", err)
			}
			if err := createWorkflowEvent(ctx, q, id, "workflow.canceled", "{}"); err != nil {
				return err
			}
		} else if err := createWorkflowEvent(ctx, q, id, "workflow.cancel_requested", "{}"); err != nil {
			return err
		}
		row, err = q.GetWorkflowExecution(ctx, id)
		if err != nil {
			return fmt.Errorf("getting canceled workflow execution: %w", err)
		}
		execution = workflowExecutionFromSQLC(row)
		return nil
	})
	if err != nil {
		return WorkflowExecution{}, err
	}
	return execution, nil
}

func (d *Database) FinishWorkflowExecution(ctx context.Context, id, owner, state string, runAt time.Time, lastError string) error {
	if state != WorkflowExecutionPending && state != WorkflowExecutionSucceeded && state != WorkflowExecutionFailed && state != WorkflowExecutionCanceled {
		return fmt.Errorf("invalid workflow completion state %q", state)
	}
	return d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		finishedAt := sql.NullInt64{}
		if state != WorkflowExecutionPending {
			finishedAt = nullInt64(time.Now().Unix())
		}
		updated, err := q.FinishWorkflowExecution(ctx, sqlc.FinishWorkflowExecutionParams{
			State:      state,
			RunAt:      runAt.Unix(),
			LastError:  nullString(lastError),
			FinishedAt: finishedAt,
			ID:         id,
			LeaseOwner: nullString(owner),
		})
		if err != nil {
			return fmt.Errorf("finishing workflow execution: %w", err)
		}
		if updated == 0 {
			return ErrWorkflowExecutionNotFound
		}
		if err := q.DeleteWorkflowResourceLocks(ctx, id); err != nil {
			return fmt.Errorf("releasing workflow resources: %w", err)
		}
		eventType := "workflow." + state
		if state == WorkflowExecutionPending {
			eventType = "workflow.retry_scheduled"
		}
		return createWorkflowEvent(ctx, q, id, eventType, workflowEventData(lastError))
	})
}

func (d *Database) StartWorkflowActivity(ctx context.Context, executionID, name, inputHash string, now time.Time) (WorkflowActivity, bool, error) {
	if executionID == "" || name == "" || inputHash == "" {
		return WorkflowActivity{}, false, errors.New("invalid workflow activity")
	}

	var activity WorkflowActivity
	completed := false
	err := d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		row, err := q.GetWorkflowActivity(ctx, sqlc.GetWorkflowActivityParams{ExecutionID: executionID, Name: name})
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := q.CreateWorkflowActivity(ctx, sqlc.CreateWorkflowActivityParams{
				ExecutionID: executionID,
				Name:        name,
				InputHash:   inputHash,
				CreatedAt:   now.Unix(),
			}); err != nil {
				return fmt.Errorf("creating workflow activity: %w", err)
			}
			if err := createWorkflowEvent(ctx, q, executionID, "activity.scheduled", workflowActivityEventData(name)); err != nil {
				return err
			}
			row, err = q.GetWorkflowActivity(ctx, sqlc.GetWorkflowActivityParams{ExecutionID: executionID, Name: name})
		}
		if err != nil {
			return fmt.Errorf("getting workflow activity: %w", err)
		}
		if row.InputHash != inputHash {
			return fmt.Errorf("workflow activity %q input changed", name)
		}
		activity = workflowActivityFromSQLC(row)
		if activity.State == "completed" {
			completed = true
			return nil
		}
		if _, err := q.StartWorkflowActivity(ctx, sqlc.StartWorkflowActivityParams{
			StartedAt:   nullInt64(now.Unix()),
			ExecutionID: executionID,
			Name:        name,
		}); err != nil {
			return fmt.Errorf("starting workflow activity: %w", err)
		}
		if err := createWorkflowEvent(ctx, q, executionID, "activity.started", workflowActivityEventData(name)); err != nil {
			return err
		}
		row, err = q.GetWorkflowActivity(ctx, sqlc.GetWorkflowActivityParams{ExecutionID: executionID, Name: name})
		if err != nil {
			return fmt.Errorf("getting started workflow activity: %w", err)
		}
		activity = workflowActivityFromSQLC(row)
		return nil
	})
	if err != nil {
		return WorkflowActivity{}, false, err
	}
	return activity, completed, nil
}

func (d *Database) CheckpointWorkflowActivity(ctx context.Context, executionID, name string, checkpoint json.RawMessage) error {
	if !json.Valid(checkpoint) {
		return errors.New("workflow checkpoint must be valid JSON")
	}
	return d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		updated, err := q.CheckpointWorkflowActivity(ctx, sqlc.CheckpointWorkflowActivityParams{
			Checkpoint:  nullString(string(checkpoint)),
			ExecutionID: executionID,
			Name:        name,
		})
		if err != nil {
			return fmt.Errorf("checkpointing workflow activity: %w", err)
		}
		if updated == 0 {
			return ErrWorkflowExecutionNotFound
		}
		return createWorkflowEvent(ctx, q, executionID, "activity.checkpointed", workflowActivityEventData(name))
	})
}

func (d *Database) CompleteWorkflowActivity(ctx context.Context, executionID, name string, result json.RawMessage, now time.Time) error {
	if !json.Valid(result) {
		return errors.New("workflow activity result must be valid JSON")
	}
	return d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		updated, err := q.CompleteWorkflowActivity(ctx, sqlc.CompleteWorkflowActivityParams{
			Result:      nullString(string(result)),
			CompletedAt: nullInt64(now.Unix()),
			ExecutionID: executionID,
			Name:        name,
		})
		if err != nil {
			return fmt.Errorf("completing workflow activity: %w", err)
		}
		if updated == 0 {
			return ErrWorkflowExecutionNotFound
		}
		return createWorkflowEvent(ctx, q, executionID, "activity.completed", workflowActivityEventData(name))
	})
}

func (d *Database) RetryWorkflowActivity(ctx context.Context, executionID, name, lastError string) error {
	return d.RunInTransaction(ctx, func(_ *Transaction, q *sqlc.Queries) error {
		updated, err := q.FailWorkflowActivity(ctx, sqlc.FailWorkflowActivityParams{
			LastError:   nullString(lastError),
			ExecutionID: executionID,
			Name:        name,
		})
		if err != nil {
			return fmt.Errorf("retrying workflow activity: %w", err)
		}
		if updated == 0 {
			return ErrWorkflowExecutionNotFound
		}
		return createWorkflowEvent(ctx, q, executionID, "activity.failed", workflowActivityEventData(name))
	})
}

func (d *Database) ListWorkflowExecutionEvents(ctx context.Context, executionID string) ([]WorkflowEvent, error) {
	rows, err := d.readQueries.ListWorkflowExecutionEvents(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("listing workflow execution events: %w", err)
	}
	events := make([]WorkflowEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, WorkflowEvent{
			Sequence:    row.Sequence,
			ExecutionID: row.ExecutionID,
			Type:        row.EventType,
			Data:        json.RawMessage(row.Data),
			CreatedAt:   time.Unix(row.CreatedAt, 0),
		})
	}
	return events, nil
}

func validateWorkflowSubmit(submit WorkflowSubmit) error {
	if submit.ID == "" || submit.Kind == "" || submit.DefinitionID == "" || submit.Trigger == "" || submit.DedupeKey == "" {
		return errors.New("workflow execution identity is required")
	}
	if !json.Valid(submit.Payload) {
		return errors.New("workflow payload must be valid JSON")
	}
	if submit.MaxAttempts < 1 {
		return errors.New("workflow max attempts must be positive")
	}
	if submit.RetryInitialDelay < time.Second || submit.RetryMaxDelay < submit.RetryInitialDelay {
		return errors.New("invalid workflow retry delays")
	}
	if submit.RunAt.IsZero() {
		return errors.New("workflow run time is required")
	}
	for _, resource := range submit.Resources {
		if strings.TrimSpace(resource) == "" {
			return errors.New("workflow resource key is empty")
		}
	}
	return nil
}

func normalizedWorkflowResources(resources []string) []string {
	seen := make(map[string]struct{}, len(resources))
	for _, resource := range resources {
		seen[resource] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func workflowExecutionFromSQLC(row sqlc.JobExecution) WorkflowExecution {
	return WorkflowExecution{
		ID:                row.ID,
		Kind:              row.Kind,
		DefinitionID:      row.DefinitionID,
		Trigger:           row.Trigger,
		DedupeKey:         row.DedupeKey,
		Payload:           json.RawMessage(row.Payload),
		State:             row.State,
		Attempt:           int(row.Attempt),
		MaxAttempts:       int(row.MaxAttempts),
		RetryInitialDelay: time.Duration(row.RetryInitialSeconds) * time.Second,
		RetryMaxDelay:     time.Duration(row.RetryMaxSeconds) * time.Second,
		RunAt:             time.Unix(row.RunAt, 0),
		LeaseOwner:        fromNullString(row.LeaseOwner),
		LeaseUntil:        fromNullTime(row.LeaseUntil),
		CancelRequested:   row.CancelRequested != 0,
		LastError:         fromNullString(row.LastError),
		ParentExecutionID: fromNullString(row.ParentExecutionID),
		CreatedAt:         time.Unix(row.CreatedAt, 0),
		StartedAt:         fromNullTime(row.StartedAt),
		FinishedAt:        fromNullTime(row.FinishedAt),
	}
}

func workflowActivityFromSQLC(row sqlc.JobExecutionActivity) WorkflowActivity {
	return WorkflowActivity{
		ExecutionID: row.ExecutionID,
		Name:        row.Name,
		InputHash:   row.InputHash,
		State:       row.State,
		Attempt:     int(row.Attempt),
		Result:      json.RawMessage(fromNullString(row.Result)),
		Checkpoint:  json.RawMessage(fromNullString(row.Checkpoint)),
		LastError:   fromNullString(row.LastError),
		CreatedAt:   time.Unix(row.CreatedAt, 0),
		StartedAt:   fromNullTime(row.StartedAt),
		CompletedAt: fromNullTime(row.CompletedAt),
	}
}

func createWorkflowEvent(ctx context.Context, q *sqlc.Queries, executionID, eventType, data string) error {
	if err := q.CreateWorkflowExecutionEvent(ctx, sqlc.CreateWorkflowExecutionEventParams{
		ExecutionID: executionID,
		EventType:   eventType,
		Data:        data,
		CreatedAt:   time.Now().Unix(),
	}); err != nil {
		return fmt.Errorf("creating workflow event: %w", err)
	}
	return nil
}

func workflowEventData(lastError string) string {
	data, _ := json.Marshal(map[string]string{"error": lastError})
	return string(data)
}

func workflowActivityEventData(name string) string {
	data, _ := json.Marshal(map[string]string{"name": name})
	return string(data)
}

func nullString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func nullInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: true}
}

func fromNullTime(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.Unix(value.Int64, 0)
}
