//go:build linux

// Package store persists durable workflow executions in a dedicated
// SQLite database, separate from the main PBS Plus database, so engine
// write churn can never contend with configuration writes.
package jobdb

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/sqldb"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb/jobquery"
)

//go:embed migrations/*.sql
var migrations embed.FS

const DefaultPath = "/etc/proxmox-backup/pbs-plus/jobs.db"

// Execution states.
const (
	StatePending   = "pending"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
	StateCanceled  = "canceled"
)

// ErrNotFound reports a missing execution or activity.
var ErrNotFound = errors.New("store: execution not found")

// SubmitRequest describes a new durable execution.
type SubmitRequest struct {
	ID                string
	Kind              string
	WorkflowVersion   string
	DefinitionID      string
	Trigger           string
	DedupeKey         string
	Payload           []byte
	Resources         []string
	MaxAttempts       int
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	RunAt             time.Time
	ParentExecutionID string
}

// Execution is one durable workflow run.
type Execution struct {
	ID                string
	Kind              string
	WorkflowVersion   string
	DefinitionID      string
	Trigger           string
	DedupeKey         string
	Payload           []byte
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

// Activity is one replayable step of an execution, keyed by name and
// input hash; completed activities are skipped on replay.
type Activity struct {
	ExecutionID string
	Name        string
	Position    int
	InputHash   string
	State       string
	Attempt     int
	Result      []byte
	Checkpoint  []byte
	LastError   string
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
}

// Event is one entry of an execution's durable history log.
type Event struct {
	Sequence    int64
	ExecutionID string
	Type        string
	Data        []byte
	CreatedAt   time.Time
}

// Store is the engine database handle.
type Store struct {
	*sqldb.Handle
	write *jobquery.Queries
	read  *jobquery.Queries
}

// Open opens (creating if needed) the engine database at path and
// applies migrations. Empty path falls back to DefaultPath.
func Open(path string) (*Store, error) {
	if path == "" {
		path = DefaultPath
	}
	db, err := sqldb.Open(path, migrations, "migrations")
	if err != nil {
		return nil, err
	}
	return &Store{Handle: db, write: jobquery.New(db.Writer()), read: jobquery.New(db.Reader())}, nil
}

// Submit inserts a new execution; when DedupeKey already exists it
// returns the existing execution with created=false.
func (d *Store) Submit(ctx context.Context, req SubmitRequest) (Execution, bool, error) {
	if req.WorkflowVersion == "" {
		req.WorkflowVersion = "1"
	}
	if err := validateSubmit(req); err != nil {
		return Execution{}, false, err
	}

	var execution Execution
	created := false
	err := d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		err := q.CreateExecution(ctx, jobquery.CreateExecutionParams{
			ID:                  req.ID,
			Kind:                req.Kind,
			DefinitionID:        req.DefinitionID,
			Trigger:             req.Trigger,
			DedupeKey:           req.DedupeKey,
			Payload:             string(req.Payload),
			MaxAttempts:         int64(req.MaxAttempts),
			RetryInitialSeconds: int64(req.RetryInitialDelay / time.Second),
			RetryMaxSeconds:     int64(req.RetryMaxDelay / time.Second),
			RunAt:               req.RunAt.Unix(),
			CreatedAt:           time.Now().Unix(),
			ParentExecutionID:   nullString(req.ParentExecutionID),
			WorkflowVersion:     req.WorkflowVersion,
		})
		if err != nil {
			row, getErr := q.GetExecutionByDedupeKey(ctx, req.DedupeKey)
			if getErr != nil {
				return fmt.Errorf("creating execution: %w", err)
			}
			execution = executionFromRow(row)
			return nil
		}

		for _, resource := range normalizedResources(req.Resources) {
			if err := q.CreateExecutionResource(ctx, jobquery.CreateExecutionResourceParams{
				ExecutionID: req.ID,
				ResourceKey: resource,
			}); err != nil {
				return fmt.Errorf("creating execution resource: %w", err)
			}
		}
		if err := createEvent(ctx, q, req.ID, "execution.submitted", nil); err != nil {
			return err
		}
		row, err := q.GetExecution(ctx, req.ID)
		if err != nil {
			return fmt.Errorf("getting created execution: %w", err)
		}
		execution = executionFromRow(row)
		created = true
		return nil
	})
	if err != nil {
		return Execution{}, false, err
	}
	return execution, created, nil
}

func (d *Store) GetExecution(ctx context.Context, id string) (Execution, error) {
	row, err := d.read.GetExecution(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Execution{}, ErrNotFound
	}
	if err != nil {
		return Execution{}, fmt.Errorf("getting execution: %w", err)
	}
	return executionFromRow(row), nil
}

func (d *Store) GetActiveExecution(ctx context.Context, kind, definitionID string) (Execution, error) {
	row, err := d.read.GetActiveExecutionByDefinition(ctx, jobquery.GetActiveExecutionByDefinitionParams{
		Kind:         kind,
		DefinitionID: definitionID,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Execution{}, ErrNotFound
	}
	if err != nil {
		return Execution{}, fmt.Errorf("getting active execution: %w", err)
	}
	return executionFromRow(row), nil
}

// Claim requeues expired executions, purges expired resource locks,
// then atomically claims one due execution together with its resource
// locks. Executions whose resources are busy are deferred, not claimed.
func (d *Store) Claim(ctx context.Context, owner string, now, leaseUntil time.Time) (Execution, bool, error) {
	if owner == "" || !leaseUntil.After(now) {
		return Execution{}, false, errors.New("invalid claim lease")
	}

	var claimed Execution
	ok := false
	err := d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		canceledIDs, err := q.ListExpiredCanceledExecutionIDs(ctx, nullInt64(now.Unix()))
		if err != nil {
			return fmt.Errorf("listing expired canceled executions: %w", err)
		}
		for _, id := range canceledIDs {
			updated, err := q.CancelExpiredExecution(ctx, jobquery.CancelExpiredExecutionParams{
				FinishedAt: nullInt64(now.Unix()),
				ID:         id,
				LeaseUntil: nullInt64(now.Unix()),
			})
			if err != nil {
				return fmt.Errorf("recovering canceled execution: %w", err)
			}
			if updated == 0 {
				continue
			}
			if err := q.DeleteResourceLocks(ctx, id); err != nil {
				return fmt.Errorf("releasing canceled execution resources: %w", err)
			}
			if err := createEvent(ctx, q, id, "execution.canceled", nil); err != nil {
				return err
			}
		}
		recoveredIDs, err := q.RequeueExpiredExecutions(ctx, jobquery.RequeueExpiredExecutionsParams{
			RunAt:      now.Unix(),
			LeaseUntil: nullInt64(now.Unix()),
		})
		if err != nil {
			return fmt.Errorf("recovering expired executions: %w", err)
		}
		for _, id := range recoveredIDs {
			if err := q.ResetRunningActivities(ctx, id); err != nil {
				return fmt.Errorf("resetting recovered execution activities: %w", err)
			}
			if err := createEvent(ctx, q, id, "execution.recovered", nil); err != nil {
				return err
			}
		}
		if err := q.DeleteExpiredResourceLocks(ctx, now.Unix()); err != nil {
			return fmt.Errorf("deleting expired resource locks: %w", err)
		}
		ids, err := q.ListClaimableExecutionIDs(ctx, now.Unix())
		if err != nil {
			return fmt.Errorf("listing claimable executions: %w", err)
		}
		for _, id := range ids {
			resources, err := q.ListExecutionResources(ctx, id)
			if err != nil {
				return fmt.Errorf("listing execution resources: %w", err)
			}
			locked := true
			for _, resource := range resources {
				updated, err := q.CreateResourceLock(ctx, jobquery.CreateResourceLockParams{
					ResourceKey: resource,
					ExecutionID: id,
					LeaseUntil:  leaseUntil.Unix(),
				})
				if err != nil {
					return fmt.Errorf("locking execution resource: %w", err)
				}
				if updated == 0 {
					locked = false
					break
				}
			}
			if !locked {
				if err := q.DeleteResourceLocks(ctx, id); err != nil {
					return fmt.Errorf("releasing execution resources: %w", err)
				}
				if err := q.DelayExecution(ctx, jobquery.DelayExecutionParams{RunAt: now.Add(time.Second).Unix(), ID: id}); err != nil {
					return fmt.Errorf("delaying locked execution: %w", err)
				}
				continue
			}
			updated, err := q.ClaimExecution(ctx, jobquery.ClaimExecutionParams{
				LeaseOwner: nullString(owner),
				LeaseUntil: nullInt64(leaseUntil.Unix()),
				StartedAt:  nullInt64(now.Unix()),
				ID:         id,
				RunAt:      now.Unix(),
			})
			if err != nil {
				return fmt.Errorf("claiming execution: %w", err)
			}
			if updated == 0 {
				if err := q.DeleteResourceLocks(ctx, id); err != nil {
					return fmt.Errorf("releasing execution resources: %w", err)
				}
				continue
			}
			if err := createEvent(ctx, q, id, "execution.started", nil); err != nil {
				return err
			}
			row, err := q.GetExecution(ctx, id)
			if err != nil {
				return fmt.Errorf("getting claimed execution: %w", err)
			}
			claimed = executionFromRow(row)
			ok = true
			return nil
		}
		return nil
	})
	if err != nil {
		return Execution{}, false, err
	}
	return claimed, ok, nil
}

func (d *Store) RenewLease(ctx context.Context, id, owner string, attempt int, leaseUntil time.Time) error {
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		updated, err := q.RenewExecutionLease(ctx, jobquery.RenewExecutionLeaseParams{
			LeaseUntil: nullInt64(leaseUntil.Unix()),
			ID:         id,
			LeaseOwner: nullString(owner),
			Attempt:    int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("renewing lease: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		if err := q.RenewResourceLocks(ctx, jobquery.RenewResourceLocksParams{
			LeaseUntil:  leaseUntil.Unix(),
			ExecutionID: id,
		}); err != nil {
			return fmt.Errorf("renewing resource locks: %w", err)
		}
		return nil
	})
}

// Cancel requests cancellation; a pending execution cancels immediately,
// a running one cancels when its worker observes the request.
func (d *Store) Cancel(ctx context.Context, id string, now time.Time) (Execution, error) {
	var execution Execution
	err := d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		updated, err := q.RequestExecutionCancellation(ctx, id)
		if err != nil {
			return fmt.Errorf("requesting cancellation: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		row, err := q.GetExecution(ctx, id)
		if err != nil {
			return fmt.Errorf("getting execution for cancellation: %w", err)
		}
		if row.State == StatePending {
			if _, err := q.CancelPendingExecution(ctx, jobquery.CancelPendingExecutionParams{
				FinishedAt: nullInt64(now.Unix()),
				ID:         id,
			}); err != nil {
				return fmt.Errorf("canceling pending execution: %w", err)
			}
			if err := q.DeleteResourceLocks(ctx, id); err != nil {
				return fmt.Errorf("releasing canceled execution resources: %w", err)
			}
			if err := createEvent(ctx, q, id, "execution.canceled", nil); err != nil {
				return err
			}
		} else if err := createEvent(ctx, q, id, "execution.cancel_requested", nil); err != nil {
			return err
		}
		row, err = q.GetExecution(ctx, id)
		if err != nil {
			return fmt.Errorf("getting canceled execution: %w", err)
		}
		execution = executionFromRow(row)
		return nil
	})
	if err != nil {
		return Execution{}, err
	}
	return execution, nil
}

// Finish transitions a claimed execution to a terminal state, or back
// to pending with a delayed run_at when a retry is scheduled.
func (d *Store) Finish(ctx context.Context, id, owner string, attempt int, state string, runAt time.Time, lastError string) error {
	switch state {
	case StatePending, StateSucceeded, StateFailed, StateCanceled:
	default:
		return fmt.Errorf("invalid completion state %q", state)
	}
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		finishedAt := sql.NullInt64{}
		if state != StatePending {
			finishedAt = nullInt64(time.Now().Unix())
		}
		updated, err := q.FinishExecution(ctx, jobquery.FinishExecutionParams{
			State:      state,
			RunAt:      runAt.Unix(),
			LastError:  nullString(lastError),
			FinishedAt: finishedAt,
			ID:         id,
			LeaseOwner: nullString(owner),
			Attempt:    int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("finishing execution: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		if err := q.DeleteResourceLocks(ctx, id); err != nil {
			return fmt.Errorf("releasing execution resources: %w", err)
		}
		eventType := "execution." + state
		if state == StatePending {
			eventType = "execution.retry_scheduled"
		}
		return createEvent(ctx, q, id, eventType, map[string]string{"error": lastError})
	})
}

// StartActivity returns the activity for (execution, name), starting it
// unless it already completed; completed activities report done=true
// with their persisted result.
func (d *Store) StartActivity(ctx context.Context, executionID, owner string, attempt, position int, name, inputHash string, now time.Time) (Activity, bool, error) {
	if executionID == "" || owner == "" || attempt < 1 || position < 1 || name == "" || inputHash == "" {
		return Activity{}, false, errors.New("invalid activity")
	}

	var activity Activity
	completed := false
	err := d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		execution, err := q.GetExecution(ctx, executionID)
		if err != nil {
			return fmt.Errorf("getting workflow execution: %w", err)
		}
		if execution.State != StateRunning || fromNullString(execution.LeaseOwner) != owner || execution.Attempt != int64(attempt) || fromNullTime(execution.LeaseUntil).Before(time.Now()) {
			return ErrNotFound
		}
		row, err := q.GetActivity(ctx, jobquery.GetActivityParams{ExecutionID: executionID, Name: name})
		if errors.Is(err, sql.ErrNoRows) {
			atPosition, positionErr := q.GetActivityAtPosition(ctx, jobquery.GetActivityAtPositionParams{
				ExecutionID: executionID,
				Position:    int64(position),
			})
			if positionErr == nil {
				return fmt.Errorf("workflow replay mismatch at activity %d: recorded %q, got %q", position, atPosition.Name, name)
			}
			if !errors.Is(positionErr, sql.ErrNoRows) {
				return fmt.Errorf("getting activity at position: %w", positionErr)
			}
			if _, err := q.CreateActivity(ctx, jobquery.CreateActivityParams{
				ExecutionID: executionID,
				Name:        name,
				InputHash:   inputHash,
				CreatedAt:   now.Unix(),
				Position:    int64(position),
			}); err != nil {
				return fmt.Errorf("creating activity: %w", err)
			}
			if err := createEvent(ctx, q, executionID, "activity.scheduled", map[string]string{"name": name}); err != nil {
				return err
			}
			row, err = q.GetActivity(ctx, jobquery.GetActivityParams{ExecutionID: executionID, Name: name})
		}
		if err != nil {
			return fmt.Errorf("getting activity: %w", err)
		}
		if row.InputHash != inputHash {
			return fmt.Errorf("activity %q input changed", name)
		}
		if row.Position != int64(position) {
			return fmt.Errorf("workflow replay mismatch for activity %q: recorded position %d, got %d", name, row.Position, position)
		}
		activity = activityFromRow(row)
		if activity.State == "completed" {
			completed = true
			return nil
		}
		updated, err := q.StartActivity(ctx, jobquery.StartActivityParams{
			StartedAt:   nullInt64(now.Unix()),
			ExecutionID: executionID,
			Name:        name,
			LeaseOwner:  nullString(owner),
			Attempt:     int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("starting activity: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		if err := createEvent(ctx, q, executionID, "activity.started", map[string]string{"name": name}); err != nil {
			return err
		}
		row, err = q.GetActivity(ctx, jobquery.GetActivityParams{ExecutionID: executionID, Name: name})
		if err != nil {
			return fmt.Errorf("getting started activity: %w", err)
		}
		activity = activityFromRow(row)
		return nil
	})
	if err != nil {
		return Activity{}, false, err
	}
	return activity, completed, nil
}

func (d *Store) CheckpointActivity(ctx context.Context, executionID, owner string, attempt int, name string, checkpoint []byte) error {
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		updated, err := q.CheckpointActivity(ctx, jobquery.CheckpointActivityParams{
			Checkpoint:  nullString(string(checkpoint)),
			ExecutionID: executionID,
			Name:        name,
			LeaseOwner:  nullString(owner),
			Attempt:     int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("checkpointing activity: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		return createEvent(ctx, q, executionID, "activity.checkpointed", map[string]string{"name": name})
	})
}

func (d *Store) CompleteActivity(ctx context.Context, executionID, owner string, attempt int, name string, result []byte, now time.Time) error {
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		updated, err := q.CompleteActivity(ctx, jobquery.CompleteActivityParams{
			Result:      nullString(string(result)),
			CompletedAt: nullInt64(now.Unix()),
			ExecutionID: executionID,
			Name:        name,
			LeaseOwner:  nullString(owner),
			Attempt:     int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("completing activity: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		return createEvent(ctx, q, executionID, "activity.completed", map[string]string{"name": name})
	})
}

func (d *Store) FailActivity(ctx context.Context, executionID, owner string, attempt int, name, lastError string) error {
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		updated, err := q.FailActivity(ctx, jobquery.FailActivityParams{
			LastError:   nullString(lastError),
			ExecutionID: executionID,
			Name:        name,
			LeaseOwner:  nullString(owner),
			Attempt:     int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("failing activity: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		return createEvent(ctx, q, executionID, "activity.failed", map[string]string{"name": name})
	})
}

// InvalidateActivity un-completes a completed activity so a retry
// re-runs it; used when a later activity reveals the completed one's
// external effect actually failed.
func (d *Store) InvalidateActivity(ctx context.Context, executionID, owner string, attempt int, name string) error {
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		updated, err := q.InvalidateActivity(ctx, jobquery.InvalidateActivityParams{
			ExecutionID: executionID,
			Name:        name,
			LeaseOwner:  nullString(owner),
			Attempt:     int64(attempt),
		})
		if err != nil {
			return fmt.Errorf("invalidating activity: %w", err)
		}
		if updated == 0 {
			return ErrNotFound
		}
		return createEvent(ctx, q, executionID, "activity.invalidated", map[string]string{"name": name})
	})
}

func (d *Store) EnsureReplayComplete(ctx context.Context, executionID, owner string, attempt, position int) error {
	if attempt < 1 || position < 0 {
		return errors.New("invalid workflow replay position")
	}
	return d.RunInTransaction(ctx, func(tx *sqldb.Tx) error {
		q := d.write.WithTx(tx.Tx)
		row, err := q.GetExecution(ctx, executionID)
		if err != nil {
			return fmt.Errorf("getting workflow execution: %w", err)
		}
		if row.State != StateRunning || fromNullString(row.LeaseOwner) != owner || row.Attempt != int64(attempt) || fromNullTime(row.LeaseUntil).Before(time.Now()) {
			return ErrNotFound
		}
		remaining, err := q.CountActivitiesAfterPosition(ctx, jobquery.CountActivitiesAfterPositionParams{
			ExecutionID: executionID,
			Position:    int64(position),
		})
		if err != nil {
			return fmt.Errorf("checking workflow replay: %w", err)
		}
		if remaining != 0 {
			return fmt.Errorf("workflow replay ended before %d recorded activities", remaining)
		}
		return nil
	})
}

func (d *Store) ListEvents(ctx context.Context, executionID string) ([]Event, error) {
	rows, err := d.read.ListExecutionEvents(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("listing execution events: %w", err)
	}
	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, Event{
			Sequence:    row.Sequence,
			ExecutionID: row.ExecutionID,
			Type:        row.EventType,
			Data:        []byte(row.Data),
			CreatedAt:   time.Unix(row.CreatedAt, 0),
		})
	}
	return events, nil
}

func validateSubmit(req SubmitRequest) error {
	if req.ID == "" || req.Kind == "" || req.DefinitionID == "" || req.Trigger == "" || req.DedupeKey == "" {
		return errors.New("execution identity is required")
	}
	if len(req.Payload) == 0 {
		return errors.New("execution payload is required")
	}
	if req.MaxAttempts < 1 {
		return errors.New("execution max attempts must be positive")
	}
	if req.RetryInitialDelay < time.Second || req.RetryMaxDelay < req.RetryInitialDelay {
		return errors.New("invalid execution retry delays")
	}
	if req.RunAt.IsZero() {
		return errors.New("execution run time is required")
	}
	if slices.Contains(req.Resources, "") {
		return errors.New("execution resource key is empty")
	}
	return nil
}

func normalizedResources(resources []string) []string {
	seen := make(map[string]struct{}, len(resources))
	keys := make([]string, 0, len(resources))
	for _, resource := range resources {
		if _, dup := seen[resource]; !dup {
			seen[resource] = struct{}{}
			keys = append(keys, resource)
		}
	}
	return keys
}

func createEvent(ctx context.Context, q *jobquery.Queries, executionID, eventType string, data map[string]string) error {
	if data == nil {
		data = map[string]string{}
	}
	encoded, err := jsonMarshal(data)
	if err != nil {
		return err
	}
	if err := q.CreateExecutionEvent(ctx, jobquery.CreateExecutionEventParams{
		ExecutionID: executionID,
		EventType:   eventType,
		Data:        encoded,
		CreatedAt:   time.Now().Unix(),
	}); err != nil {
		return fmt.Errorf("creating execution event: %w", err)
	}
	return nil
}

func executionFromRow(row jobquery.JobExecution) Execution {
	return Execution{
		ID:                row.ID,
		Kind:              row.Kind,
		WorkflowVersion:   row.WorkflowVersion,
		DefinitionID:      row.DefinitionID,
		Trigger:           row.Trigger,
		DedupeKey:         row.DedupeKey,
		Payload:           []byte(row.Payload),
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

func activityFromRow(row jobquery.JobExecutionActivity) Activity {
	return Activity{
		ExecutionID: row.ExecutionID,
		Name:        row.Name,
		Position:    int(row.Position),
		InputHash:   row.InputHash,
		State:       row.State,
		Attempt:     int(row.Attempt),
		Result:      nullBytes(row.Result),
		Checkpoint:  nullBytes(row.Checkpoint),
		LastError:   fromNullString(row.LastError),
		CreatedAt:   time.Unix(row.CreatedAt, 0),
		StartedAt:   fromNullTime(row.StartedAt),
		CompletedAt: fromNullTime(row.CompletedAt),
	}
}
