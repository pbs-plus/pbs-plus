-- name: CreateExecution :exec
INSERT INTO job_executions (
    id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, created_at,
    parent_execution_id, workflow_version
) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?);

-- name: GetExecution :one
SELECT id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, lease_owner,
    lease_until, cancel_requested, last_error, parent_execution_id, created_at,
    started_at, finished_at, workflow_version
FROM job_executions
WHERE id = ?;

-- name: GetExecutionByDedupeKey :one
SELECT id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, lease_owner,
    lease_until, cancel_requested, last_error, parent_execution_id, created_at,
    started_at, finished_at, workflow_version
FROM job_executions
WHERE dedupe_key = ?;

-- name: GetActiveExecutionByDefinition :one
SELECT id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, lease_owner,
    lease_until, cancel_requested, last_error, parent_execution_id, created_at,
    started_at, finished_at, workflow_version
FROM job_executions
WHERE kind = ? AND definition_id = ? AND state IN ('pending', 'running')
ORDER BY created_at DESC
LIMIT 1;

-- name: CreateExecutionResource :exec
INSERT INTO job_execution_resources (execution_id, resource_key) VALUES (?, ?);

-- name: ListClaimableExecutionIDs :many
SELECT id
FROM job_executions
WHERE state = 'pending' AND cancel_requested = 0 AND run_at <= ?
ORDER BY run_at, created_at
LIMIT 32;

-- name: ListExpiredCanceledExecutionIDs :many
SELECT id
FROM job_executions
WHERE state = 'running' AND cancel_requested = 1 AND lease_until < ?
ORDER BY lease_until;

-- name: CancelExpiredExecution :execrows
UPDATE job_executions
SET state = 'canceled', lease_owner = NULL, lease_until = NULL, finished_at = ?
WHERE id = ? AND state = 'running' AND cancel_requested = 1 AND lease_until < ?;

-- name: RequeueExpiredExecutions :many
UPDATE job_executions
SET state = 'pending', lease_owner = NULL, lease_until = NULL, run_at = ?
WHERE state = 'running' AND lease_until < ? AND cancel_requested = 0
RETURNING id;

-- name: ResetRunningActivities :exec
UPDATE job_execution_activities
SET state = 'pending'
WHERE execution_id = ? AND state = 'running';

-- name: ClaimExecution :execrows
UPDATE job_executions
SET state = 'running', attempt = attempt + 1, lease_owner = ?, lease_until = ?,
    started_at = COALESCE(started_at, ?)
WHERE id = ? AND state = 'pending' AND cancel_requested = 0 AND run_at <= ?;

-- name: DeleteExpiredResourceLocks :exec
DELETE FROM job_resource_locks WHERE lease_until < ?;

-- name: CreateResourceLock :execrows
INSERT INTO job_resource_locks (resource_key, execution_id, lease_until)
VALUES (?, ?, ?)
ON CONFLICT(resource_key) DO UPDATE SET
    execution_id = excluded.execution_id,
    lease_until = excluded.lease_until
WHERE job_resource_locks.execution_id = excluded.execution_id;

-- name: DeleteResourceLocks :exec
DELETE FROM job_resource_locks WHERE execution_id = ?;

-- name: RenewResourceLocks :exec
UPDATE job_resource_locks SET lease_until = ? WHERE execution_id = ?;

-- name: GetResourceLockHolder :one
SELECT execution_id FROM job_resource_locks
WHERE resource_key = ? AND lease_until >= ?
LIMIT 1;

-- name: ListExecutionResources :many
SELECT resource_key FROM job_execution_resources WHERE execution_id = ? ORDER BY resource_key;

-- name: DelayExecution :exec
UPDATE job_executions SET run_at = ? WHERE id = ? AND state = 'pending';

-- name: ReleaseExecutionClaim :exec
UPDATE job_executions
SET state = 'pending', lease_owner = NULL, lease_until = NULL, run_at = ?
WHERE id = ? AND state = 'running' AND lease_owner = ?;

-- name: RenewExecutionLease :execrows
UPDATE job_executions SET lease_until = ?
WHERE id = ? AND state = 'running' AND lease_owner = ? AND attempt = ?
  AND lease_until >= unixepoch();

-- name: RequestExecutionCancellation :execrows
UPDATE job_executions SET cancel_requested = 1
WHERE id = ? AND state IN ('pending', 'running');

-- name: CancelPendingExecution :execrows
UPDATE job_executions
SET state = 'canceled', finished_at = ?, lease_owner = NULL, lease_until = NULL
WHERE id = ? AND state = 'pending' AND cancel_requested = 1;

-- name: FinishExecution :execrows
UPDATE job_executions
SET state = ?, run_at = ?, lease_owner = NULL, lease_until = NULL,
    last_error = ?, finished_at = ?
WHERE id = ? AND state = 'running' AND lease_owner = ? AND attempt = ?
  AND lease_until >= unixepoch();

-- name: CreateExecutionEvent :exec
INSERT INTO job_execution_events (execution_id, event_type, data, created_at)
VALUES (?, ?, ?, ?);

-- name: ListExecutionEvents :many
SELECT sequence, execution_id, event_type, data, created_at
FROM job_execution_events
WHERE execution_id = ?
ORDER BY sequence;

-- name: GetActivity :one
SELECT execution_id, name, input_hash, state, attempt, result, checkpoint,
    last_error, created_at, started_at, completed_at, position
FROM job_execution_activities
WHERE execution_id = ? AND name = ?;

-- name: GetActivityAtPosition :one
SELECT execution_id, name, input_hash, state, attempt, result, checkpoint,
    last_error, created_at, started_at, completed_at, position
FROM job_execution_activities
WHERE execution_id = ? AND position = ?;

-- name: CountActivitiesAfterPosition :one
SELECT COUNT(*)
FROM job_execution_activities
WHERE execution_id = ? AND position > ?;

-- name: CreateActivity :execrows
INSERT INTO job_execution_activities (
    execution_id, name, input_hash, state, created_at, position
) VALUES (?, ?, ?, 'pending', ?, ?)
ON CONFLICT(execution_id, name) DO NOTHING;

-- name: StartActivity :execrows
UPDATE job_execution_activities
SET state = 'running', attempt = attempt + 1, started_at = ?, last_error = NULL
WHERE execution_id = ? AND name = ? AND state IN ('pending', 'running')
  AND EXISTS (
    SELECT 1
    FROM job_executions
    WHERE id = job_execution_activities.execution_id
      AND state = 'running'
      AND lease_owner = ?
      AND job_executions.attempt = ?
      AND lease_until >= unixepoch()
  );

-- name: CheckpointActivity :execrows
UPDATE job_execution_activities
SET checkpoint = ?
WHERE execution_id = ? AND name = ? AND state = 'running'
  AND EXISTS (
    SELECT 1
    FROM job_executions
    WHERE id = job_execution_activities.execution_id
      AND state = 'running'
      AND lease_owner = ?
      AND job_executions.attempt = ?
      AND lease_until >= unixepoch()
  );

-- name: CompleteActivity :execrows
UPDATE job_execution_activities
SET state = 'completed', result = ?, completed_at = ?
WHERE execution_id = ? AND name = ? AND state = 'running'
  AND EXISTS (
    SELECT 1
    FROM job_executions
    WHERE id = job_execution_activities.execution_id
      AND state = 'running'
      AND lease_owner = ?
      AND job_executions.attempt = ?
      AND lease_until >= unixepoch()
  );

-- name: InvalidateActivity :execrows
UPDATE job_execution_activities
SET state = 'pending', result = NULL, checkpoint = NULL, completed_at = NULL
WHERE execution_id = ? AND name = ? AND state = 'completed'
  AND EXISTS (
    SELECT 1
    FROM job_executions
    WHERE id = job_execution_activities.execution_id
      AND state = 'running'
      AND lease_owner = ?
      AND job_executions.attempt = ?
      AND lease_until >= unixepoch()
  );

-- name: FailActivity :execrows
UPDATE job_execution_activities
SET state = 'pending', last_error = ?
WHERE execution_id = ? AND name = ? AND state = 'running'
  AND EXISTS (
    SELECT 1
    FROM job_executions
    WHERE id = job_execution_activities.execution_id
      AND state = 'running'
      AND lease_owner = ?
      AND job_executions.attempt = ?
      AND lease_until >= unixepoch()
  );
