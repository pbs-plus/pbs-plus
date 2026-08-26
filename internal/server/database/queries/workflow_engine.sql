-- name: CreateWorkflowExecution :exec
INSERT INTO job_executions (
    id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, created_at,
    parent_execution_id
) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?);

-- name: GetWorkflowExecution :one
SELECT id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, lease_owner,
    lease_until, cancel_requested, last_error, parent_execution_id, created_at,
    started_at, finished_at
FROM job_executions
WHERE id = ?;

-- name: GetWorkflowExecutionByDedupeKey :one
SELECT id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, lease_owner,
    lease_until, cancel_requested, last_error, parent_execution_id, created_at,
    started_at, finished_at
FROM job_executions
WHERE dedupe_key = ?;

-- name: GetActiveWorkflowExecutionByDefinition :one
SELECT id, kind, definition_id, trigger, dedupe_key, payload, state, attempt,
    max_attempts, retry_initial_seconds, retry_max_seconds, run_at, lease_owner,
    lease_until, cancel_requested, last_error, parent_execution_id, created_at,
    started_at, finished_at
FROM job_executions
WHERE kind = ? AND definition_id = ? AND state IN ('pending', 'running')
ORDER BY created_at DESC
LIMIT 1;

-- name: CreateWorkflowExecutionResource :exec
INSERT INTO job_execution_resources (execution_id, resource_key) VALUES (?, ?);

-- name: ListClaimableWorkflowExecutionIDs :many
SELECT id
FROM job_executions
WHERE state = 'pending' AND cancel_requested = 0 AND run_at <= ?
ORDER BY run_at, created_at
LIMIT 32;

-- name: RequeueExpiredWorkflowExecutions :exec
UPDATE job_executions
SET state = 'pending', lease_owner = NULL, lease_until = NULL, run_at = ?
WHERE state = 'running' AND lease_until < ? AND cancel_requested = 0;

-- name: ClaimWorkflowExecution :execrows
UPDATE job_executions
SET state = 'running', attempt = attempt + 1, lease_owner = ?, lease_until = ?,
    started_at = COALESCE(started_at, ?)
WHERE id = ? AND state = 'pending' AND cancel_requested = 0 AND run_at <= ?;

-- name: DeleteExpiredWorkflowResourceLocks :exec
DELETE FROM job_resource_locks WHERE lease_until < ?;

-- name: CreateWorkflowResourceLock :execrows
INSERT INTO job_resource_locks (resource_key, execution_id, lease_until)
VALUES (?, ?, ?)
ON CONFLICT(resource_key) DO UPDATE SET
    execution_id = excluded.execution_id,
    lease_until = excluded.lease_until
WHERE job_resource_locks.execution_id = excluded.execution_id;

-- name: DeleteWorkflowResourceLocks :exec
DELETE FROM job_resource_locks WHERE execution_id = ?;

-- name: RenewWorkflowResourceLocks :exec
UPDATE job_resource_locks SET lease_until = ? WHERE execution_id = ?;

-- name: ListWorkflowExecutionResources :many
SELECT resource_key FROM job_execution_resources WHERE execution_id = ? ORDER BY resource_key;

-- name: DelayWorkflowExecution :exec
UPDATE job_executions SET run_at = ? WHERE id = ? AND state = 'pending';

-- name: ReleaseWorkflowExecutionClaim :exec
UPDATE job_executions
SET state = 'pending', lease_owner = NULL, lease_until = NULL, run_at = ?
WHERE id = ? AND state = 'running' AND lease_owner = ?;

-- name: RenewWorkflowExecutionLease :execrows
UPDATE job_executions SET lease_until = ?
WHERE id = ? AND state = 'running' AND lease_owner = ?;

-- name: RequestWorkflowExecutionCancellation :execrows
UPDATE job_executions SET cancel_requested = 1
WHERE id = ? AND state IN ('pending', 'running');

-- name: CancelPendingWorkflowExecution :execrows
UPDATE job_executions
SET state = 'canceled', finished_at = ?, lease_owner = NULL, lease_until = NULL
WHERE id = ? AND state = 'pending' AND cancel_requested = 1;

-- name: FinishWorkflowExecution :execrows
UPDATE job_executions
SET state = ?, run_at = ?, lease_owner = NULL, lease_until = NULL,
    last_error = ?, finished_at = ?
WHERE id = ? AND state = 'running' AND lease_owner = ?;

-- name: CreateWorkflowExecutionEvent :exec
INSERT INTO job_execution_events (execution_id, event_type, data, created_at)
VALUES (?, ?, ?, ?);

-- name: ListWorkflowExecutionEvents :many
SELECT sequence, execution_id, event_type, data, created_at
FROM job_execution_events
WHERE execution_id = ?
ORDER BY sequence;

-- name: GetWorkflowActivity :one
SELECT execution_id, name, input_hash, state, attempt, result, checkpoint,
    last_error, created_at, started_at, completed_at
FROM job_execution_activities
WHERE execution_id = ? AND name = ?;

-- name: CreateWorkflowActivity :execrows
INSERT INTO job_execution_activities (
    execution_id, name, input_hash, state, created_at
) VALUES (?, ?, ?, 'pending', ?)
ON CONFLICT(execution_id, name) DO NOTHING;

-- name: StartWorkflowActivity :execrows
UPDATE job_execution_activities
SET state = 'running', attempt = attempt + 1, started_at = ?, last_error = NULL
WHERE execution_id = ? AND name = ? AND state IN ('pending', 'running');

-- name: CheckpointWorkflowActivity :execrows
UPDATE job_execution_activities
SET checkpoint = ?
WHERE execution_id = ? AND name = ? AND state = 'running';

-- name: CompleteWorkflowActivity :execrows
UPDATE job_execution_activities
SET state = 'completed', result = ?, completed_at = ?
WHERE execution_id = ? AND name = ? AND state = 'running';

-- name: FailWorkflowActivity :execrows
UPDATE job_execution_activities
SET state = 'pending', last_error = ?
WHERE execution_id = ? AND name = ? AND state = 'running';
