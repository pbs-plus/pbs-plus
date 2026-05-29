-- name: CreateVerificationJob :exec
INSERT INTO verification_jobs (
    id, backup_job_id, store, namespace, mode, schedule, comment,
    spot_config, last_run_upid, last_successful_upid,
    last_run_status, retry_count, retry, retry_interval,
    last_run_starttime, last_run_endtime, last_successful_endtime,
    run_on_backup_complete, pending_since, notification_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetVerificationJob :one
SELECT *
FROM verification_jobs
WHERE id = ?
LIMIT 1;

-- name: ListAllVerificationJobs :many
SELECT *
FROM verification_jobs
ORDER BY id;

-- name: UpdateVerificationJob :exec
UPDATE verification_jobs
SET backup_job_id = ?, store = ?, namespace = ?, mode = ?, schedule = ?,
    comment = ?, spot_config = ?, last_run_upid = ?, last_successful_upid = ?,
    last_run_status = ?, retry_count = ?, retry = ?, retry_interval = ?,
    last_run_starttime = ?, last_run_endtime = ?, last_successful_endtime = ?,
    run_on_backup_complete = ?, pending_since = ?, notification_mode = ?
WHERE id = ?;

-- name: DeleteVerificationJob :execrows
DELETE FROM verification_jobs WHERE id = ?;

-- name: VerificationJobExists :one
SELECT 1 FROM verification_jobs WHERE id = ? LIMIT 1;

-- name: CreateVerificationResult :execresult
INSERT INTO verification_results (
    verification_job_id, upid, snapshot, snapshot_time,
    total_files, verified_files, failed_files, skipped_files,
    status, started_at, completed_at, details, total_population
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetVerificationResults :many
SELECT *
FROM verification_results
WHERE verification_job_id = ?
ORDER BY started_at DESC;

-- name: GetLatestVerificationResult :one
SELECT *
FROM verification_results
WHERE verification_job_id = ?
ORDER BY started_at DESC
LIMIT 1;

-- name: UpdateVerificationResult :exec
UPDATE verification_results
SET upid = ?, total_files = ?, verified_files = ?, failed_files = ?,
    skipped_files = ?, status = ?, completed_at = ?, details = ?,
    total_population = ?
WHERE id = ?;

-- name: MarkVerificationResultStatus :exec
UPDATE verification_results
SET status = ?, completed_at = ?
WHERE id = ?;

-- name: DeleteVerificationResults :execrows
DELETE FROM verification_results WHERE verification_job_id = ?;
