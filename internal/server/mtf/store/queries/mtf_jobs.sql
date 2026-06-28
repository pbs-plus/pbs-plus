-- name: CreateMtfJob :exec
INSERT INTO mtf_jobs (
    id, source_kind, source_ref, datastore, namespace, comment,
    notification_mode, spanning, overwrite_mappings, changer, drive,
    current_pid, last_run_upid, last_successful_upid,
    last_run_status, retry_count,
    last_run_starttime, last_run_endtime, last_successful_endtime, duration
) VALUES (
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?, ?,
    ?, ?,
    ?, ?, ?, ?
);

-- name: GetMtfJob :one
SELECT * FROM mtf_jobs WHERE id = ? LIMIT 1;

-- name: ListAllMtfJobs :many
SELECT * FROM mtf_jobs ORDER BY id;

-- name: ListQueuedMtfJobs :many
SELECT * FROM mtf_jobs WHERE last_run_upid LIKE '%pbsplusgen-queue%' ORDER BY id;

-- name: UpdateMtfJob :exec
UPDATE mtf_jobs
SET source_kind = ?, source_ref = ?, datastore = ?, namespace = ?,
    comment = ?, notification_mode = ?, spanning = ?,
    overwrite_mappings = ?, changer = ?, drive = ?,
    last_run_upid = ?, last_successful_upid = ?,
    last_run_status = ?, retry_count = ?,
    last_run_starttime = ?, last_run_endtime = ?,
    last_successful_endtime = ?, duration = ?
WHERE id = ?;

-- name: UpdateMtfJobHistory :exec
UPDATE mtf_jobs
SET current_pid = ?, last_run_upid = ?, last_successful_upid = ?,
    last_run_status = ?, retry_count = ?,
    last_run_starttime = ?, last_run_endtime = ?,
    last_successful_endtime = ?, duration = ?
WHERE id = ?;

-- name: DeleteMtfJob :execrows
DELETE FROM mtf_jobs WHERE id = ?;

-- name: MtfJobExists :one
SELECT 1 FROM mtf_jobs WHERE id = ? LIMIT 1;

-- name: CountMtfJobs :one
SELECT COUNT(*) FROM mtf_jobs;
