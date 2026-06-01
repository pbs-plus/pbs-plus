-- name: CreateNotificationBatch :exec
INSERT INTO notification_batches (name, comment, notification_mode, wait_timeout_secs, send_on_timeout)
VALUES (?, ?, ?, ?, ?);

-- name: GetNotificationBatch :one
SELECT * FROM notification_batches WHERE name = ?;

-- name: ListNotificationBatches :many
SELECT * FROM notification_batches ORDER BY name;

-- name: UpdateNotificationBatch :exec
UPDATE notification_batches
SET comment = ?, notification_mode = ?, wait_timeout_secs = ?, send_on_timeout = ?
WHERE name = ?;

-- name: DeleteNotificationBatch :exec
DELETE FROM notification_batches WHERE name = ?;

-- name: AddJobToBatch :exec
INSERT INTO notification_batch_jobs (batch_name, job_type, job_id)
VALUES (?, ?, ?);

-- name: RemoveJobFromBatch :exec
DELETE FROM notification_batch_jobs
WHERE batch_name = ? AND job_type = ? AND job_id = ?;

-- name: GetBatchJobsByBatch :many
SELECT * FROM notification_batch_jobs WHERE batch_name = ?;

-- name: GetBatchForJob :one
SELECT nb.*
FROM notification_batches nb
JOIN notification_batch_jobs nbj ON nb.name = nbj.batch_name
WHERE nbj.job_type = ? AND nbj.job_id = ?;

-- name: GetBatchJobsByJobType :many
SELECT * FROM notification_batch_jobs WHERE job_type = ?;

-- name: RemoveJobsByBatch :exec
DELETE FROM notification_batch_jobs WHERE batch_name = ?;

-- name: RemoveJobFromAllBatches :exec
DELETE FROM notification_batch_jobs WHERE job_type = ? AND job_id = ?;

-- name: ListBatchJobs :many
SELECT * FROM notification_batch_jobs ORDER BY batch_name, job_type, job_id;
