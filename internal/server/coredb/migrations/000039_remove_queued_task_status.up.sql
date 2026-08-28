UPDATE backups
SET last_run_upid = NULL, last_run_status = 0
WHERE last_run_upid LIKE '%pbsplusgen-queue%';

UPDATE restores
SET last_run_upid = NULL, last_run_status = 0
WHERE last_run_upid LIKE '%pbsplusgen-queue%';

UPDATE verification_jobs
SET last_run_upid = NULL, last_run_status = 0
WHERE last_run_upid LIKE '%pbsplusgen-queue%';
