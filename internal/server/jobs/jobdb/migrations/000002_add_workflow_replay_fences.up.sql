ALTER TABLE job_executions ADD COLUMN workflow_version TEXT NOT NULL DEFAULT '1';

ALTER TABLE job_execution_activities ADD COLUMN position INTEGER NOT NULL DEFAULT 0;

UPDATE job_execution_activities AS activity
SET position = (
    SELECT COUNT(*)
    FROM job_execution_activities AS prior
    WHERE prior.execution_id = activity.execution_id AND prior.rowid <= activity.rowid
);

CREATE UNIQUE INDEX job_execution_activities_position_idx
ON job_execution_activities(execution_id, position);
