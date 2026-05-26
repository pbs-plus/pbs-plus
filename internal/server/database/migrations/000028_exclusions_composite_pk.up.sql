-- Change exclusions PK from (path) to (job_id, path) so the same
-- exclusion pattern can be reused across different backup jobs.

CREATE TABLE exclusions_new (
  job_id TEXT NOT NULL DEFAULT '',
  path   TEXT NOT NULL,
  comment TEXT,
  PRIMARY KEY (job_id, path)
);

INSERT INTO exclusions_new (job_id, path, comment)
SELECT job_id, path, comment
FROM exclusions;

DROP TABLE exclusions;

ALTER TABLE exclusions_new RENAME TO exclusions;
