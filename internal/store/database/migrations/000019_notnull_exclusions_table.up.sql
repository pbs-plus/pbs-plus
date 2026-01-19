CREATE TABLE exclusions_new (
  path TEXT PRIMARY KEY,
  job_id TEXT NOT NULL DEFAULT '',
  comment TEXT
);

INSERT INTO exclusions_new (path, job_id, comment)
SELECT path, COALESCE(job_id, ''), comment
FROM exclusions;

DROP TABLE exclusions;

ALTER TABLE exclusions_new RENAME TO exclusions;
