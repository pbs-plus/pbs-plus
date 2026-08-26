CREATE TABLE exclusions_old (
  path TEXT PRIMARY KEY,
  job_id TEXT,
  comment TEXT
);

INSERT INTO exclusions_old (path, job_id, comment)
SELECT path, job_id, comment
FROM exclusions;

DROP TABLE exclusions;

ALTER TABLE exclusions_old RENAME TO exclusions;
