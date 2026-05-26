-- Revert to single-column PK on path.
-- NOTE: data loss will occur if the same path exists under multiple job_ids.

CREATE TABLE exclusions_old (
  path TEXT PRIMARY KEY,
  job_id TEXT,
  comment TEXT
);

INSERT OR IGNORE INTO exclusions_old (path, job_id, comment)
SELECT path, job_id, comment
FROM exclusions;

DROP TABLE exclusions;

ALTER TABLE exclusions_old RENAME TO exclusions;
