ALTER TABLE data_sets ADD COLUMN source_media_seq INTEGER DEFAULT 0;
UPDATE data_sets SET source_media_seq = first_media_seq WHERE source_media_seq = 0;
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_sets_family_set ON data_sets(media_family_id, set_number);
