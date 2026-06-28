-- name: UpsertMediaFamily :execlastid
INSERT INTO media_families (id, name, total_tapes, has_catalog, last_scanned)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    name = excluded.name,
    total_tapes = MAX(media_families.total_tapes, excluded.total_tapes),
    has_catalog = MAX(media_families.has_catalog, excluded.has_catalog),
    last_scanned = excluded.last_scanned;

-- name: GetMediaFamily :one
SELECT * FROM media_families WHERE id = ? LIMIT 1;

-- name: ListMediaFamilies :many
SELECT * FROM media_families ORDER BY name;

-- name: UpdateMediaFamilyScan :exec
UPDATE media_families SET last_scanned = ? WHERE id = ?;

-- name: DeleteMediaFamily :execrows
DELETE FROM media_families WHERE id = ?;

-- name: CreateDataSet :execlastid
INSERT INTO data_sets (
    media_family_id, set_number, name, description, owner, machine_name,
    write_time, num_directories, num_files, num_corrupt, size, sset_pba,
    first_media_seq, source_media_seq
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetDataSet :one
SELECT * FROM data_sets WHERE id = ? LIMIT 1;

-- name: FindDataSet :one
SELECT * FROM data_sets WHERE media_family_id = ? AND set_number = ? LIMIT 1;

-- name: ListDataSetsByFamily :many
SELECT * FROM data_sets WHERE media_family_id = ? ORDER BY set_number;

-- name: ListAllDataSets :many
SELECT * FROM data_sets ORDER BY write_time DESC;

-- name: DeleteDataSetsByFamily :execrows
DELETE FROM data_sets WHERE media_family_id = ?;

-- name: UpsertDataSet :one
INSERT INTO data_sets (
    media_family_id, set_number, name, description, owner, machine_name,
    write_time, num_directories, num_files, num_corrupt, size, sset_pba,
    first_media_seq, source_media_seq
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(media_family_id, set_number) DO UPDATE SET
    name = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.name ELSE data_sets.name END,
    description = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.description ELSE data_sets.description END,
    owner = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.owner ELSE data_sets.owner END,
    machine_name = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.machine_name ELSE data_sets.machine_name END,
    write_time = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.write_time ELSE data_sets.write_time END,
    num_directories = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.num_directories ELSE data_sets.num_directories END,
    num_files = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.num_files ELSE data_sets.num_files END,
    num_corrupt = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.num_corrupt ELSE data_sets.num_corrupt END,
    size = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.size ELSE data_sets.size END,
    sset_pba = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.sset_pba ELSE data_sets.sset_pba END,
    first_media_seq = CASE WHEN excluded.source_media_seq >= data_sets.source_media_seq THEN excluded.first_media_seq ELSE data_sets.first_media_seq END,
    source_media_seq = CASE WHEN excluded.source_media_seq > data_sets.source_media_seq THEN excluded.source_media_seq ELSE data_sets.source_media_seq END
RETURNING id;

-- name: CreateDataSetVolume :exec
INSERT INTO data_set_volumes (data_set_id, device, volume_label, machine_name, mapped_namespace)
VALUES (?, ?, ?, ?, ?);

-- name: ListVolumesByDataSet :many
SELECT * FROM data_set_volumes WHERE data_set_id = ? ORDER BY id;

-- name: DeleteVolumesByDataSet :execrows
DELETE FROM data_set_volumes WHERE data_set_id = ?;
