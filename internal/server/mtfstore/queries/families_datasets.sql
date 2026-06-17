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
    write_time, num_directories, num_files, num_corrupt, size, first_media_seq
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetDataSet :one
SELECT * FROM data_sets WHERE id = ? LIMIT 1;

-- name: ListDataSetsByFamily :many
SELECT * FROM data_sets WHERE media_family_id = ? ORDER BY set_number;

-- name: ListAllDataSets :many
SELECT * FROM data_sets ORDER BY write_time DESC;

-- name: DeleteDataSetsByFamily :execrows
DELETE FROM data_sets WHERE media_family_id = ?;

-- name: CreateDataSetVolume :exec
INSERT INTO data_set_volumes (data_set_id, device, volume_label, machine_name, mapped_namespace)
VALUES (?, ?, ?, ?, ?);

-- name: ListVolumesByDataSet :many
SELECT * FROM data_set_volumes WHERE data_set_id = ? ORDER BY id;

-- name: DeleteVolumesByDataSet :execrows
DELETE FROM data_set_volumes WHERE data_set_id = ?;
