-- name: UpsertCartridge :exec
INSERT INTO mtf_cartridges (
    barcode, label, media_family_id, sequence, role, catalog_type,
    is_bkf_file, source_path,
    volumes, directories, files, empty_files, file_bytes,
    sparse_files, compressed_files, encrypted_files,
    has_catalog, catalog_bytes, sets_closed,
    status, last_scanned
) VALUES (
    ?, ?, ?, ?, ?, ?,
    ?, ?,
    ?, ?, ?, ?, ?,
    ?, ?, ?,
    ?, ?, ?,
    ?, ?
)
ON CONFLICT(barcode) DO UPDATE SET
    label = excluded.label,
    media_family_id = excluded.media_family_id,
    sequence = excluded.sequence,
    role = excluded.role,
    catalog_type = excluded.catalog_type,
    is_bkf_file = excluded.is_bkf_file,
    source_path = excluded.source_path,
    volumes = excluded.volumes,
    directories = excluded.directories,
    files = excluded.files,
    empty_files = excluded.empty_files,
    file_bytes = excluded.file_bytes,
    sparse_files = excluded.sparse_files,
    compressed_files = excluded.compressed_files,
    encrypted_files = excluded.encrypted_files,
    has_catalog = excluded.has_catalog,
    catalog_bytes = excluded.catalog_bytes,
    sets_closed = excluded.sets_closed,
    status = excluded.status,
    last_scanned = excluded.last_scanned;

-- name: GetCartridge :one
SELECT * FROM mtf_cartridges WHERE barcode = ? LIMIT 1;

-- name: ListCartridges :many
SELECT * FROM mtf_cartridges ORDER BY media_family_id, sequence;

-- name: ListCartridgesByFamily :many
SELECT * FROM mtf_cartridges WHERE media_family_id = ? ORDER BY sequence;

-- name: CartridgeExists :one
SELECT 1 FROM mtf_cartridges WHERE barcode = ? LIMIT 1;

-- name: DeleteCartridge :execrows
DELETE FROM mtf_cartridges WHERE barcode = ?;

-- name: CreateInventoryRun :execlastid
INSERT INTO mtf_inventory_runs (changer, started_at, status) VALUES (?, ?, 'running');

-- name: CompleteInventoryRun :exec
UPDATE mtf_inventory_runs
SET completed_at = ?, status = ?, cartridges = ?, message = ?
WHERE id = ?;

-- name: ListInventoryRuns :many
SELECT * FROM mtf_inventory_runs ORDER BY started_at DESC LIMIT ?;

-- name: GetInventoryRun :one
SELECT * FROM mtf_inventory_runs WHERE id = ? LIMIT 1;
