-- name: CreateChanger :exec
INSERT INTO mtf_changers (name, device, comment) VALUES (?, ?, ?);

-- name: GetChanger :one
SELECT * FROM mtf_changers WHERE name = ? LIMIT 1;

-- name: ListChangers :many
SELECT * FROM mtf_changers ORDER BY name;

-- name: UpdateChanger :exec
UPDATE mtf_changers SET device = ?, comment = ? WHERE name = ?;

-- name: DeleteChanger :execrows
DELETE FROM mtf_changers WHERE name = ?;

-- name: CreateDrive :exec
INSERT INTO mtf_drives (name, device, changer, drive_index, comment)
VALUES (?, ?, ?, ?, ?);

-- name: GetDrive :one
SELECT * FROM mtf_drives WHERE name = ? LIMIT 1;

-- name: ListDrives :many
SELECT * FROM mtf_drives ORDER BY name;

-- name: UpdateDrive :exec
UPDATE mtf_drives SET device = ?, changer = ?, drive_index = ?, comment = ?
WHERE name = ?;

-- name: DeleteDrive :execrows
DELETE FROM mtf_drives WHERE name = ?;
