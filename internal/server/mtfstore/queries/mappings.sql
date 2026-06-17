-- name: CreateMapping :execlastid
INSERT INTO namespace_mappings (
    name, priority, match_regex, template, is_default, enabled, comment
) VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: GetMapping :one
SELECT * FROM namespace_mappings WHERE id = ? LIMIT 1;

-- name: ListMappings :many
SELECT * FROM namespace_mappings ORDER BY priority ASC, id ASC;

-- name: ListEnabledMappings :many
SELECT * FROM namespace_mappings WHERE enabled = 1 ORDER BY priority ASC, id ASC;

-- name: UpdateMapping :exec
UPDATE namespace_mappings
SET name = ?, priority = ?, match_regex = ?, template = ?,
    is_default = ?, enabled = ?, comment = ?
WHERE id = ?;

-- name: DeleteMapping :execrows
DELETE FROM namespace_mappings WHERE id = ?;

-- name: MappingExists :one
SELECT 1 FROM namespace_mappings WHERE id = ? LIMIT 1;

-- name: CountMappings :one
SELECT COUNT(*) FROM namespace_mappings;
