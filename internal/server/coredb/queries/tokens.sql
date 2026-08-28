-- name: CreateToken :exec
INSERT INTO tokens (token, comment, created_at, revoked)
VALUES (?, ?, ?, ?);

-- name: GetToken :one
SELECT token, comment, created_at, revoked
FROM tokens
WHERE token = ?;

-- name: ListAllTokens :many
SELECT token, comment, created_at, revoked
FROM tokens
ORDER BY created_at DESC;

-- name: ListNonRevokedTokens :many
SELECT token, comment, created_at, revoked
FROM tokens
WHERE revoked = 0
ORDER BY created_at DESC;

-- name: RevokeToken :exec
UPDATE tokens 
SET revoked = 1 
WHERE token = ?;

-- name: DeleteToken :exec
DELETE FROM tokens WHERE token = ?;

-- name: ListAllTokensWithDetails :many
SELECT token, created_at, revoked
FROM tokens;
