-- name: CreateAgentHost :exec
INSERT INTO agent_hosts (name, ip, auth, token_used, os)
VALUES (?, ?, ?, ?, ?);

-- name: UpdateAgentHost :exec
UPDATE agent_hosts SET
    ip = ?, auth = ?, token_used = ?, os = ?
WHERE name = ?;

-- name: DeleteAgentHost :execrows
DELETE FROM agent_hosts WHERE name = ?;

-- name: GetAgentHost :one
SELECT name, ip, auth, token_used, os
FROM agent_hosts
WHERE name = ?;

-- name: ListAllAgentHosts :many
SELECT name, ip, auth, token_used, os
FROM agent_hosts
ORDER BY name;

-- name: GetAgentHostAuth :one
SELECT auth FROM agent_hosts WHERE name = ?;

-- name: AgentHostExists :one
SELECT 1 FROM agent_hosts WHERE name = ? LIMIT 1;
