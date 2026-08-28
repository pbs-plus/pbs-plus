#!/usr/bin/env bash

api() {
	docker exec pbs-plus-test curl -k -fsS "$@"
}

run_backup_job() {
	local job="$1"
	local encoded
	encoded=$(printf '%s' "$job" | base64 -w0)

	local code
	code=$(docker exec pbs-plus-test curl -k -sS -o /dev/null -w '%{http_code}' \
		-X POST "https://localhost:8017/api2/extjs/d2d/backup?job=${encoded}" \
		-H "Content-Type: application/x-www-form-urlencoded")
	if [[ "$code" != 200 ]]; then
		printf 'failed to queue %s: HTTP %s\n' "$job" "$code"
		return 1
	fi
	printf 'queued %s\n' "$job"
}

wait_for_backup() {
	local job="$1"
	local timeout="$2"
	local deadline=$((SECONDS + timeout))
	local history state

	while ((SECONDS < deadline)); do
		history=$(api "https://localhost:8017/api2/json/d2d/backup" -H "Accept: application/json")
		state=$(jq -r --arg job "$job" '.data[] | select(.id == $job) | ."last-run-state" // empty' <<<"$history")
		if [[ "$state" == OK ]]; then
			printf '%s finished\n' "$job"
			return 0
		fi
		sleep 2
	done

	printf '%s did not reach state OK within %ss\n' "$job" "$timeout"
	jq -r --arg job "$job" '.data[] | select(.id == $job)' <<<"${history:-}"
	return 1
}

agent_connect_count() {
	docker logs pbs-plus-agent 2>&1 |
		grep -cE "tls: connection established|quic: connection established" || true
}

wait_for_agent_reconnect() {
	local before="$1"
	local timeout="$2"
	local deadline=$((SECONDS + timeout))

	while ((SECONDS < deadline)); do
		if (($(agent_connect_count) > before)); then
			printf 'agent reconnected\n'
			return 0
		fi
		sleep 2
	done

	printf 'agent did not reconnect within %ss\n' "$timeout"
	docker logs --tail=100 pbs-plus-agent
	return 1
}

batch_pending() {
	local batch="$1"
	api "https://localhost:8017/api2/json/d2d/notification-batch/status" |
		jq -r --arg batch "$batch" '.data[$batch] // 0'
}

wait_for_server() {
	local timeout="$1"
	local deadline=$((SECONDS + timeout))

	while ((SECONDS < deadline)); do
		if api "https://localhost:8017/api2/json/d2d/notification-batch/status" >/dev/null 2>&1; then
			printf 'server responsive again\n'
			return 0
		fi
		sleep 2
	done

	printf 'server did not come back within %ss\n' "$timeout"
	docker logs --tail=100 pbs-plus-test
	return 1
}
