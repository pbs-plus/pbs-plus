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

target_connected() {
	local target="$1"
	api "https://localhost:8017/api2/json/d2d/target" |
		jq -r --arg t "$target" \
			'[.data[] | select(.name == $t) | .connection_status] | first // false'
}

wait_for_agent_reconnect() {
	local target="$1"
	local timeout="$2"
	local start=$SECONDS
	local deadline=$((SECONDS + timeout))
	local status next_report=0

	printf 'waiting for the server to see %s reconnect (timeout %ss)\n' \
		"$target" "$timeout"

	while ((SECONDS < deadline)); do
		status=$(target_connected "$target")
		if [[ "$status" == true ]]; then
			printf 'agent session re-established after %ss\n' "$((SECONDS - start))"
			return 0
		fi
		if ((SECONDS >= next_report)); then
			printf '  ... still disconnected at %ss (connection_status=%s)\n' \
				"$((SECONDS - start))" "$status"
			next_report=$((SECONDS + 15))
		fi
		sleep 2
	done

	printf 'agent did not reconnect within %ss\n' "$timeout"
	printf '=== targets as the server sees them ===\n'
	api "https://localhost:8017/api2/json/d2d/target" |
		jq -r '.data[] | "\(.name)\tconnected=\(.connection_status)\tversion=\(.agent_version)"' || true
	printf '=== agent logs ===\n'
	docker logs --tail=150 pbs-plus-agent
	printf '=== server logs ===\n'
	docker logs --tail=150 pbs-plus-test
	return 1
}

server_log_count() {
	local needle="$1"
	docker logs pbs-plus-test 2>&1 | grep -cF "$needle" || true
}

wait_for_server_log_increase() {
	local needle="$1"
	local before="$2"
	local timeout="$3"
	local start=$SECONDS
	local deadline=$((SECONDS + timeout))
	local seen

	printf 'waiting for the restarted server to log %q (had %s, timeout %ss)\n' \
		"$needle" "$before" "$timeout"

	while ((SECONDS < deadline)); do
		seen=$(server_log_count "$needle")
		if ((seen > before)); then
			printf 'restarted server logged %q after %ss (%s occurrences)\n' \
				"$needle" "$((SECONDS - start))" "$seen"
			return 0
		fi
		sleep 2
	done

	printf 'restarted server never logged %q within %ss\n' "$needle" "$timeout"
	docker logs --tail=200 pbs-plus-test 2>&1 | grep -Fi quic || true
	return 1
}

batch_pending() {
	local batch="$1"
	api "https://localhost:8017/api2/json/d2d/notification-batch/status" |
		jq -r --arg batch "$batch" '.data[$batch] // 0'
}

wait_for_server() {
	local timeout="$1"
	local start=$SECONDS
	local deadline=$((SECONDS + timeout))

	printf 'waiting for the pbs-plus API to answer again (timeout %ss)\n' "$timeout"

	while ((SECONDS < deadline)); do
		if api "https://localhost:8017/api2/json/d2d/notification-batch/status" >/dev/null 2>&1; then
			printf 'server responsive again after %ss\n' "$((SECONDS - start))"
			return 0
		fi
		sleep 2
	done

	printf 'server did not come back within %ss\n' "$timeout"
	docker logs --tail=100 pbs-plus-test
	return 1
}
