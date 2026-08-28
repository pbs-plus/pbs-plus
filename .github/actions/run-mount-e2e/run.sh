#!/bin/bash
set -uo pipefail

PASS=0
FAIL=0
SKIP=0

ok()   { echo "   $1"; ((PASS++)); }
fail() { echo "   $1"; ((FAIL++)); }
skip() { echo "  ⊘ $1"; ((SKIP++)); }

section() { echo ""; echo "══════════════════════════════════════════"; echo "  $1"; echo "══════════════════════════════════════════"; }

die() { echo "FATAL: $1"; dump_logs; exit 1; }

dump_logs() {
	echo "--- server log (last 30 lines) ---"
	docker logs --tail 30 pbs-plus-test 2>&1 || true
	echo "--- mount process logs ---"
	docker exec pbs-plus-test sh -c 'cat /var/run/pbs-plus-mounts/*.log 2>/dev/null | tail -40' || true
	echo "--- end logs ---"
}

PBS_API="https://localhost:8017"
DATASTORE="test"
NAMESPACE="test"
HOST_DIR="/mnt/test/ns/test/host/test-host"
INIT_GROUP_DIR="/mnt/test/ns/test/host/e2e-init"
ENC_DS=$(printf %s "$DATASTORE" | base64 -w0)
MOUNT_BASE="/mnt/pbs-plus-restores"

req() { docker exec pbs-plus-test curl -k -s "$@" -w "\nHTTP_CODE:%{http_code}"; }

code_of() { tail -1 <<<"$1" | sed 's/^HTTP_CODE://'; }
body_of() { sed '$d' <<<"$1"; }

api_post() {
	local path=$1; shift
	req -X POST "$PBS_API$path" -H "Content-Type: application/x-www-form-urlencoded" "$@"
}
api_get() { req "$PBS_API$1"; }

sessions_field() {
	docker exec pbs-plus-test curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -r "$@" 2>/dev/null
}

wait_for() {
	local desc=$1 timeout=$2; shift 2
	local deadline=$((SECONDS + timeout))
	while [ $SECONDS -lt $deadline ]; do
		if "$@" >/dev/null 2>&1; then
			ok "$desc"
			return 0
		fi
		sleep 2
	done
	fail "$desc (timeout after ${timeout}s)"
	dump_logs
	return 1
}

session_mounted() { [ "$(sessions_field --arg mp "$1" '.data[]? | select(.["mount-point"]==$mp) | .mounted' | head -1)" = "true" ]; }
session_gone()    { [ -z "$(sessions_field --arg mp "$1" '.data[]? | select(.["mount-point"]==$mp) | .["mount-point"]' | head -1)" ]; }

latest_snapshot() {
	docker exec pbs-plus-test sh -c "ls -1 '$1' 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1"
}

submit_ok() {
	local code
	code=$(code_of "$1")
	[ "$code" = "200" ] && grep -q '"success": *true\|"success":true' <<<"$(body_of "$1")"
}

section "PHASE 1: Mount existing snapshot read-only via API"

SNAP=$(latest_snapshot "$HOST_DIR")
[ -n "$SNAP" ] || die "no snapshot found under $HOST_DIR"
echo "Using snapshot: $SNAP"
MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-test-host/$SNAP"

RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-host" \
	-d "backup-time=$SNAP" -d "mode=ro")
submit_ok "$RESP" && ok "mount request accepted" || fail "mount request rejected: $(body_of "$RESP")"

wait_for "ro session mounted at $MP" 90 session_mounted "$MP" || true

docker exec pbs-plus-test mountpoint -q "$MP" && ok "mountpoint active" || fail "mountpoint not active"
FILES=$(docker exec pbs-plus-test ls "$MP" 2>/dev/null | head -3)
[ -n "$FILES" ] && ok "archive content listed" || fail "archive empty"

MODE=$(sessions_field --arg mp "$MP" '.data[]? | select(.["mount-point"]==$mp) | .mode' | head -1)
[ "$MODE" = "ro" ] && ok "session mode is ro" || fail "session mode = ${MODE:-missing}"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$MP")
submit_ok "$RESP" && ok "unmount request accepted" || fail "unmount rejected: $(body_of "$RESP")"

wait_for "ro session unmounted" 60 session_gone "$MP" || true
docker exec pbs-plus-test test ! -e "$MP" && ok "mountpoint cleaned up" || fail "mountpoint still exists"

section "PHASE 2: Init new archive, write, commit via API"

INIT_MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-e2e-init/init"

RESP=$(api_post "/api2/extjs/config/d2d-init/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init")
submit_ok "$RESP" && ok "init request accepted" || fail "init request rejected: $(body_of "$RESP")"

wait_for "init session mounted at $INIT_MP" 90 session_mounted "$INIT_MP" || true

CAP=$(sessions_field --arg mp "$INIT_MP" '.data[]? | select(.["mount-point"]==$mp) | .["commit-capable"]' | head -1)
[ "$CAP" = "true" ] && ok "init session commit-capable" || fail "init session not commit-capable (${CAP:-missing})"

docker exec pbs-plus-test sh -c "echo hello-e2e > '$INIT_MP/hello.txt'"
docker exec pbs-plus-test sh -c "mkdir -p '$INIT_MP/nested' && echo nested-e2e > '$INIT_MP/nested/file.txt'"
[ "$(docker exec pbs-plus-test cat "$INIT_MP/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "wrote and read hello.txt through mount" || fail "write/read through init mount failed"

BEFORE=$(latest_snapshot "$INIT_GROUP_DIR")
RESP=$(api_post "/api2/extjs/config/d2d-commit/$ENC_DS" -d "mount-path=$INIT_MP")
submit_ok "$RESP" && ok "commit request accepted" || fail "commit rejected: $(body_of "$RESP")"

wait_for "commit produced new snapshot dir" 180 bash -c "
	[ \"\$(docker exec pbs-plus-test sh -c \"ls -1 '$INIT_GROUP_DIR' 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1\")\" != '$BEFORE' ]" || true

NEW_SNAP=$(latest_snapshot "$INIT_GROUP_DIR")
[ -n "$NEW_SNAP" ] && [ "$NEW_SNAP" != "$BEFORE" ] && ok "new snapshot: $NEW_SNAP" || die "no new snapshot after commit"
docker exec pbs-plus-test sh -c "ls '$INIT_GROUP_DIR/$NEW_SNAP'/*.didx >/dev/null 2>&1" \
	&& ok "didx present in new snapshot" || fail "no didx in new snapshot"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$INIT_MP" -d "force=1")
submit_ok "$RESP" && ok "init unmount request accepted" || fail "init unmount rejected: $(body_of "$RESP")"
wait_for "init session unmounted" 60 session_gone "$INIT_MP" || true

section "PHASE 3: Remount committed snapshot, verify data"

MP3="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-e2e-init/$NEW_SNAP"
RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "mode=ro")
submit_ok "$RESP" && ok "remount request accepted" || fail "remount rejected: $(body_of "$RESP")"

wait_for "committed snapshot mounted" 90 session_mounted "$MP3" || true
[ "$(docker exec pbs-plus-test cat "$MP3/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "committed hello.txt readable" || fail "committed hello.txt wrong or missing"
[ "$(docker exec pbs-plus-test cat "$MP3/nested/file.txt" 2>/dev/null)" = "nested-e2e" ] \
	&& ok "committed nested file readable" || fail "committed nested file wrong or missing"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$MP3")
submit_ok "$RESP" && ok "remount unmount accepted" || fail "remount unmount rejected: $(body_of "$RESP")"
wait_for "remount session unmounted" 60 session_gone "$MP3" || true

section "PHASE 4: Mount profiles"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles" \
	-d "datastore=$DATASTORE" -d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-host" \
	-d "mode=ro" -d "auto-mount=0" -d "schedule=not a schedule")
CODE=$(code_of "$RESP")
[ "$CODE" = "400" ] && ok "invalid schedule rejected" || fail "invalid schedule accepted (HTTP $CODE)"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles" \
	-d "datastore=$DATASTORE" -d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-host" \
	-d "mode=ro" -d "auto-mount=0" -d "schedule=02:00")
submit_ok "$RESP" && ok "profile created" || fail "profile create rejected: $(body_of "$RESP")"

PROFILE_ID=$(docker exec pbs-plus-test curl -k -s "$PBS_API/api2/extjs/config/d2d-mount-profiles" \
	| jq -r --arg id "$DATASTORE" '.data[]? | select(.datastore==$id) | .id' | head -1)
[ -n "$PROFILE_ID" ] && ok "profile listed: $PROFILE_ID" || die "profile not listed"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles/$PROFILE_ID" \
	-X PUT -d "datastore=$DATASTORE" -d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-host" \
	-d "mode=ro" -d "auto-mount=0" -d "schedule=*:00/30")
submit_ok "$RESP" && ok "profile updated" || fail "profile update rejected: $(body_of "$RESP")"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles/$PROFILE_ID/mount")
submit_ok "$RESP" && ok "mount-now accepted" || fail "mount-now rejected: $(body_of "$RESP")"

LATEST=$(latest_snapshot "$HOST_DIR")
PROFILE_MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-test-host/$LATEST"
wait_for "profile auto-mounted latest snapshot" 120 session_mounted "$PROFILE_MP" || true

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$PROFILE_MP" -d "force=1")
submit_ok "$RESP" && ok "profile mount unmounted" || fail "profile mount unmount rejected: $(body_of "$RESP")"
wait_for "profile session unmounted" 60 session_gone "$PROFILE_MP" || true

RESP=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-mount-profiles/$PROFILE_ID")
CODE=$(code_of "$RESP")
[ "$CODE" = "200" ] && ok "profile deleted" || fail "profile delete rejected (HTTP $CODE)"
LEFT=$(docker exec pbs-plus-test curl -k -s "$PBS_API/api2/extjs/config/d2d-mount-profiles" \
	| jq -r --arg id "$PROFILE_ID" '.data[]? | select(.id==$id) | .id' | head -1)
[ -z "$LEFT" ] && ok "profile gone from list" || fail "profile still listed"

section "RESULTS"

TOTAL=$((PASS + FAIL + SKIP))
echo ""
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "  Skipped: $SKIP"
echo "  Total:  $TOTAL"
echo ""
if [ "$FAIL" -gt 0 ]; then
	echo "   SOME TESTS FAILED"
	exit 1
else
	echo "   ALL TESTS PASSED"
	exit 0
fi
