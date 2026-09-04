#!/bin/bash
set -uo pipefail

PASS=0
FAIL=0

ok() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }
section() { echo ""; echo "=========================================="; echo "  $1"; echo "=========================================="; }

PBS_API="https://localhost:8017"
DATASTORE="test"
NAMESPACE="test"
HOST_DIR="/mnt/test/ns/test/host/test-backup-job"
INIT_GROUP_DIR="/mnt/test/ns/test/host/e2e-init"
ENC_DS=$(printf %s "$DATASTORE" | base64 -w0)
OUTPOST="e2e-nfs"
NFS_PORT=32049
CLIENT_BASE="/tmp/pbs-plus-outpost-client"
CLIENT_A="$CLIENT_BASE/backup"
CLIENT_B="$CLIENT_BASE/init"

req() { curl -k -s "$@" -w "\nHTTP_CODE:%{http_code}"; }
code_of() { tail -1 <<<"$1" | sed 's/^HTTP_CODE://'; }
body_of() { sed '$d' <<<"$1"; }

dump_logs() {
	echo "--- outposts ---"
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" || true
	echo ""
	echo "--- mount sessions ---"
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" || true
	echo ""
	echo "--- NFS listener ---"
	ss -lntp 2>/dev/null | grep ":$NFS_PORT" || true
	echo "--- mount task logs ---"
	find /var/log/proxmox-backup/tasks -type f 2>/dev/null | grep -E ':(mount|unmount):' | while read -r file; do
		echo "== $file =="
		tail -40 "$file" 2>/dev/null || true
	done
}

api_post() {
	local path=$1
	shift
	req -X POST "$PBS_API$path" -H "Content-Type: application/x-www-form-urlencoded" "$@"
}

submit_ok() {
	local code
	code=$(code_of "$1")
	[ "$code" = "200" ] && grep -q '"success": *true\|"success":true' <<<"$(body_of "$1")"
}

latest_snapshot() {
	ls -1 "$1" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1
}

didx_in() {
	ls -1 "$1" 2>/dev/null | grep -E '\.mpxar\.didx$' | head -1 || ls -1 "$1" 2>/dev/null | grep -E '\.pxar\.didx$' | head -1
}

session_ready() {
	local backup_id=$1
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id "$backup_id" --arg outpost "$OUTPOST" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id and .mounted == true and (.endpoint | length > 0))' \
		>/dev/null
}

session_gone() {
	local backup_id=$1
	! curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id "$backup_id" --arg outpost "$OUTPOST" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id)' \
		>/dev/null
}

session_endpoint() {
	local backup_id=$1
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -r \
		--arg id "$backup_id" --arg outpost "$OUTPOST" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id) | .endpoint' | head -1
}

snapshot_session_absent() {
	local backup_id=$1
	local backup_time=$2
	! curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id "$backup_id" --arg backup_time "$backup_time" \
		'.data[]? | select(.["backup-id"] == $id and .["backup-time"] == $backup_time)' \
		>/dev/null
}

detach_existing_session() {
	local backup_id=$1
	local backup_time=$2
	local file_name=$3
	local response
	if snapshot_session_absent "$backup_id" "$backup_time"; then
		return 0
	fi
	response=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" \
		-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=$backup_id" \
		-d "backup-time=$backup_time" -d "file-name=$file_name" -d "force=1")
	submit_ok "$response" || {
		fail "existing $backup_id session detach rejected: $(body_of "$response")"
		return 1
	}
	wait_for "existing $backup_id session removed" 120 snapshot_session_absent "$backup_id" "$backup_time"
}

outpost_share_count() {
	local expected=$1
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" | jq -e \
		--arg name "$OUTPOST" --argjson expected "$expected" \
		'.data[]? | select(.name == $name and .running == true and (.attached | length) == $expected)' \
		>/dev/null
}

wait_for() {
	local desc=$1
	local timeout=$2
	shift 2
	local deadline=$((SECONDS + timeout))
	while [ "$SECONDS" -lt "$deadline" ]; do
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

mount_share() {
	local share=$1
	local target=$2
	mkdir -p "$target"
	timeout 60 mount -t nfs \
		-o "nfsvers=3,proto=tcp,mountproto=tcp,port=$NFS_PORT,mountport=$NFS_PORT,nolock,noacl,ro,soft,timeo=50,retrans=2" \
		"pbs-plus-test:/$share" "$target"
}

cleanup_client_mounts() {
	for mount_point in "$CLIENT_A" "$CLIENT_B"; do
		if mountpoint -q "$mount_point"; then
			timeout 30 umount -f -l "$mount_point" >/dev/null 2>&1 || true
		fi
	done
	rm -rf "$CLIENT_BASE"
}

trap cleanup_client_mounts EXIT

section "Locate snapshots"

SNAP_A=$(latest_snapshot "$HOST_DIR")
SNAP_B=$(latest_snapshot "$INIT_GROUP_DIR")
[ -n "$SNAP_A" ] || { fail "test-backup-job snapshot missing"; exit 1; }
[ -n "$SNAP_B" ] || { fail "e2e-init snapshot missing"; exit 1; }
DIDX_A=$(didx_in "$HOST_DIR/$SNAP_A")
DIDX_B=$(didx_in "$INIT_GROUP_DIR/$SNAP_B")
[ -n "$DIDX_A" ] || { fail "test-backup-job archive missing"; exit 1; }
[ -n "$DIDX_B" ] || { fail "e2e-init archive missing"; exit 1; }

detach_existing_session test-backup-job "$SNAP_A" "$DIDX_A" || true
detach_existing_session e2e-init "$SNAP_B" "$DIDX_B" || true

section "Create one NFS outpost"

RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=$OUTPOST" -d "type=nfs" -d "listen-addr=0.0.0.0:$NFS_PORT")
submit_ok "$RESP" && ok "outpost created" || fail "outpost create rejected: $(body_of "$RESP")"
wait_for "outpost listener running" 30 outpost_share_count 0 || true

section "Attach two snapshot shares"

RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
	-d "backup-time=$SNAP_A" -d "file-name=$DIDX_A" -d "mode=ro" -d "outpost=$OUTPOST")
submit_ok "$RESP" && ok "first share request accepted" || fail "first share rejected: $(body_of "$RESP")"

RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$SNAP_B" -d "file-name=$DIDX_B" -d "mode=ro" -d "outpost=$OUTPOST")
submit_ok "$RESP" && ok "second share request accepted" || fail "second share rejected: $(body_of "$RESP")"

wait_for "first share attached" 240 session_ready test-backup-job || true
wait_for "second share attached" 240 session_ready e2e-init || true
wait_for "one outpost reports two shares" 30 outpost_share_count 2 || true

ENDPOINT_A=$(session_endpoint test-backup-job)
ENDPOINT_B=$(session_endpoint e2e-init)
SHARE_A=${ENDPOINT_A##*/}
SHARE_B=${ENDPOINT_B##*/}
[ -n "$SHARE_A" ] && [ -n "$SHARE_B" ] && [ "$SHARE_A" != "$SHARE_B" ] \
	&& ok "shares have distinct export names" || fail "invalid endpoints: $ENDPOINT_A, $ENDPOINT_B"

section "Read both shares through NFSv3"

mount_share "$SHARE_A" "$CLIENT_A" && ok "first share mounted over NFSv3" || fail "first NFS mount failed"
mount_share "$SHARE_B" "$CLIENT_B" && ok "second share mounted over NFSv3" || fail "second NFS mount failed"

mountpoint -q "$CLIENT_A" && ok "first kernel mount active" || fail "first kernel mount inactive"
mountpoint -q "$CLIENT_B" && ok "second kernel mount active" || fail "second kernel mount inactive"
[ -n "$(ls -A "$CLIENT_A" 2>/dev/null)" ] && ok "first share content readable" || fail "first share empty"
[ "$(cat "$CLIENT_B/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "second share content readable" || fail "second share content mismatch"
[ ! -e "$CLIENT_A/hello.txt" ] && [ -e "$CLIENT_B/hello.txt" ] \
	&& ok "share contents remain isolated" || fail "share contents crossed exports"

cleanup_client_mounts

section "Detach shares and remove outpost"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
	-d "backup-time=$SNAP_A" -d "file-name=$DIDX_A" -d "force=1")
submit_ok "$RESP" && ok "first detach accepted" || fail "first detach rejected: $(body_of "$RESP")"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$SNAP_B" -d "file-name=$DIDX_B" -d "force=1")
submit_ok "$RESP" && ok "second detach accepted" || fail "second detach rejected: $(body_of "$RESP")"

wait_for "first outpost session removed" 120 session_gone test-backup-job || true
wait_for "second outpost session removed" 120 session_gone e2e-init || true
wait_for "outpost reports no shares" 30 outpost_share_count 0 || true

RESP=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$OUTPOST")
submit_ok "$RESP" && ok "outpost removed" || fail "outpost delete rejected: $(body_of "$RESP")"

section "RESULTS"

TOTAL=$((PASS + FAIL))
echo ""
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "  Total:  $TOTAL"
echo ""
if [ "$FAIL" -gt 0 ]; then
	echo "  SOME TESTS FAILED"
	dump_logs
	exit 1
fi
echo "  ALL TESTS PASSED"
