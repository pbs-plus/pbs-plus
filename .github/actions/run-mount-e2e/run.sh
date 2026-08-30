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
	echo "--- d2d-mounts raw response ---"
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" || true
	echo ""
	echo "--- session files ---"
	ls -la /var/lib/pbs-plus/mount-sessions 2>/dev/null || true
	echo "--- mount process logs ---"
	for f in /var/run/pbs-plus-mounts/*.log; do
		[ -e "$f" ] || continue
		echo "== $f =="
		tail -40 "$f" 2>/dev/null || true
	done
	echo "--- workflow task logs ---"
	find /var/log/proxmox-backup/tasks -type f 2>/dev/null | grep -E ':(init|commit|compose|mount|unmount):' | while read -r f; do
		echo "== $f =="
		tail -60 "$f" 2>/dev/null || true
	done
	echo "--- recent task files ---"
	find /var/log/proxmox-backup/tasks -type f -mmin -15 2>/dev/null | head -20
	echo "--- end logs ---"
}

PBS_API="https://localhost:8017"
DATASTORE="test"
NAMESPACE="test"
HOST_DIR="/mnt/test/ns/test/host/test-backup-job"
INIT_GROUP_DIR="/mnt/test/ns/test/host/e2e-init"
ENC_DS=$(printf %s "$DATASTORE" | base64 -w0)
MOUNT_BASE="/mnt/pbs-plus-restores"

req() { curl -k -s "$@" -w "\nHTTP_CODE:%{http_code}"; }

code_of() { tail -1 <<<"$1" | sed 's/^HTTP_CODE://'; }
body_of() { sed '$d' <<<"$1"; }

api_post() {
	local path=$1; shift
	req -X POST "$PBS_API$path" -H "Content-Type: application/x-www-form-urlencoded" "$@"
}

sessions_field() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -r "$@" 2>/dev/null
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
session_offline() { [ "$(sessions_field --arg mp "$1" '.data[]? | select(.["mount-point"]==$mp) | .mounted' | head -1)" = "false" ]; }
session_gone()    { [ -z "$(sessions_field --arg mp "$1" '.data[]? | select(.["mount-point"]==$mp) | .["mount-point"]' | head -1)" ]; }
overlay_journal_dirs() { find /mnt/test/.pbs-plus/mount-overlays -maxdepth 2 -name .pxar-journal -type d 2>/dev/null | wc -l; }

latest_snapshot() {
	ls -1 "$1" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1
}

didx_in() {
	ls -1 "$1" 2>/dev/null | grep -E '\.mpxar\.didx$' | head -1 || ls -1 "$1" 2>/dev/null | grep -E '\.pxar\.didx$' | head -1
}

group_newer_than() {
	local new
	new=$(latest_snapshot "$1")
	[ -n "$new" ] && [ "$new" != "$2" ]
}

commit_errored() {
	find /var/log/proxmox-backup/tasks -type f 2>/dev/null | grep -E ':commit:' | while read -r f; do
		grep -q "TASK ERROR" "$f" 2>/dev/null && { echo "$f"; return 0; }
	done
	return 1
}

compose_errored() {
	find /var/log/proxmox-backup/tasks -type f 2>/dev/null | grep -E ':compose:' | while read -r f; do
		grep -q "TASK ERROR" "$f" 2>/dev/null && { echo "$f"; return 0; }
	done
	return 1
}

submit_ok() {
	local code
	code=$(code_of "$1")
	[ "$code" = "200" ] && grep -q '"success": *true\|"success":true' <<<"$(body_of "$1")"
}

section "PHASE 1: Mount existing snapshot read-only via API"

SNAP=$(latest_snapshot "$HOST_DIR")
[ -n "$SNAP" ] || die "no snapshot found under $HOST_DIR"
DIDX=$(didx_in "$HOST_DIR/$SNAP")
[ -n "$DIDX" ] || die "no pxar didx found under $HOST_DIR/$SNAP"
echo "Using snapshot: $SNAP (archive: $DIDX)"
MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-test-backup-job/$SNAP"

RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
	-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "mode=ro")
if submit_ok "$RESP"; then
	ok "mount request accepted"
	wait_for "ro session mounted at $MP" 240 session_mounted "$MP" || true
else
	fail "mount request rejected: $(body_of "$RESP")"
fi

mountpoint -q "$MP" && ok "mountpoint active" || fail "mountpoint not active"
FILES=$(ls "$MP" 2>/dev/null | head -3)
[ -n "$FILES" ] && ok "archive content listed" || fail "archive empty"

MODE=$(sessions_field --arg mp "$MP" '.data[]? | select(.["mount-point"]==$mp) | .mode' | head -1)
[ "$MODE" = "ro" ] && ok "session mode is ro" || fail "session mode = ${MODE:-missing}"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$MP")
if submit_ok "$RESP"; then
	ok "unmount request accepted"
	wait_for "ro session unmounted" 120 session_gone "$MP" || true
else
	fail "unmount rejected: $(body_of "$RESP")"
fi
[ ! -e "$MP" ] && ok "mountpoint cleaned up" || fail "mountpoint still exists"

section "PHASE 2: Init new archive, write, commit via API"

INIT_MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-e2e-init/init"

RESP=$(api_post "/api2/extjs/config/d2d-init/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init")
if submit_ok "$RESP"; then
	ok "init request accepted"
	wait_for "init session mounted at $INIT_MP" 240 session_mounted "$INIT_MP" || true
else
	fail "init request rejected: $(body_of "$RESP")"
fi

CAP=$(sessions_field --arg mp "$INIT_MP" '.data[]? | select(.["mount-point"]==$mp) | .["commit-capable"]' | head -1)
[ "$CAP" = "true" ] && ok "init session commit-capable" || fail "init session not commit-capable (${CAP:-missing})"

echo hello-e2e > "$INIT_MP/hello.txt" || fail "cannot write hello.txt through mount"
mkdir -p "$INIT_MP/nested" && echo nested-e2e > "$INIT_MP/nested/file.txt"
[ "$(cat "$INIT_MP/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "wrote and read hello.txt through mount" || fail "write/read through init mount failed"

BEFORE=$(latest_snapshot "$INIT_GROUP_DIR")

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$INIT_MP")
if submit_ok "$RESP"; then
	ok "keep-unmount request accepted"
	wait_for "init session offline but preserved" 120 session_offline "$INIT_MP" || fail "rw unmount did not preserve session"
	[ "$(overlay_journal_dirs)" -ge 1 ] \
		&& ok "uncommitted changes preserved in overlay journal" || fail "no overlay journal after keep-unmount"
else
	fail "keep-unmount rejected: $(body_of "$RESP")"
fi

RESP=$(api_post "/api2/extjs/config/d2d-init/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init")
if submit_ok "$RESP"; then
	wait_for "init session remounted at $INIT_MP" 240 session_mounted "$INIT_MP" || true
	[ "$(cat "$INIT_MP/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
		&& ok "uncommitted changes restored after remount" || fail "changes lost across keep-unmount/remount"
else
	fail "re-init rejected: $(body_of "$RESP")"
fi
RESP=$(api_post "/api2/extjs/config/d2d-commit/$ENC_DS" -d "mount-path=$INIT_MP")
if submit_ok "$RESP"; then
	ok "commit request accepted"
	COMMIT_DEADLINE=$((SECONDS + 420))
	COMMIT_OK=0
	while [ $SECONDS -lt $COMMIT_DEADLINE ]; do
		if group_newer_than "$INIT_GROUP_DIR" "$BEFORE" 2>/dev/null; then COMMIT_OK=1; break; fi
		if ERRF=$(commit_errored); then
			echo "commit task failed (see log below)"
			tail -20 "$ERRF"
			break
		fi
		sleep 2
	done
	[ $COMMIT_OK = 1 ] && ok "commit produced new snapshot dir" || { fail "commit did not produce snapshot (after $((SECONDS))s)"; dump_logs; }
else
	fail "commit rejected: $(body_of "$RESP")"
fi

NEW_SNAP=$(latest_snapshot "$INIT_GROUP_DIR")
[ -n "$NEW_SNAP" ] && [ "$NEW_SNAP" != "$BEFORE" ] && ok "new snapshot: $NEW_SNAP" || die "no new snapshot after commit"
ls "$INIT_GROUP_DIR/$NEW_SNAP"/*.didx >/dev/null 2>&1 \
	&& ok "didx present in new snapshot" || fail "no didx in new snapshot"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$INIT_MP" -d "force=1")
if submit_ok "$RESP"; then
	ok "init unmount request accepted"
	wait_for "init session unmounted" 120 session_gone "$INIT_MP" || true
else
	fail "init unmount rejected: $(body_of "$RESP")"
fi

section "PHASE 2b: Discard uncommitted rw changes via force unmount"

RESP=$(api_post "/api2/extjs/config/d2d-init/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init")
if submit_ok "$RESP"; then
	wait_for "discard-init session mounted at $INIT_MP" 240 session_mounted "$INIT_MP" || true
	[ "$(overlay_journal_dirs)" -ge 1 ] && ok "overlay journal live" || true
else
	fail "discard-init rejected: $(body_of "$RESP")"
fi
echo discard-me > "$INIT_MP/discard-me.txt" || fail "cannot write discard-me.txt"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$INIT_MP")
if submit_ok "$RESP"; then
	wait_for "discard-init session offline but preserved" 120 session_offline "$INIT_MP" || fail "keep-unmount did not preserve session"
else
	fail "discard keep-unmount rejected: $(body_of "$RESP")"
fi
[ "$(overlay_journal_dirs)" -ge 1 ] && ok "changes awaiting discard" || fail "overlay journal missing before discard"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$INIT_MP" -d "force=1")
if submit_ok "$RESP"; then
	wait_for "discard-init session gone" 120 session_gone "$INIT_MP" || fail "force unmount did not remove session"
	[ "$(overlay_journal_dirs)" = 0 ] \
		&& ok "uncommitted changes discarded" || fail "overlay journal survived force unmount"
else
	fail "discard unmount rejected: $(body_of "$RESP")"
fi

section "PHASE 3: Remount committed snapshot, verify data"

MP3="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-e2e-init/$NEW_SNAP"
DIDX3=$(didx_in "$INIT_GROUP_DIR/$NEW_SNAP")
[ -n "$DIDX3" ] || die "no didx in committed snapshot $NEW_SNAP"
RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "file-name=$DIDX3" -d "mode=ro")
if submit_ok "$RESP"; then
	ok "remount request accepted"
	wait_for "committed snapshot mounted" 240 session_mounted "$MP3" || true
else
	fail "remount rejected: $(body_of "$RESP")"
fi

[ "$(cat "$MP3/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "committed hello.txt readable" || fail "committed hello.txt wrong or missing"
[ "$(cat "$MP3/nested/file.txt" 2>/dev/null)" = "nested-e2e" ] \
	&& ok "committed nested file readable" || fail "committed nested file wrong or missing"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$MP3")
if submit_ok "$RESP"; then
	ok "remount unmount accepted"
	wait_for "remount session unmounted" 120 session_gone "$MP3" || true
else
	fail "remount unmount rejected: $(body_of "$RESP")"
fi

section "PHASE 4: Mount profiles"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles" \
	-d "datastore=$DATASTORE" -d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
	-d "mode=ro" -d "auto-mount=0" -d "schedule=not a schedule")
CODE=$(code_of "$RESP")
[ "$CODE" = "400" ] && ok "invalid schedule rejected" || fail "invalid schedule accepted (HTTP $CODE)"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles" \
	-d "datastore=$DATASTORE" -d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
	-d "mode=ro" -d "auto-mount=0" -d "schedule=02:00")
submit_ok "$RESP" && ok "profile created" || fail "profile create rejected: $(body_of "$RESP")"

PROFILE_ID=$(curl -k -s "$PBS_API/api2/extjs/config/d2d-mount-profiles" \
	| jq -r --arg id "$DATASTORE" '.data[]? | select(.datastore==$id) | .id' | head -1)
[ -n "$PROFILE_ID" ] && ok "profile listed: $PROFILE_ID" || die "profile not listed"

RESP=$(req -X PUT "$PBS_API/api2/extjs/config/d2d-mount-profiles/$PROFILE_ID" \
	-H "Content-Type: application/x-www-form-urlencoded" \
	-d "datastore=$DATASTORE" -d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
	-d "mode=ro" -d "auto-mount=0" -d "schedule=*:00/30")
submit_ok "$RESP" && ok "profile updated" || fail "profile update rejected: $(body_of "$RESP")"

RESP=$(api_post "/api2/extjs/config/d2d-mount-profiles/$PROFILE_ID/mount")
submit_ok "$RESP" && ok "mount-now accepted" || fail "mount-now rejected: $(body_of "$RESP")"

LATEST=$(latest_snapshot "$HOST_DIR")
PROFILE_MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-test-backup-job/$LATEST"
wait_for "profile auto-mounted latest snapshot" 240 session_mounted "$PROFILE_MP" || true

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$PROFILE_MP" -d "force=1")
submit_ok "$RESP" && ok "profile mount unmounted" || fail "profile mount unmount rejected: $(body_of "$RESP")"
wait_for "profile session unmounted" 120 session_gone "$PROFILE_MP" || true

RESP=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-mount-profiles/$PROFILE_ID")
CODE=$(code_of "$RESP")
[ "$CODE" = "200" ] && ok "profile deleted" || fail "profile delete rejected (HTTP $CODE)"
LEFT=$(curl -k -s "$PBS_API/api2/extjs/config/d2d-mount-profiles" \
	| jq -r --arg id "$PROFILE_ID" '.data[]? | select(.id==$id) | .id' | head -1)
[ -z "$LEFT" ] && ok "profile gone from list" || fail "profile still listed"

section "PHASE 5: Compose new snapshot from selection"

COMPOSE_GROUP_DIR="/mnt/test/ns/test/host/e2e-compose"

RESP=$(api_post "/api2/extjs/config/d2d-compose/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "file-name=$DIDX3" \
	-d "target-ns=$NAMESPACE" -d "target-type=host" -d "target-id=e2e-compose")
CODE=$(code_of "$RESP")
[ "$CODE" = "400" ] && ok "compose without paths rejected" || fail "compose without paths accepted (HTTP $CODE)"

SEL=$(printf /hello.txt | base64 -w0)
RESP=$(api_post "/api2/extjs/config/d2d-compose/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "file-name=$DIDX3" \
	-d "target-ns=$NAMESPACE" -d "target-type=host" -d "target-id=e2e-compose" \
	-d "paths=$SEL")
if submit_ok "$RESP"; then
	ok "compose request accepted"
	COMPOSE_DEADLINE=$((SECONDS + 420))
	COMPOSE_OK=0
	while [ $SECONDS -lt $COMPOSE_DEADLINE ]; do
		if [ -n "$(latest_snapshot "$COMPOSE_GROUP_DIR")" ]; then COMPOSE_OK=1; break; fi
		if ERRF=$(compose_errored); then
			echo "compose task failed (see log below)"
			tail -20 "$ERRF"
			break
		fi
		sleep 2
	done
	[ $COMPOSE_OK = 1 ] && ok "compose produced target snapshot" || { fail "compose produced no snapshot (after $((SECONDS))s)"; dump_logs; }
else
	fail "compose rejected: $(body_of "$RESP")"
fi

COMPOSE_SNAP=$(latest_snapshot "$COMPOSE_GROUP_DIR")
[ -n "$COMPOSE_SNAP" ] && ok "composed snapshot: $COMPOSE_SNAP" || die "no composed snapshot"
COMPOSE_DIDX=$(didx_in "$COMPOSE_GROUP_DIR/$COMPOSE_SNAP")
[ -n "$COMPOSE_DIDX" ] && ok "composed didx: $COMPOSE_DIDX" || die "no didx in composed snapshot"

COMPOSE_MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-e2e-compose/$COMPOSE_SNAP"
RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-compose" \
	-d "backup-time=$COMPOSE_SNAP" -d "file-name=$COMPOSE_DIDX" -d "mode=ro")
if submit_ok "$RESP"; then
	ok "composed mount request accepted"
	wait_for "composed snapshot mounted" 240 session_mounted "$COMPOSE_MP" || true
else
	fail "composed mount rejected: $(body_of "$RESP")"
fi

[ "$(cat "$COMPOSE_MP/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "selected hello.txt present in composed snapshot" || fail "composed hello.txt wrong or missing"
[ ! -e "$COMPOSE_MP/nested" ] \
	&& ok "unselected nested/ excluded from composed snapshot" || fail "unselected nested/ leaked into composed snapshot"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$COMPOSE_MP")
if submit_ok "$RESP"; then
	ok "composed unmount accepted"
	wait_for "composed session unmounted" 120 session_gone "$COMPOSE_MP" || true
else
	fail "composed unmount rejected: $(body_of "$RESP")"
fi

section "PHASE 5B: Clone a complete snapshot without rebuilding indexes"

CLONE_GROUP_DIR="/mnt/test/ns/test/host/e2e-clone"
ROOT_SEL=$(printf / | base64 -w0)
RESP=$(api_post "/api2/extjs/config/d2d-compose/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "file-name=$DIDX3" \
	-d "target-ns=$NAMESPACE" -d "target-type=host" -d "target-id=e2e-clone" \
	-d "paths=$ROOT_SEL")
if submit_ok "$RESP"; then
	ok "whole-root clone request accepted"
	CLONE_DEADLINE=$((SECONDS + 420))
	CLONE_OK=0
	while [ $SECONDS -lt $CLONE_DEADLINE ]; do
		if [ -n "$(latest_snapshot "$CLONE_GROUP_DIR")" ]; then CLONE_OK=1; break; fi
		if ERRF=$(compose_errored); then
			echo "whole-root clone task failed (see log below)"
			tail -20 "$ERRF"
			break
		fi
		sleep 2
	done
	[ $CLONE_OK = 1 ] && ok "whole-root clone produced target snapshot" || { fail "whole-root clone produced no snapshot"; dump_logs; }
else
	fail "whole-root clone rejected: $(body_of "$RESP")"
fi

CLONE_SNAP=$(latest_snapshot "$CLONE_GROUP_DIR")
SOURCE_META="$INIT_GROUP_DIR/$NEW_SNAP/$DIDX3"
SOURCE_PAYLOAD="${SOURCE_META%.mpxar.didx}.ppxar.didx"
TARGET_META="$CLONE_GROUP_DIR/$CLONE_SNAP/e2e-clone.mpxar.didx"
TARGET_PAYLOAD="$CLONE_GROUP_DIR/$CLONE_SNAP/e2e-clone.ppxar.didx"
cmp -s "$SOURCE_META" "$TARGET_META" \
	&& ok "whole-root metadata index cloned byte-for-byte" || fail "whole-root metadata index was rebuilt"
cmp -s "$SOURCE_PAYLOAD" "$TARGET_PAYLOAD" \
	&& ok "whole-root payload index cloned byte-for-byte" || fail "whole-root payload index was rebuilt"

section "PHASE 6: Flattened compose of a directory"

FLAT_GROUP_DIR="/mnt/test/ns/test/host/e2e-flatten"

FLAT_SEL=$(printf /nested | base64 -w0)
RESP=$(api_post "/api2/extjs/config/d2d-compose/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "file-name=$DIDX3" \
	-d "target-ns=$NAMESPACE" -d "target-type=host" -d "target-id=e2e-flatten" \
	-d "paths=$FLAT_SEL" -d "strip-root=1")
if submit_ok "$RESP"; then
	ok "flatten compose request accepted"
	FLAT_DEADLINE=$((SECONDS + 420))
	FLAT_OK=0
	while [ $SECONDS -lt $FLAT_DEADLINE ]; do
		if [ -n "$(latest_snapshot "$FLAT_GROUP_DIR")" ]; then FLAT_OK=1; break; fi
		if ERRF=$(compose_errored); then
			echo "flatten compose task failed (see log below)"
			tail -20 "$ERRF"
			break
		fi
		sleep 2
	done
	[ $FLAT_OK = 1 ] && ok "flatten compose produced target snapshot" || { fail "flatten compose produced no snapshot (after $((SECONDS))s)"; dump_logs; }
else
	fail "flatten compose rejected: $(body_of "$RESP")"
fi

FLAT_SNAP=$(latest_snapshot "$FLAT_GROUP_DIR")
FLAT_DIDX=$(didx_in "$FLAT_GROUP_DIR/$FLAT_SNAP")
[ -n "$FLAT_DIDX" ] && ok "flattened didx: $FLAT_DIDX" || die "no didx in flattened snapshot"

FLAT_MP="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-e2e-flatten/$FLAT_SNAP"
RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-flatten" \
	-d "backup-time=$FLAT_SNAP" -d "file-name=$FLAT_DIDX" -d "mode=ro")
if submit_ok "$RESP"; then
	ok "flattened mount request accepted"
	wait_for "flattened snapshot mounted" 240 session_mounted "$FLAT_MP" || true
else
	fail "flattened mount rejected: $(body_of "$RESP")"
fi

[ "$(cat "$FLAT_MP/file.txt" 2>/dev/null)" = "nested-e2e" ] \
	&& ok "directory contents at snapshot root" || fail "flattened file.txt wrong or missing"
[ ! -e "$FLAT_MP/nested" ] \
	&& ok "selected directory itself excluded" || fail "selected directory leaked into flattened snapshot"
[ ! -e "$FLAT_MP/hello.txt" ] \
	&& ok "unselected sibling excluded" || fail "unselected sibling leaked into flattened snapshot"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$FLAT_MP")
if submit_ok "$RESP"; then
	ok "flattened unmount accepted"
	wait_for "flattened session unmounted" 120 session_gone "$FLAT_MP" || true
else
	fail "flattened unmount rejected: $(body_of "$RESP")"
fi

section "PHASE 7: Stop a queued compose through PBS task control"

JOBS_DB="/etc/proxmox-backup/pbs-plus/jobs.db"
STOP_TARGET="e2e-stop"
STOP_GROUP_DIR="/mnt/test/ns/test/host/$STOP_TARGET"
STOP_KEY_RAW="$DATASTORE|$NAMESPACE|host|$STOP_TARGET|compose"
STOP_KEY="$DATASTORE-$(printf %s "$STOP_KEY_RAW" | sha256sum | cut -c1-16)"
STOP_LOCK="snapshot-compose:$STOP_KEY"
STOP_BLOCKER="e2e-stop-blocker-$$"
NOW=$(date +%s)

sqlite3 -cmd '.timeout 5000' "$JOBS_DB" "
INSERT INTO job_executions (
	id, kind, workflow_version, definition_id, trigger, dedupe_key, payload, state,
	attempt, max_attempts, retry_initial_seconds, retry_max_seconds, run_at,
	lease_owner, lease_until, cancel_requested, created_at, started_at
) VALUES (
	'$STOP_BLOCKER', 'e2e.blocker', '1', '$STOP_KEY', 'e2e', '$STOP_BLOCKER', '{}', 'running',
	1, 1, 1, 1, $NOW, 'e2e', $((NOW + 600)), 0, $NOW, $NOW
);
INSERT INTO job_resource_locks (resource_key, execution_id, lease_until)
VALUES ('$STOP_LOCK', '$STOP_BLOCKER', $((NOW + 600)));
" || die "cannot create compose resource blocker"

RESP=$(api_post "/api2/extjs/config/d2d-compose/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$NEW_SNAP" -d "file-name=$DIDX3" \
	-d "target-ns=$NAMESPACE" -d "target-type=host" -d "target-id=$STOP_TARGET" \
	-d "paths=$SEL")
STOP_UPID=$(body_of "$RESP" | jq -r '.data // empty')
if submit_ok "$RESP" && [ -n "$STOP_UPID" ]; then
	ok "queued compose request accepted"
	sleep 2
	STOP_STATE=$(sqlite3 -cmd '.timeout 5000' "$JOBS_DB" \
		"SELECT state || ':' || attempt FROM job_executions WHERE kind = 'snapshot.compose' AND definition_id = '$STOP_KEY' ORDER BY created_at DESC LIMIT 1;")
	[ "$STOP_STATE" = "pending:0" ] \
		&& ok "resource-blocked compose stayed queued without spending an attempt" \
		|| fail "queued compose state was ${STOP_STATE:-missing}, expected pending:0"

	if proxmox-backup-manager task stop "$STOP_UPID"; then
		ok "PBS task stop accepted"
	else
		fail "PBS task stop rejected"
	fi

	STOP_DEADLINE=$((SECONDS + 30))
	STOP_CANCELED=0
	while [ $SECONDS -lt $STOP_DEADLINE ]; do
		STOP_STATE=$(sqlite3 -cmd '.timeout 5000' "$JOBS_DB" \
			"SELECT state FROM job_executions WHERE kind = 'snapshot.compose' AND definition_id = '$STOP_KEY' ORDER BY created_at DESC LIMIT 1;")
		if [ "$STOP_STATE" = "canceled" ]; then STOP_CANCELED=1; break; fi
		sleep 1
	done
	[ $STOP_CANCELED = 1 ] && ok "queued compose execution canceled" || fail "queued compose state = ${STOP_STATE:-missing}"

	STOP_LOG=$(proxmox-backup-manager task log "$STOP_UPID" 2>&1 || true)
	grep -q "abort requested" <<<"$STOP_LOG" \
		&& ok "task log recorded abort request" || fail "task log missing abort request"
	grep -q "TASK ERROR: context canceled" <<<"$STOP_LOG" \
		&& ok "stopped queued task reached terminal state" || fail "stopped queued task remained active"
else
	fail "queued compose rejected: $(body_of "$RESP")"
fi

sqlite3 -cmd '.timeout 5000' "$JOBS_DB" "
DELETE FROM job_resource_locks WHERE execution_id = '$STOP_BLOCKER';
DELETE FROM job_executions WHERE id = '$STOP_BLOCKER';
"
sleep 2
[ -z "$(latest_snapshot "$STOP_GROUP_DIR")" ] \
	&& ok "canceled compose published no snapshot" || fail "canceled compose published a snapshot"

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
