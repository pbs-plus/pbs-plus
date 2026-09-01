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
	echo "--- mount process logs ---"
	for f in /var/run/pbs-plus-mounts/*.log; do
		[ -e "$f" ] || continue
		echo "== $f =="
		tail -40 "$f" 2>/dev/null || true
	done
	echo "--- recent backup and mount task logs ---"
	find /var/log/proxmox-backup/tasks -type f -mmin -30 2>/dev/null | head -20 | while read -r f; do
		echo "== $f =="
		tail -30 "$f" 2>/dev/null || true
	done
	echo "--- end logs ---"
}

PBS_API="https://localhost:8017"
DATASTORE="test"
NAMESPACE="test"
STORE_DIR="/mnt/test/ns/test/host"
MOUNT_BASE="/mnt/pbs-plus-restores"
ARCH="e2e-archives"
ENC_DS=$(printf %s "$DATASTORE" | base64 -w0)

req() { curl -k -s "$@" -w "\nHTTP_CODE:%{http_code}"; }

code_of() { tail -1 <<<"$1" | sed 's/^HTTP_CODE://'; }
body_of() { sed '$d' <<<"$1"; }

api_post() {
	local path=$1; shift
	req -X POST "$PBS_API$path" -H "Content-Type: application/x-www-form-urlencoded" "$@"
}

submit_ok() {
	local code
	code=$(code_of "$1")
	[ "$code" = "200" ] && grep -q '"success": *true\|"success":true' <<<"$(body_of "$1")"
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
	return 1
}

session_mounted() { [ "$(sessions_field --arg mp "$1" '.data[]? | select(.["mount-point"]==$mp) | .mounted' | head -1)" = "true" ]; }
session_gone()    { [ -z "$(sessions_field --arg mp "$1" '.data[]? | select(.["mount-point"]==$mp) | .["mount-point"]' | head -1)" ]; }

latest_snapshot() {
	ls -1 "$1" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1
}

didx_in() {
	ls -1 "$1" 2>/dev/null | grep -E '\.mpxar\.didx$' | head -1 || ls -1 "$1" 2>/dev/null | grep -E '\.pxar\.didx$' | head -1
}

create_backup_job() {
	local id=$1; shift
	api_post "/api2/extjs/config/disk-backup" \
		-d "id=$id" \
		-d "target=test-host - Root" \
		-d "subpath=/test-backup" \
		-d "store=$DATASTORE" \
		-d "ns=$NAMESPACE" \
		-d "schedule=" \
		-d "retry=" \
		-d "retry-interval=" \
		-d "max-dir-entries=" \
		-d "mode=" \
		-d "sourcemode=" \
		-d "readmode=" \
		-d "include-xattr=" \
		-d "comment=" \
		-d "rawexclusions=" \
		-d "pre_script=" \
		-d "post_script=" \
		"$@"
}

run_backup() {
	local b64
	b64=$(printf %s "$1" | base64 -w0)
	submit_ok "$(req -X POST "$PBS_API/api2/extjs/d2d/backup?job=$b64" -H "Content-Type: application/x-www-form-urlencoded")"
}

wait_snapshot() {
	local group=$1 before=$2 timeout=$3 snap didx
	local deadline=$((SECONDS + timeout))
	while [ $SECONDS -lt $deadline ]; do
		snap=$(latest_snapshot "$group")
		if [ -n "$snap" ] && [ "$snap" != "$before" ]; then
			didx=$(didx_in "$group/$snap")
			if [ -n "$didx" ]; then
				echo "$snap"
				return 0
			fi
		fi
		sleep 3
	done
	return 1
}

MOUNTED_MP=""

mount_snapshot() {
	local bid=$1 snap=$2 group mp didx resp
	group="$STORE_DIR/$bid"
	mp="$MOUNT_BASE/$DATASTORE/$NAMESPACE/host-$bid/$snap"
	didx=$(didx_in "$group/$snap")
	[ -n "$didx" ] || { fail "no didx in $group/$snap"; return 1; }
	resp=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
		-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=$bid" \
		-d "backup-time=$snap" -d "file-name=$didx" -d "mode=ro")
	if submit_ok "$resp"; then
		if wait_for "snapshot $bid/$snap mounted at $mp" 240 session_mounted "$mp"; then
			MOUNTED_MP="$mp"
			return 0
		fi
		return 1
	fi
	fail "mount rejected for $bid/$snap: $(body_of "$resp")"
	return 1
}

unmount_snapshot() {
	local mp=$1 resp
	resp=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" -d "mount-path=$mp")
	if submit_ok "$resp"; then
		wait_for "session unmounted at $mp" 120 session_gone "$mp" || true
	else
		fail "unmount rejected for $mp: $(body_of "$resp")"
	fi
	MOUNTED_MP=""
}

content_is() {
	local mp=$1 rel=$2 want=$3 got
	got=$(cat "$mp/$rel" 2>/dev/null)
	[ "$got" = "$want" ]
}

file_exists()  { [ -f "$1" ]; }
file_missing() { [ ! -e "$1" ]; }
dir_exists()   { [ -d "$1" ]; }

run_and_mount() {
	local bid=$1 group before snap
	group="$STORE_DIR/$bid"
	before=$(latest_snapshot "$group")
	if run_backup "$bid"; then
		ok "backup $bid accepted"
	else
		fail "backup $bid rejected"
		dump_logs
		return 1
	fi
	snap=$(wait_snapshot "$group" "$before" 300)
	if [ -n "$snap" ]; then
		ok "backup $bid produced snapshot $snap"
	else
		fail "backup $bid produced no snapshot"
		dump_logs
		return 1
	fi
	mount_snapshot "$bid" "$snap"
}

section "PHASE 1: expansion disabled keeps archives as plain files"

BEFORE=$(latest_snapshot "$STORE_DIR/test-backup-job")
if run_backup "test-backup-job"; then
	ok "control backup accepted"
else
	die "control backup rejected"
fi
SNAP=$(wait_snapshot "$STORE_DIR/test-backup-job" "$BEFORE" 300)
[ -n "$SNAP" ] || die "control backup produced no snapshot"
ok "control snapshot: $SNAP"

if mount_snapshot "test-backup-job" "$SNAP"; then
	MP="$MOUNTED_MP"
	file_exists "$MP/$ARCH/docs.zip" && ok "docs.zip kept as plain file" || fail "docs.zip missing without expansion"
	file_exists "$MP/$ARCH/outer.zip" && ok "outer.zip kept as plain file" || fail "outer.zip missing without expansion"
	file_exists "$MP/$ARCH/data.7z" && ok "data.7z kept as plain file" || fail "data.7z missing without expansion"
	file_missing "$MP/$ARCH/readme.txt" && ok "zip contents not exposed without expansion" || fail "readme.txt leaked without expansion"
	file_missing "$MP/$ARCH/deep.txt" && ok "nested contents not exposed without expansion" || fail "deep.txt leaked without expansion"
	file_missing "$MP/$ARCH/seven.txt" && ok "7z contents not exposed without expansion" || fail "seven.txt leaked without expansion"
	file_missing "$MP/$ARCH/sub/note.txt" && ok "zip subdir contents not exposed without expansion" || fail "zip subdir leaked without expansion"
	unmount_snapshot "$MP"
else
	die "control snapshot mount failed"
fi

section "PHASE 2: expansion merges zip, nested zip, and 7z contents"

RESP=$(create_backup_job "test-expand-job" -d "expand-archives=1")
submit_ok "$RESP" && ok "expand job created" || die "expand job rejected: $(body_of "$RESP")"

if run_and_mount "test-expand-job"; then
	MP="$MOUNTED_MP"
	file_missing "$MP/$ARCH/docs.zip" && ok "docs.zip hidden when expanded" || fail "docs.zip still visible when expanded"
	file_missing "$MP/$ARCH/outer.zip" && ok "outer.zip hidden when expanded" || fail "outer.zip still visible when expanded"
	file_missing "$MP/$ARCH/mid.zip" && ok "nested mid.zip hidden when expanded" || fail "nested mid.zip still visible when expanded"
	file_missing "$MP/$ARCH/data.7z" && ok "data.7z hidden when expanded" || fail "data.7z still visible when expanded"
	content_is "$MP" "$ARCH/readme.txt" "zip readme content" && ok "zip readme.txt expanded" || fail "zip readme.txt wrong or missing"
	content_is "$MP" "$ARCH/sub/note.txt" "sub note" && ok "zip sub/note.txt expanded" || fail "zip sub/note.txt wrong or missing"
	dir_exists "$MP/$ARCH/empty" && ok "childless zip dir preserved" || fail "childless zip dir missing"
	content_is "$MP" "$ARCH/collision.txt" "real wins" && ok "real file shadows virtual collision" || fail "virtual collision beat real file"
	content_is "$MP" "$ARCH/deep.txt" "deep content" && ok "nested zip deep.txt expanded" || fail "nested deep.txt wrong or missing"
	content_is "$MP" "$ARCH/seven.txt" "seven content" && ok "7z seven.txt expanded" || fail "seven.txt wrong or missing"
	unmount_snapshot "$MP"
else
	die "expand backup or mount failed"
fi

section "PHASE 3: expand-max-depth=0 stops nested expansion"

RESP=$(create_backup_job "test-expand-depth-job" -d "expand-archives=1" -d "expand-max-depth=0")
submit_ok "$RESP" && ok "depth-limited job created" || die "depth job rejected: $(body_of "$RESP")"

if run_and_mount "test-expand-depth-job"; then
	MP="$MOUNTED_MP"
	file_missing "$MP/$ARCH/outer.zip" && ok "outer.zip expanded at root depth" || fail "outer.zip visible at depth 0"
	file_exists "$MP/$ARCH/mid.zip" && ok "mid.zip kept as file at depth 0" || fail "mid.zip missing at depth 0"
	file_missing "$MP/$ARCH/deep.txt" && ok "deep.txt not exposed at depth 0" || fail "deep.txt leaked at depth 0"
	content_is "$MP" "$ARCH/readme.txt" "zip readme content" && ok "root zip still expanded at depth 0" || fail "root zip not expanded at depth 0"
	unmount_snapshot "$MP"
else
	die "depth backup or mount failed"
fi

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
