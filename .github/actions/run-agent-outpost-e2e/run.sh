#!/bin/bash
set -uo pipefail

PASS=0
FAIL=0

ok() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }
section() { echo ""; echo "=========================================="; echo "  $1"; echo "=========================================="; }

PHASE="${1:-main}"
PBS_API="https://localhost:8017"
DATASTORE="test"
NAMESPACE="test"
HOST_DIR="/mnt/test/ns/test/host/test-backup-job"
ENC_DS=$(printf %s "$DATASTORE" | base64 -w0)
AGENT="test-host"
AGENT_ADDR="pbs-plus-agent"
EXPORT="e2e-export"
EXPORT_PORT=39100
AGENT_NFS="e2e-agent-nfs"
AGENT_NFS_PORT=32050
AGENT_S3="e2e-agent-s3"
AGENT_S3_PORT=39101
ACCESS="e2e-export-access"
SECRET="e2e-export-secret"
WORK="/tmp/agent-outpost-e2e"
CLIENT="$WORK/nfs"
REFERENCE="$WORK/reference.txt"
PROBE_FILE="random1.txt"
MC_RELEASE="${MC_RELEASE:-RELEASE.2025-08-13T08-35-41Z}"

req() { curl -k -s "$@" -w "\nHTTP_CODE:%{http_code}"; }
code_of() { tail -1 <<<"$1" | sed 's/^HTTP_CODE://'; }
body_of() { sed '$d' <<<"$1"; }
mc() { command mc --insecure "$@"; }

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

dump_logs() {
	echo "--- outposts ---"
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" || true
	echo ""
	echo "--- mount sessions ---"
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" || true
	echo ""
	echo "--- local listeners ---"
	ss -lntp 2>/dev/null | grep -E ":($EXPORT_PORT|$AGENT_NFS_PORT|$AGENT_S3_PORT)\b" || true
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

outpost_field() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" | jq -r \
		--arg name "$1" '.data[]? | select(.name == $name) | '"$2"
}

outpost_healthy() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" | jq -e \
		--arg name "$1" '.data[]? | select(.name == $name and .running == true and ((.error // "") == ""))' >/dev/null
}

outpost_offline() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" | jq -e \
		--arg name "$1" '.data[]? | select(.name == $name and .running == true and ((.error // "") != ""))' >/dev/null
}

session_ready() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id test-backup-job --arg outpost "$1" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id and .mounted == true and (.endpoint | length > 0))' \
		>/dev/null
}

session_endpoint() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -r \
		--arg id test-backup-job --arg outpost "$1" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id) | .endpoint' | head -1
}

snapshot_session_absent() {
	! curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id test-backup-job --arg backup_time "$SNAP" \
		'.data[]? | select(.["backup-id"] == $id and .["backup-time"] == $backup_time)' \
		>/dev/null
}

attach() {
	local outpost=$1
	local resp
	resp=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
		-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
		-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "mode=ro" -d "outpost=$outpost")
	submit_ok "$resp" && ok "share attach to $outpost accepted" || fail "share attach to $outpost rejected: $(body_of "$resp")"
	wait_for "share attached to $outpost" 240 session_ready "$outpost"
}

detach() {
	local resp
	if snapshot_session_absent; then
		return 0
	fi
	resp=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" \
		-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=test-backup-job" \
		-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "force=1")
	submit_ok "$resp" || fail "share detach rejected: $(body_of "$resp")"
	wait_for "share detached" 120 snapshot_session_absent
}

create_outpost() {
	local resp
	resp=$(api_post "/api2/extjs/config/d2d-outposts" "$@")
	submit_ok "$resp" && return 0
	echo "$(body_of "$resp")"
	return 1
}

delete_outpost() {
	req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$1" >/dev/null 2>&1 || true
}

export_config() {
	jq -cn --arg a "$ACCESS" --arg s "$SECRET" '{region: "us-east-1", credentials: [{access_key: $a, secret_key: $s}]}'
}

local_listener() {
	ss -lnt 2>/dev/null | grep -qE ":$1\b"
}

tcp_open() {
	timeout 3 bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null
}

install_mc() {
	if type -P mc >/dev/null 2>&1; then
		return 0
	fi
	local url="https://github.com/minio/mc/releases/download/${MC_RELEASE}/mc.linux-amd64.${MC_RELEASE}"
	curl -fsSL -o /usr/local/bin/mc "$url" && chmod +x /usr/local/bin/mc
}

mount_agent_share() {
	local share=$1
	mkdir -p "$CLIENT"
	timeout 60 mount -t nfs \
		-o "nfsvers=3,proto=tcp,mountproto=tcp,port=$AGENT_NFS_PORT,mountport=$AGENT_NFS_PORT,nolock,noacl,ro,soft,timeo=50,retrans=2" \
		"$AGENT_ADDR:/$share" "$CLIENT"
}

unmount_client() {
	if mountpoint -q "$CLIENT" 2>/dev/null; then
		timeout 30 umount -f -l "$CLIENT" >/dev/null 2>&1 || true
	fi
}

read_through_agent_nfs() {
	local endpoint share
	endpoint=$(session_endpoint "$AGENT_NFS")
	share=${endpoint##*/}
	if [ -n "${E2E_NFS_READER:-}" ]; then
		"$E2E_NFS_READER" "$AGENT_ADDR:$AGENT_NFS_PORT" "/$share" cat "$PROBE_FILE" >"$WORK/agent-nfs.txt" \
			&& cmp -s "$WORK/agent-nfs.txt" "$REFERENCE" \
			&& ok "agent-hosted NFS returns the same bytes as the server export (userspace client)" \
			|| fail "agent-hosted NFS read failed or differs (userspace client)"
		[ "$("$E2E_NFS_READER" "$AGENT_ADDR:$AGENT_NFS_PORT" "/$share" ls / | grep -c '^random.*\.txt$')" = "50" ] \
			&& ok "agent-hosted NFS lists all 50 files (userspace client)" || fail "agent-hosted NFS listing incomplete (userspace client)"
		return 0
	fi
	mount_agent_share "$share" && ok "agent-hosted share mounted over NFSv3" || { fail "agent-hosted NFS mount failed"; return 1; }
	cmp -s "$CLIENT/$PROBE_FILE" "$REFERENCE" \
		&& ok "agent-hosted NFS returns the same bytes as the server export" \
		|| fail "agent-hosted NFS content differs from the server export"
	[ "$(find "$CLIENT" -mindepth 1 -maxdepth 1 -name 'random*.txt' | wc -l)" = "50" ] \
		&& ok "agent-hosted NFS lists all 50 files" || fail "agent-hosted NFS listing incomplete: $(ls -1 "$CLIENT" | wc -l)"
	unmount_client
}

locate_snapshot() {
	SNAP=$(find "$HOST_DIR" -mindepth 1 -maxdepth 1 -type d -name '????-??-??T*' -printf '%f\n' | sort | tail -1)
	[ -n "$SNAP" ] || { fail "test-backup-job snapshot missing"; exit 1; }
	DIDX=$(find "$HOST_DIR/$SNAP" -maxdepth 1 -name '*.mpxar.didx' -printf '%f\n' | head -1)
	[ -n "$DIDX" ] || DIDX=$(find "$HOST_DIR/$SNAP" -maxdepth 1 -name '*.pxar.didx' -printf '%f\n' | head -1)
	[ -n "$DIDX" ] || { fail "test-backup-job archive missing"; exit 1; }
}

finish() {
	section "RESULTS ($PHASE)"
	echo ""
	echo "  Passed: $PASS"
	echo "  Failed: $FAIL"
	echo ""
	if [ "$FAIL" -gt 0 ]; then
		echo "  SOME TESTS FAILED"
		dump_logs
		exit 1
	fi
	echo "  ALL TESTS PASSED"
}

trap unmount_client EXIT
locate_snapshot

case "$PHASE" in
main)
	rm -rf "$WORK"
	mkdir -p "$WORK"
	detach
	for name in "$EXPORT" "$AGENT_NFS" "$AGENT_S3"; do delete_outpost "$name"; done
	install_mc && ok "mc installed" || { fail "mc install failed"; finish; }

	section "S3 snapshot export served by the server"

	out=$(create_outpost -d "name=$EXPORT" -d "type=s3-export" -d "listen-addr=0.0.0.0:$EXPORT_PORT" \
		--data-urlencode "s3-export=$(export_config)") \
		&& ok "s3-export outpost created" || fail "s3-export create rejected: $out"
	wait_for "s3-export outpost running" 30 outpost_healthy "$EXPORT"
	[ "$(outpost_field "$EXPORT" '.["s3-export"].credentials[0].secret_key // ""')" != "$SECRET" ] \
		&& ok "secret key redacted in outpost listing" || fail "secret key exposed in outpost listing"
	attach "$EXPORT"
	BUCKET=$(session_endpoint "$EXPORT")
	BUCKET=${BUCKET##*/}
	[ -n "$BUCKET" ] && ok "export bucket is $BUCKET" || fail "export endpoint missing"

	mc alias set exp "https://127.0.0.1:$EXPORT_PORT" "$ACCESS" "$SECRET" --api S3v4 --path on >/dev/null
	mc ls exp | grep -q "$BUCKET/" && ok "bucket listed" || fail "bucket not listed: $(mc ls exp 2>&1)"
	[ "$(mc ls exp/"$BUCKET"/ 2>/dev/null | grep -c 'random.*\.txt')" = "50" ] \
		&& ok "bucket lists all 50 objects" || fail "bucket listing incomplete: $(mc ls exp/"$BUCKET"/ 2>&1 | head)"
	mc cat "exp/$BUCKET/$PROBE_FILE" >"$REFERENCE" 2>/dev/null && [ -s "$REFERENCE" ] \
		&& ok "object downloaded" || fail "object download failed"
	echo "write-attempt" >"$WORK/upload.txt"
	mc cp "$WORK/upload.txt" "exp/$BUCKET/upload.txt" >/dev/null 2>&1 \
		&& fail "write accepted on a read-only export" || ok "write rejected on a read-only export"
	mc alias set bad "https://127.0.0.1:$EXPORT_PORT" "$ACCESS" "wrong-secret" --api S3v4 --path on >/dev/null
	mc ls "bad/$BUCKET/" >/dev/null 2>&1 && fail "wrong secret accepted" || ok "wrong secret rejected"
	detach

	section "Samba cannot be placed on an agent"

	out=$(create_outpost -d "name=e2e-agent-smb" -d "type=samba" -d "agent=$AGENT" -d "guest=1") \
		&& { fail "samba outpost on an agent accepted"; delete_outpost e2e-agent-smb; } \
		|| { grep -qi agent <<<"$out" && ok "samba outpost on an agent rejected" || fail "samba rejected for another reason: $out"; }

	section "NFS outpost hosted by the agent"

	out=$(create_outpost -d "name=$AGENT_NFS" -d "type=nfs" -d "agent=$AGENT" -d "listen-addr=0.0.0.0:$AGENT_NFS_PORT") \
		&& ok "agent NFS outpost created" || fail "agent NFS outpost create rejected: $out"
	wait_for "agent tunnel established" 90 outpost_healthy "$AGENT_NFS"
	local_listener "$AGENT_NFS_PORT" && fail "server bound the agent outpost port itself" || ok "server does not bind the agent port"
	wait_for "agent listens on $AGENT_NFS_PORT" 30 tcp_open "$AGENT_ADDR" "$AGENT_NFS_PORT"
	attach "$AGENT_NFS"
	[[ "$(session_endpoint "$AGENT_NFS")" == nfs://$AGENT:$AGENT_NFS_PORT/* ]] \
		&& ok "endpoint advertises the agent host" || fail "unexpected endpoint: $(session_endpoint "$AGENT_NFS")"
	read_through_agent_nfs
	;;

offline)
	section "Agent goes offline"

	wait_for "agent outpost reports the tunnel down" 90 outpost_offline "$AGENT_NFS"
	echo "    status: $(outpost_field "$AGENT_NFS" '.error')"
	session_ready "$AGENT_NFS" && ok "share stays attached while the agent is offline" || fail "share dropped while the agent was offline"
	;;

recover)
	section "Agent comes back"

	wait_for "agent tunnel re-established" 120 outpost_healthy "$AGENT_NFS"
	wait_for "agent listens again" 30 tcp_open "$AGENT_ADDR" "$AGENT_NFS_PORT"
	read_through_agent_nfs
	detach

	section "S3 snapshot export hosted by the agent"

	out=$(create_outpost -d "name=$AGENT_S3" -d "type=s3-export" -d "agent=$AGENT" -d "listen-addr=0.0.0.0:$AGENT_S3_PORT" \
		--data-urlencode "s3-export=$(export_config)") \
		&& ok "agent s3-export outpost created" || fail "agent s3-export create rejected: $out"
	wait_for "agent s3-export tunnel established" 90 outpost_healthy "$AGENT_S3"
	attach "$AGENT_S3"
	BUCKET=$(session_endpoint "$AGENT_S3")
	BUCKET=${BUCKET##*/}
	mc alias set agent "https://$AGENT_ADDR:$AGENT_S3_PORT" "$ACCESS" "$SECRET" --api S3v4 --path on >/dev/null
	mc cat "agent/$BUCKET/$PROBE_FILE" >"$WORK/agent-s3.txt" 2>/dev/null \
		&& cmp -s "$WORK/agent-s3.txt" "$REFERENCE" \
		&& ok "agent-hosted S3 returns the same bytes" || fail "agent-hosted S3 download failed or differs"
	mc cp "$WORK/upload.txt" "agent/$BUCKET/upload.txt" >/dev/null 2>&1 \
		&& fail "write accepted through the agent" || ok "write rejected through the agent"
	detach

	section "Remove outposts"

	for name in "$EXPORT" "$AGENT_NFS" "$AGENT_S3"; do
		resp=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$name")
		submit_ok "$resp" && ok "$name removed" || fail "$name delete rejected: $(body_of "$resp")"
	done
	tcp_close() { ! tcp_open "$AGENT_ADDR" "$1"; }
	wait_for "agent stops listening on $AGENT_NFS_PORT" 30 tcp_close "$AGENT_NFS_PORT"
	wait_for "agent stops listening on $AGENT_S3_PORT" 30 tcp_close "$AGENT_S3_PORT"
	rm -rf "$WORK"
	;;

*)
	echo "unknown phase $PHASE" >&2
	exit 2
	;;
esac

finish
