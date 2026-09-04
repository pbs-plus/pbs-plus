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
INIT_GROUP_DIR="/mnt/test/ns/test/host/e2e-init"
ENC_DS=$(printf %s "$DATASTORE" | base64 -w0)
SMB_OUTPOST="smb"
SMB_SHARE_NAME="restore-public"
SAMBA_INCLUDE="/var/lib/pbs-plus/outposts/samba-$SMB_OUTPOST.conf"
CLIENT_SMB="/tmp/pbs-plus-vfs-smb"
ACL_OUTPOST="smb-acl"
ACL_INCLUDE="/var/lib/pbs-plus/outposts/samba-$ACL_OUTPOST.conf"
ACL_USER="e2erestore"
ACL_PASS="e2e-restore-pw"
CLIENT_SMB_ACL="/tmp/pbs-plus-vfs-smb-acl"

req() { curl -k -s "$@" -w "\nHTTP_CODE:%{http_code}"; }
code_of() { tail -1 <<<"$1" | sed 's/^HTTP_CODE://'; }
body_of() { sed '$d' <<<"$1"; }

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
	echo "--- samba include file ---"
	cat "$SAMBA_INCLUDE" 2>/dev/null || true
	cat "$ACL_INCLUDE" 2>/dev/null || true
	echo "--- mount process logs ---"
	ls -la /var/run/pbs-plus-mounts/ /run/pbs-plus-outposts/ 2>/dev/null || true
	tail -60 /var/run/pbs-plus-mounts/*.log 2>/dev/null || true
	echo "--- journal ---"
	journalctl --no-pager -n 200 2>/dev/null | tail -200 || true
	echo "--- mount task logs ---"
	find /var/log/proxmox-backup/tasks -type f 2>/dev/null | grep -E ':(mount|unmount):' | while read -r file; do
		echo "== $file =="
		tail -40 "$file" 2>/dev/null || true
	done
}

latest_snapshot() {
	ls -1 "$1" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1
}

repro_pxarmount() {
	echo "--- manual pxar-mount repro ---"
	local snap didx mpxar ppxar
	snap=$(latest_snapshot "$INIT_GROUP_DIR") || return 0
	didx=$(didx_in "$INIT_GROUP_DIR/$snap") || return 0
	mpxar="$INIT_GROUP_DIR/$snap/$didx"
	ppxar="$INIT_GROUP_DIR/$snap/$(printf %s "$didx" | sed 's/\.mpxar\.didx$/.ppxar.didx/')"
	mkdir -p /tmp/pxarmount-repro
	/usr/bin/pxar-mount --pbs-store /mnt/test --mpxar-didx "$mpxar" --ppxar-didx "$ppxar" /tmp/pxarmount-repro &
	local pid=$!
	for i in $(seq 1 15); do
		sleep 1
		if mountpoint -q /tmp/pxarmount-repro; then
			echo "mounted after ${i}s"
			ls /tmp/pxarmount-repro || true
			break
		fi
	done
	kill "$pid" 2>/dev/null || true
	umount /tmp/pxarmount-repro 2>/dev/null || true
}

didx_in() {
	ls -1 "$1" 2>/dev/null | grep -E '\.mpxar\.didx$' | head -1 || ls -1 "$1" 2>/dev/null | grep -E '\.pxar\.didx$' | head -1
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
	deadline=$((SECONDS + 120))
	while [ "$SECONDS" -lt "$deadline" ]; do
		snapshot_session_absent "$backup_id" "$backup_time" && return 0
		sleep 2
	done
	fail "existing $backup_id session removal timed out"
	return 1
}

session_ready() {
	local backup_id=$1
	local outpost=$2
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id "$backup_id" --arg outpost "$outpost" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id and .mounted == true and (.endpoint | length > 0))' \
		>/dev/null
}

session_endpoint() {
	local backup_id=$1
	local outpost=$2
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -r \
		--arg id "$backup_id" --arg outpost "$outpost" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id) | .endpoint' | head -1
}

session_mountpoint() {
	local backup_id=$1
	local outpost=$2
	curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -r \
		--arg id "$backup_id" --arg outpost "$outpost" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id) | .["mount-point"]' | head -1
}

session_gone() {
	local backup_id=$1
	local outpost=$2
	! curl -k -s "$PBS_API/api2/extjs/config/d2d-mounts" | jq -e \
		--arg id "$backup_id" --arg outpost "$outpost" \
		'.data[]? | select(.outpost == $outpost and .["backup-id"] == $id)' \
		>/dev/null
}

outpost_shares() {
	local name=$1
	local expected=$2
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" | jq -r \
		--arg name "$name" \
		'.data[]? | select(.name == $name) | (.attached // []) | length' | grep -qx "$expected"
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

cleanup_client_mounts() {
	for mount_point in "$CLIENT_SMB" "$CLIENT_SMB_ACL"; do
		if mountpoint -q "$mount_point"; then
			timeout 30 umount -f -l "$mount_point" >/dev/null 2>&1 || true
		fi
	done
	rm -rf "$CLIENT_SMB" "$CLIENT_SMB_ACL"
}

trap cleanup_client_mounts EXIT

wait_for_proc() {
	local name=$1
	local timeout=$2
	shift 2
	local deadline=$((SECONDS + timeout))
	while [ "$SECONDS" -lt "$deadline" ]; do
		if "$@" >/dev/null 2>&1; then
			ok "$name"
			return 0
		fi
		sleep 1
	done
	fail "$name (timeout after ${timeout}s)"
	return 1
}

section "Start smbd"

mkdir -p /etc/samba
cat > /etc/samba/smb.conf <<EOF
[global]
	workgroup = WORKGROUP
	security = user
	map to guest = Bad User
	guest account = root
	load printers = no
	include = $SAMBA_INCLUDE
	include = $ACL_INCLUDE
EOF
mkdir -p "$(dirname "$SAMBA_INCLUDE")"
touch "$SAMBA_INCLUDE" "$ACL_INCLUDE"
useradd -M -s /usr/sbin/nologin "$ACL_USER" 2>/dev/null || true
printf '%s\n%s\n' "$ACL_PASS" "$ACL_PASS" | smbpasswd -s -a "$ACL_USER" >/dev/null 2>&1 \
	&& ok "samba local restore account created" || fail "smbpasswd could not create $ACL_USER"
smbd -D
wait_for_proc "smbd listening on 445" 30 ss -lnt sport = :445

section "Locate snapshot"

SNAP=$(latest_snapshot "$INIT_GROUP_DIR")
[ -n "$SNAP" ] || { fail "e2e-init snapshot missing"; exit 1; }
DIDX=$(didx_in "$INIT_GROUP_DIR/$SNAP")
[ -n "$DIDX" ] || { fail "e2e-init archive missing"; exit 1; }

detach_existing_session e2e-init "$SNAP" "$DIDX" || true

section "Serve one share through samba"

RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=$SMB_OUTPOST" -d "type=samba" -d "guest=1" -d "browseable=1")
submit_ok "$RESP" && ok "samba outpost created" || fail "samba outpost rejected: $(body_of "$RESP")"

RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "mode=ro" -d "outpost=$SMB_OUTPOST" \
	-d "share-name=$SMB_SHARE_NAME")
submit_ok "$RESP" && ok "samba share request accepted" || fail "samba share rejected: $(body_of "$RESP")"

wait_for "samba share attached" 240 session_ready e2e-init "$SMB_OUTPOST" || true
wait_for "samba outpost reports one share" 30 outpost_shares "$SMB_OUTPOST" 1 || true

ENDPOINT=$(session_endpoint e2e-init "$SMB_OUTPOST")
SMB_SHARE=${ENDPOINT##*/}
[ "$SMB_SHARE" = "$SMB_SHARE_NAME" ] && ok "custom samba share name reported: $ENDPOINT" \
	|| fail "custom samba share name mismatch: $ENDPOINT"
grep -q "\\[$SMB_SHARE\\]" "$SAMBA_INCLUDE" 2>/dev/null \
	&& ok "samba include file has share stanza" || fail "samba include file missing share stanza"
grep -q "browseable = yes" "$SAMBA_INCLUDE" 2>/dev/null \
	&& ok "samba share marked browseable" || fail "samba share not marked browseable"
smbclient -L //127.0.0.1 -N -g 2>/dev/null | grep -Fq "Disk|$SMB_SHARE|" \
	&& ok "samba share visible in server enumeration" || fail "samba share missing from server enumeration"

mkdir -p "$CLIENT_SMB"
if [ -n "$SMB_SHARE" ] && timeout 60 mount -t cifs \
	-o "guest,vers=3.0" "//127.0.0.1/$SMB_SHARE" "$CLIENT_SMB"; then
	ok "share mounted through samba CIFS"
else
	fail "samba CIFS mount failed (share: $SMB_SHARE)"
fi

mountpoint -q "$CLIENT_SMB" && ok "samba kernel mount active" || fail "samba kernel mount inactive"
[ "$(cat "$CLIENT_SMB/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "samba share content readable" || fail "samba share content mismatch"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "force=1")
submit_ok "$RESP" && ok "samba detach accepted" || fail "samba detach rejected: $(body_of "$RESP")"
wait_for "samba session removed" 120 session_gone e2e-init "$SMB_OUTPOST" || true
wait_for "samba outpost reports no shares" 30 outpost_shares "$SMB_OUTPOST" 0 || true

RESP=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$SMB_OUTPOST")
submit_ok "$RESP" && ok "samba outpost removed" || fail "samba outpost delete rejected: $(body_of "$RESP")"

section "Enforce samba access policy"

RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=$ACL_OUTPOST" -d "type=samba")
submit_ok "$RESP" && fail "samba outpost without access policy accepted" \
	|| ok "samba outpost without guest or valid users rejected"

RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=$ACL_OUTPOST" -d "type=samba" -d "guest=1" -d "valid-users=$ACL_USER")
submit_ok "$RESP" && fail "samba outpost with guest and valid users accepted" \
	|| ok "samba outpost with contradictory access rejected"

RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=$ACL_OUTPOST" -d "type=samba" -d "valid-users=$ACL_USER" -d "force-user=$ACL_USER")
submit_ok "$RESP" && ok "samba outpost with valid users created" \
	|| fail "samba acl outpost rejected: $(body_of "$RESP")"

RESP=$(api_post "/api2/extjs/config/d2d-mount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "mode=rw" -d "outpost=$ACL_OUTPOST")
submit_ok "$RESP" && ok "samba acl share request accepted" || fail "samba acl share rejected: $(body_of "$RESP")"

wait_for "samba acl share attached" 240 session_ready e2e-init "$ACL_OUTPOST" || true

ACL_SHARE=$(session_endpoint e2e-init "$ACL_OUTPOST")
ACL_SHARE=${ACL_SHARE##*/}
[ -n "$ACL_SHARE" ] && ok "samba acl endpoint reported: $ACL_SHARE" || fail "samba acl endpoint missing"
grep -q "valid users = $ACL_USER" "$ACL_INCLUDE" 2>/dev/null \
	&& ok "acl share stanza carries valid users" || fail "acl share stanza missing valid users"
grep -q "force user = $ACL_USER" "$ACL_INCLUDE" 2>/dev/null \
	&& ok "acl share stanza carries force user" || fail "acl share stanza missing force user"

mkdir -p "$CLIENT_SMB_ACL"
if [ -n "$ACL_SHARE" ] && timeout 60 mount -t cifs \
	-o "guest,vers=3.0" "//127.0.0.1/$ACL_SHARE" "$CLIENT_SMB_ACL" 2>/dev/null; then
	fail "guest mounted a share restricted to valid users"
	timeout 30 umount -f -l "$CLIENT_SMB_ACL" >/dev/null 2>&1 || true
else
	ok "guest denied on share restricted to valid users"
fi

if [ -n "$ACL_SHARE" ] && timeout 60 mount -t cifs \
	-o "username=$ACL_USER,password=$ACL_PASS,vers=3.0" "//127.0.0.1/$ACL_SHARE" "$CLIENT_SMB_ACL"; then
	ok "valid user mounted the restricted share"
else
	fail "valid user could not mount the restricted share (share: $ACL_SHARE)"
fi
[ "$(cat "$CLIENT_SMB_ACL/hello.txt" 2>/dev/null)" = "hello-e2e" ] \
	&& ok "restricted share content readable as valid user" || fail "restricted share content mismatch"
printf 'rw-e2e\n' > "$CLIENT_SMB_ACL/created-by-samba.txt" 2>/dev/null \
	&& ok "valid user can write through rw share" || fail "valid user cannot write through rw share"
[ "$(cat "$CLIENT_SMB_ACL/created-by-samba.txt" 2>/dev/null)" = "rw-e2e" ] \
	&& ok "rw share write is readable" || fail "rw share write mismatch"
ACL_MOUNTPOINT=$(session_mountpoint e2e-init "$ACL_OUTPOST")
ACL_UID=$(id -u "$ACL_USER")
ACL_GID=$(id -g "$ACL_USER")
[ "$(stat -c '%u:%g' "$ACL_MOUNTPOINT" 2>/dev/null)" = "$ACL_UID:$ACL_GID" ] \
	&& ok "pxar mount ownership matches samba force user" || fail "pxar mount ownership does not match samba force user"

RESP=$(api_post "/api2/extjs/config/d2d-unmount/$ENC_DS" \
	-d "ns=$NAMESPACE" -d "backup-type=host" -d "backup-id=e2e-init" \
	-d "backup-time=$SNAP" -d "file-name=$DIDX" -d "force=1")
submit_ok "$RESP" && ok "samba acl detach accepted" || fail "samba acl detach rejected: $(body_of "$RESP")"
wait_for "samba acl session removed" 120 session_gone e2e-init "$ACL_OUTPOST" || true

RESP=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$ACL_OUTPOST")
submit_ok "$RESP" && ok "samba acl outpost removed" || fail "samba acl outpost delete rejected: $(body_of "$RESP")"

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
	repro_pxarmount
	exit 1
fi
echo "  ALL TESTS PASSED"
