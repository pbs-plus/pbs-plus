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
OUTPOST="e2e-s3"
S3_PORT=39000
BUCKET="mariadb-e2e"
ACCESS="e2e-access"
SECRET="e2e-secret-key"
GROUP_DIR="/mnt/$DATASTORE/ns/$NAMESPACE/host/e2e-s3"
WORK="/tmp/s3-e2e"
ENDPOINT="https://127.0.0.1:$S3_PORT"

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
	echo "--- s3 outpost log trace (last 400 lines) ---"
	docker logs --since 30m pbs-plus-test 2>&1 | grep -Ei 's3 outpost|objectstore|s3 put|s3 delete' | tail -400 || true
	echo "--- S3 listener ---"
	ss -lntp 2>/dev/null | grep ":$S3_PORT" || true
	echo "--- object snapshots on disk ---"
	ls -la "$GROUP_DIR" 2>/dev/null || true
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

s3_config() {
	cat <<'JSON'
{
	"region": "us-east-1",
	"buckets": [{
		"name": "mariadb-e2e",
		"datastore": "test",
		"namespace": "test",
		"backup_type": "host",
		"backup_id": "e2e-s3"
	}],
	"credentials": [{
		"access_key": "e2e-access",
		"secret_key": "e2e-secret-key",
		"auth_id": "root@pam",
		"grants": [{"bucket": "mariadb-e2e", "read": true, "write": true, "delete": true}]
	}]
}
JSON
}

outpost_running() {
	curl -k -s "$PBS_API/api2/extjs/config/d2d-outposts" | jq -e \
		--arg name "$OUTPOST" \
		'.data[]? | select(.name == $name and .running == true)' \
		>/dev/null
}

snapshot_count() {
	local expected=$1
	local count
	count=$(ls -1 "$GROUP_DIR" 2>/dev/null | grep -cE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' || true)
	[ "$count" = "$expected" ]
}

install_mc() {
	if type -P mc >/dev/null 2>&1; then
		return 0
	fi
	local target=/usr/local/bin/mc
	if [ ! -w /usr/local/bin ]; then
		target=/tmp/mc
	fi
	if curl -fsSL -o "$target" https://dl.min.io/client/mc/release/linux-amd64/mc \
		&& chmod +x "$target"; then
		return 0
	fi
	return 1
}

cleanup_workdir() {
	rm -rf "$WORK"
}

trap 'dump_logs' EXIT

cleanup_workdir
mkdir -p "$WORK"

section "Create S3 outpost"

req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$OUTPOST" >/dev/null 2>&1 || true

S3_JSON=$(s3_config)
RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=$OUTPOST" -d "type=s3" -d "listen-addr=0.0.0.0:$S3_PORT" \
	--data-urlencode "s3=$S3_JSON")
submit_ok "$RESP" && ok "s3 outpost created" || fail "s3 outpost create rejected: $(body_of "$RESP")"

wait_for "s3 outpost running" 30 outpost_running

ss -lnt 2>/dev/null | grep -q ":$S3_PORT" \
	&& ok "s3 listener bound on port $S3_PORT" \
	|| fail "s3 listener missing on port $S3_PORT"

section "Push objects through the real minio client"

install_mc \
	&& ok "minio client available" \
	|| fail "minio client could not be installed"

mc alias set e2e "$ENDPOINT" "$ACCESS" "$SECRET" >/dev/null 2>&1 \
	&& ok "minio client authenticated against the outpost" \
	|| fail "minio client alias setup failed"

printf 's3-e2e-payload-v1\n' >"$WORK/small.txt"
head -c 131072 /dev/urandom >"$WORK/daily backup.sql.gz"
head -c 41943040 /dev/urandom >"$WORK/big.bin"

mc cp "$WORK/small.txt" "e2e/$BUCKET/small.txt" >/dev/null 2>&1 \
	&& ok "small object pushed" || fail "small object push failed"
mc cp "$WORK/daily backup.sql.gz" "e2e/$BUCKET/daily backup.sql.gz" >/dev/null 2>&1 \
	&& ok "object key with space pushed" || fail "object key with space push failed"
mc cp "$WORK/big.bin" "e2e/$BUCKET/big.bin" >/dev/null 2>&1 \
	&& ok "40MiB object pushed" || fail "40MiB object push failed"

MULTIPART_ETAG=$(mc ls --json "e2e/$BUCKET/big.bin" 2>/dev/null | jq -r '.etag // empty' | tr -d '"')
if [ -n "$MULTIPART_ETAG" ] && grep -qE -- '-[0-9]+$' <<<"$MULTIPART_ETAG"; then
	ok "multipart upload etag is composite: $MULTIPART_ETAG"
else
	fail "multipart upload etag not composite: '$MULTIPART_ETAG'"
fi

section "Objects land as PBS snapshots"

wait_for "three object snapshots published" 60 snapshot_count 3

SNAPSHOT_DIR=$(ls -1 "$GROUP_DIR" | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | tail -1)
for file in s3-object.didx index.json.blob; do
	[ -s "$GROUP_DIR/$SNAPSHOT_DIR/$file" ] \
		&& ok "snapshot contains $file" || fail "snapshot missing $file"
done
[ -f "$GROUP_DIR/owner" ] \
	&& ok "backup group carries an owner file" || fail "backup group owner file missing"

section "Read objects back"

mc cat "e2e/$BUCKET/small.txt" 2>/dev/null | md5sum | cut -d' ' -f1 >"$WORK/got-small.md5"
md5sum "$WORK/small.txt" | cut -d' ' -f1 >"$WORK/want-small.md5"
cmp -s "$WORK/got-small.md5" "$WORK/want-small.md5" \
	&& ok "small object content round trips" || fail "small object content mismatch"

mc cat "e2e/$BUCKET/daily backup.sql.gz" 2>/dev/null | md5sum | cut -d' ' -f1 >"$WORK/got-space.md5"
md5sum "$WORK/daily backup.sql.gz" | cut -d' ' -f1 >"$WORK/want-space.md5"
cmp -s "$WORK/got-space.md5" "$WORK/want-space.md5" \
	&& ok "spaced key content round trips" || fail "spaced key content mismatch"

mc cat "e2e/$BUCKET/big.bin" 2>/dev/null | md5sum | cut -d' ' -f1 >"$WORK/got-big.md5"
md5sum "$WORK/big.bin" | cut -d' ' -f1 >"$WORK/want-big.md5"
cmp -s "$WORK/got-big.md5" "$WORK/want-big.md5" \
	&& ok "multipart object content round trips" || fail "multipart object content mismatch"

LISTED_KEY=$(mc ls --json "e2e/$BUCKET" 2>/dev/null | jq -r 'select(.type == "file") | .key' | grep -F 'daily backup.sql.gz' | head -1)
[ -n "$LISTED_KEY" ] \
	&& ok "listing returns the spaced key unencoded: $LISTED_KEY" \
	|| fail "listing lost the spaced key"

mc cat "e2e/$BUCKET/missing.sql.gz" >/dev/null 2>&1 \
	&& fail "missing object read succeeded" \
	|| ok "missing object read fails with NoSuchKey"

section "Overwrite creates a new snapshot version"

printf 's3-e2e-payload-v2\n' >"$WORK/small.txt"
mc cp "$WORK/small.txt" "e2e/$BUCKET/small.txt" >/dev/null 2>&1 \
	&& ok "overwrite pushed" || fail "overwrite push failed"

wait_for "overwrite published a fourth snapshot" 60 snapshot_count 4

mc cat "e2e/$BUCKET/small.txt" 2>/dev/null | grep -q 's3-e2e-payload-v2' \
	&& ok "reads return the newest version" || fail "reads returned a stale version"

section "Delete objects"

mc rm "e2e/$BUCKET/small.txt" >/dev/null 2>&1 \
	&& ok "small object removed" || fail "small object remove failed"
mc rm "e2e/$BUCKET/daily backup.sql.gz" >/dev/null 2>&1 \
	&& ok "spaced key object removed" || fail "spaced key object remove failed"
mc rm "e2e/$BUCKET/big.bin" >/dev/null 2>&1 \
	&& ok "multipart object removed" || fail "multipart object remove failed"

wait_for "all object snapshots removed from the datastore" 60 snapshot_count 0

mc ls "e2e/$BUCKET" 2>/dev/null | grep -q . \
	&& fail "listing still shows objects" \
	|| ok "listing is empty after deletes"

section "Remove S3 outpost"

RESP=$(req -X DELETE "$PBS_API/api2/extjs/config/d2d-outposts/$OUTPOST")
submit_ok "$RESP" && ok "s3 outpost removed" || fail "s3 outpost delete rejected: $(body_of "$RESP")"

sleep 2
ss -lnt 2>/dev/null | grep -q ":$S3_PORT" \
	&& fail "s3 listener still bound after removal" \
	|| ok "s3 listener closed after removal"

section "Enforce S3 config policy"

BAD_JSON=$(s3_config | jq -c '.buckets[0].name = "invalid_bucket_name"')
RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=e2e-s3-bad" -d "type=s3" -d "listen-addr=0.0.0.0:39001" \
	--data-urlencode "s3=$BAD_JSON")
submit_ok "$RESP" && fail "invalid bucket name accepted" \
	|| ok "invalid bucket name rejected"

BAD_JSON=$(s3_config | jq -c '.credentials[0].auth_id = "not an auth id"')
RESP=$(api_post "/api2/extjs/config/d2d-outposts" \
	-d "name=e2e-s3-bad" -d "type=s3" -d "listen-addr=0.0.0.0:39001" \
	--data-urlencode "s3=$BAD_JSON")
submit_ok "$RESP" && fail "invalid auth id accepted" \
	|| ok "invalid auth id rejected"

cleanup_workdir

section "RESULTS"

TOTAL=$((PASS + FAIL))
echo ""
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "  Total:  $TOTAL"
echo ""
if [ "$FAIL" -gt 0 ]; then
	echo "  SOME TESTS FAILED"
	exit 1
fi
echo "  ALL TESTS PASSED"
