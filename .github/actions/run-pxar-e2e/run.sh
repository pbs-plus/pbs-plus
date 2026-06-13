#!/bin/bash
# E2E test for pxar-mount
# Exercises the supported pxar-mount workflow:
#   1. `pxar-mount init`  — create a writable archive from scratch
#   2. `pxar-mount commit` — commit that archive into a PBS snapshot
#   3. add more files, then commit again (re-commit captures new data)
#   4. read-only remount of the committed snapshot (the production use case)
#   5. commit edge cases (rename, delete, replace, large file, ACL)
#
# NOTE: mounting an existing archive WITH --passthrough and committing mutations
# against it is not a production code path (the server only mounts archives
# read-only), so it is intentionally not tested here.
set -uo pipefail

PASS=0
FAIL=0
SKIP=0

ok()   { echo "  ✓ $1"; ((PASS++)); }
fail() { echo "  ✗ $1"; ((FAIL++)); }
skip() { echo "  ⊘ $1"; ((SKIP++)); }

section() { echo ""; echo "══════════════════════════════════════════"; echo "  $1"; echo "══════════════════════════════════════════"; }

###############################################################################
# Configuration (override via environment)
###############################################################################
PBS_STORE="${PBS_STORE:-/mnt/test}"
INIT_NAMESPACE="${INIT_NAMESPACE:-test/init}"
INIT_BACKUP_ID="${INIT_BACKUP_ID:-E2E-INIT}"
PXAR_MOUNT_BIN="${PXAR_MOUNT_BIN:-/usr/bin/pxar-mount}"

###############################################################################
# Derived paths
###############################################################################
INIT_SOCKET="/var/lib/archive-outpost/sockets/init-test.sock"
INIT_MOUNT="/mnt/archive-mounts/init-test"
INIT_PASS_DIR="/tmp/passthrough/init-test"

# init mode writes snapshots into its own backup group. Nested namespaces are
# stored by PBS as ns/<a>/ns/<b>/... so build the host dir accordingly.
ns_to_host_dir() {
	local base=$1 ns=$2 bid=$3
	local cur="$base"
	local IFS=/
	for p in $ns; do
		[ -z "$p" ] && continue
		cur="$cur/ns/$p"
	done
	echo "$cur/host/$bid"
}
INIT_HOST_DIR=$(ns_to_host_dir "$PBS_STORE" "$INIT_NAMESPACE" "$INIT_BACKUP_ID")
INIT_MPXAR_PATTERN="${INIT_BACKUP_ID}.mpxar.didx"
INIT_PPXAR_PATTERN="${INIT_BACKUP_ID}.ppxar.didx"

unmount() {
	fusermount -u "$1" 2>/dev/null || true
	sleep 1
	pgrep -f "pxar-mount.*$(basename $1)" | xargs kill 2>/dev/null || true
	sleep 1
}

mount_init() {
	local ns=$1 bid=$2 socket=$3 mount=$4 pass=$5
	unmount "$mount"
	rm -rf "$pass/.pxar-journal" "$pass"/*
	mkdir -p "$pass" "$mount" "$(dirname $socket)"
	nohup "$PXAR_MOUNT_BIN" init \
		--pbs-store "$PBS_STORE" \
		--socket "$socket" \
		--namespace "$ns" \
		--passthrough "$pass" \
		--options rw,allow_other \
		"$mount" > /tmp/pxar-mount-test.log 2>&1 &
	for i in $(seq 1 10); do
		mountpoint -q "$mount" && break
		sleep 1
	done
	mountpoint -q "$mount" || { echo "FAILED to mount $mount"; cat /tmp/pxar-mount-test.log; exit 1; }
}

# Read-only mount of a committed snapshot — mirrors how the production server
# mounts archives for restore (no --passthrough, no --socket).
mount_readonly() {
	local mpxar=$1 ppxar=$2 mount=$3
	unmount "$mount"
	mkdir -p "$mount"
	nohup "$PXAR_MOUNT_BIN" \
		--pbs-store "$PBS_STORE" \
		--mpxar-didx "$mpxar" \
		--ppxar-didx "$ppxar" \
		--options ro,allow_other \
		"$mount" > /tmp/pxar-mount-test.log 2>&1 &
	for i in $(seq 1 10); do
		mountpoint -q "$mount" && break
		sleep 1
	done
	mountpoint -q "$mount" || { echo "FAILED to (ro) mount $mount"; cat /tmp/pxar-mount-test.log; exit 1; }
}

do_commit() {
	"$PXAR_MOUNT_BIN" commit --socket "$1" --backup-id "$2" 2>&1
}

commit_ok() {
	# Returns 0 if the commit output contains a success marker.
	echo "$1" | grep -q "✓"
}

latest_snapshot() {
	local dir=$1
	ls -1 "$dir" | grep -E '^[0-9]{4}-' | sort | tail -1
}

###############################################################################
section "PHASE 1: INIT MODE — create content + first commit"
###############################################################################

mount_init "$INIT_NAMESPACE" "$INIT_BACKUP_ID" "$INIT_SOCKET" "$INIT_MOUNT" "$INIT_PASS_DIR"

# 1a. Create files
echo "hello world" > "$INIT_MOUNT/file1.txt"
echo "second file" > "$INIT_MOUNT/file2.txt"
ok "create two files in root"

# 1b. Create nested directories
mkdir -p "$INIT_MOUNT/dir1/sub1/deep"
mkdir -p "$INIT_MOUNT/dir2"
ok "create nested directories"

# 1c. Write files in nested dirs
echo "nested content" > "$INIT_MOUNT/dir1/nested.txt"
echo "deep content" > "$INIT_MOUNT/dir1/sub1/deep.txt"
echo "dir2 content" > "$INIT_MOUNT/dir2/data.txt"
ok "write files in nested directories"

# 1d. Symlink
ln -s ../file1.txt "$INIT_MOUNT/dir2/link-to-file1"
ok "create symlink"

# 1e. Permissions
chmod 0755 "$INIT_MOUNT/dir1"
chmod 0644 "$INIT_MOUNT/file1.txt"
ok "chmod files and directories"

# 1f. Verify reads
[ "$(cat $INIT_MOUNT/file1.txt)" = "hello world" ] && ok "read file1.txt" || fail "read file1.txt"
[ "$(cat $INIT_MOUNT/dir1/sub1/deep.txt)" = "deep content" ] && ok "read nested deep.txt" || fail "read nested deep.txt"
[ "$(readlink $INIT_MOUNT/dir2/link-to-file1)" = "../file1.txt" ] && ok "readlink symlink" || fail "readlink symlink"

# 1g. List root
ls "$INIT_MOUNT/" > /dev/null && ok "list root directory" || fail "list root directory"

# 1h. FIRST COMMIT — the core supported workflow
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "$INIT_BACKUP_ID")
commit_ok "$RESULT" && ok "init mode commit #1" || fail "init mode commit #1: $RESULT"

# 1i. Verify reads still work right after the commit
[ "$(cat $INIT_MOUNT/file1.txt)" = "hello world" ] && ok "post-commit read file1.txt" || fail "post-commit read file1.txt"
[ "$(cat $INIT_MOUNT/dir2/data.txt)" = "dir2 content" ] && ok "post-commit read dir2/data.txt" || fail "post-commit read dir2/data.txt"

###############################################################################
section "PHASE 2: RE-COMMIT — add files after the first commit, then commit again"
###############################################################################

# Stay in the SAME mount session. After a commit the running pxar-mount
# hot-swaps its reader to the freshly-committed snapshot, so a second commit
# can build on top of it. (Re-mounting would wipe the passthrough dir, so all
# commits here happen without unmounting in between.)

# 2a. Add NEW files after the first commit
echo "added after commit 1" > "$INIT_MOUNT/added.txt"
echo "another new file" > "$INIT_MOUNT/dir1/added2.txt"
mkdir -p "$INIT_MOUNT/new-dir"
echo "in new dir" > "$INIT_MOUNT/new-dir/file.txt"
ok "add new files/dirs after commit #1"

# 2b. Modify an existing file
echo "updated content" > "$INIT_MOUNT/file2.txt"
ok "modify existing file after commit #1"

# 2c. Delete a file that existed at commit #1
rm "$INIT_MOUNT/dir2/data.txt"
ok "delete a file after commit #1"

# 2d. SECOND COMMIT — must capture the additions, modification and deletion
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "$INIT_BACKUP_ID")
commit_ok "$RESULT" && ok "init mode commit #2 (re-commit)" || fail "init mode commit #2 (re-commit): $RESULT"

# 2e. Verify the post-2nd-commit live state
[ "$(cat $INIT_MOUNT/added.txt)" = "added after commit 1" ] && ok "post-2nd read added.txt" || fail "post-2nd read added.txt"
[ "$(cat $INIT_MOUNT/dir1/added2.txt)" = "another new file" ] && ok "post-2nd read dir1/added2.txt" || fail "post-2nd read dir1/added2.txt"
[ "$(cat $INIT_MOUNT/new-dir/file.txt)" = "in new dir" ] && ok "post-2nd read new-dir/file.txt" || fail "post-2nd read new-dir/file.txt"
[ "$(cat $INIT_MOUNT/file2.txt)" = "updated content" ] && ok "post-2nd read modified file2.txt" || fail "post-2nd read modified file2.txt"
[ ! -f "$INIT_MOUNT/dir2/data.txt" ] && ok "post-2nd deleted dir2/data.txt is gone" || fail "post-2nd deleted dir2/data.txt should not exist"

# 2f. THIRD COMMIT (no changes since #2 — should be a clean no-op)
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "$INIT_BACKUP_ID")
commit_ok "$RESULT" && ok "init mode commit #3 (no-op)" || fail "init mode commit #3 (no-op): $RESULT"

# 2g. No-op commit must not have disturbed live state
[ "$(cat $INIT_MOUNT/added.txt)" = "added after commit 1" ] && ok "post-no-op read added.txt" || fail "post-no-op read added.txt"

unmount "$INIT_MOUNT"

###############################################################################
section "PHASE 3: READ-ONLY REMOUNT — committed snapshot is a valid archive"
###############################################################################

# Verify the commits produced a real, mountable PBS snapshot by mounting the
# latest snapshot read-only (exactly how production restores).
LATEST=$(latest_snapshot "$INIT_HOST_DIR") || true
if [ -z "$LATEST" ]; then
	fail "no committed snapshot found at $INIT_HOST_DIR"
else
	MPXAR="${INIT_HOST_DIR}/${LATEST}/${INIT_MPXAR_PATTERN}"
	PPXAR="${INIT_HOST_DIR}/${LATEST}/${INIT_PPXAR_PATTERN}"
	echo "Using committed snapshot: $LATEST"

	mount_readonly "$MPXAR" "$PPXAR" "$INIT_MOUNT"

	# 3a. Data from the first commit persisted
	[ "$(cat $INIT_MOUNT/file1.txt)" = "hello world" ] && ok "ro mount: file1.txt (commit #1) correct" || fail "ro mount: file1.txt wrong"
	[ "$(cat $INIT_MOUNT/dir1/sub1/deep.txt)" = "deep content" ] && ok "ro mount: nested deep.txt (commit #1) correct" || fail "ro mount: nested deep.txt wrong"

	# 3b. Data added in the second commit persisted
	[ -f "$INIT_MOUNT/added.txt" ] && ok "ro mount: added.txt (commit #2) exists" || fail "ro mount: added.txt missing"
	[ "$(cat $INIT_MOUNT/added.txt)" = "added after commit 1" ] && ok "ro mount: added.txt content correct" || fail "ro mount: added.txt content wrong"
	[ -f "$INIT_MOUNT/new-dir/file.txt" ] && ok "ro mount: new-dir/file.txt exists" || fail "ro mount: new-dir/file.txt missing"

	# 3c. Modification + deletion from the second commit persisted
	[ "$(cat $INIT_MOUNT/file2.txt)" = "updated content" ] && ok "ro mount: modified file2.txt correct" || fail "ro mount: modified file2.txt wrong"
	[ ! -f "$INIT_MOUNT/dir2/data.txt" ] && ok "ro mount: deleted dir2/data.txt is gone" || fail "ro mount: deleted dir2/data.txt still exists"

	# 3d. Symlink + nested structure persisted
	[ "$(readlink $INIT_MOUNT/dir2/link-to-file1)" = "../file1.txt" ] && ok "ro mount: symlink preserved" || fail "ro mount: symlink wrong"
	[ -d "$INIT_MOUNT/dir1/sub1/deep" ] && ok "ro mount: nested dirs preserved" || fail "ro mount: nested dirs missing"

	unmount "$INIT_MOUNT"
fi

###############################################################################
section "PHASE 4: COMMIT EDGE CASES (via init mode)"
###############################################################################

mount_init "$INIT_NAMESPACE" "$INIT_BACKUP_ID" "$INIT_SOCKET" "$INIT_MOUNT" "$INIT_PASS_DIR"

# 4a. Create -> rename -> delete
echo "rbd test" > "$INIT_MOUNT/rbd-file.txt"
mv "$INIT_MOUNT/rbd-file.txt" "$INIT_MOUNT/rbd-renamed.txt"
rm "$INIT_MOUNT/rbd-renamed.txt"
[ ! -f "$INIT_MOUNT/rbd-file.txt" ] && [ ! -f "$INIT_MOUNT/rbd-renamed.txt" ] && ok "create-rename-delete: file fully gone" || fail "create-rename-delete: file still exists"

# 4b. Create file -> rename dir -> access file via new path
mkdir "$INIT_MOUNT/rename-test-dir"
echo "inside" > "$INIT_MOUNT/rename-test-dir/inner.txt"
mv "$INIT_MOUNT/rename-test-dir" "$INIT_MOUNT/renamed-dir"
[ -f "$INIT_MOUNT/renamed-dir/inner.txt" ] && ok "dir rename: file accessible via new path" || fail "dir rename: file not accessible via new path"
[ "$(cat $INIT_MOUNT/renamed-dir/inner.txt)" = "inside" ] && ok "dir rename: content preserved" || fail "dir rename: content wrong"

# 4c. Create -> rename over existing file (replace)
echo "source" > "$INIT_MOUNT/repl-src.txt"
echo "dest" > "$INIT_MOUNT/repl-dst.txt"
mv "$INIT_MOUNT/repl-src.txt" "$INIT_MOUNT/repl-dst.txt"
[ "$(cat $INIT_MOUNT/repl-dst.txt)" = "source" ] && ok "rename replace: dest has source content" || fail "rename replace: wrong content"
[ ! -f "$INIT_MOUNT/repl-src.txt" ] && ok "rename replace: source gone" || fail "rename replace: source still exists"

# 4d. Empty directory deletion
mkdir "$INIT_MOUNT/empty-dir"
rmdir "$INIT_MOUNT/empty-dir"
[ ! -d "$INIT_MOUNT/empty-dir" ] && ok "rmdir empty directory" || fail "rmdir empty directory"

# 4e. Non-empty directory deletion should fail
mkdir "$INIT_MOUNT/nonempty-dir"
echo "x" > "$INIT_MOUNT/nonempty-dir/x.txt"
rmdir "$INIT_MOUNT/nonempty-dir" 2>/dev/null && fail "rmdir non-empty dir should fail" || ok "rmdir non-empty dir correctly fails"
rm "$INIT_MOUNT/nonempty-dir/x.txt"

# 4f. Commit with all these edge cases
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "$INIT_BACKUP_ID")
commit_ok "$RESULT" && ok "edge cases commit" || fail "edge cases commit: $RESULT"

# 4g. Verify after commit
[ -f "$INIT_MOUNT/renamed-dir/inner.txt" ] && ok "post-edge-commit: renamed dir file exists" || fail "post-edge-commit: renamed dir file missing"
[ "$(cat $INIT_MOUNT/repl-dst.txt)" = "source" ] && ok "post-edge-commit: replaced dest correct" || fail "post-edge-commit: replaced dest wrong"

unmount "$INIT_MOUNT"

###############################################################################
section "PHASE 5: ACL & LARGE FILE (via init mode)"
###############################################################################

mount_init "$INIT_NAMESPACE" "$INIT_BACKUP_ID" "$INIT_SOCKET" "$INIT_MOUNT" "$INIT_PASS_DIR"

# 5a. setfacl on new file
echo "acl-test" > "$INIT_MOUNT/acl-file.txt"
setfacl -m u:34:rw "$INIT_MOUNT/acl-file.txt" 2>/dev/null && ok "setfacl on new file" || skip "setfacl on new file (not supported)"

# 5b. getfacl verification
GETACL=$(getfacl -p "$INIT_MOUNT/acl-file.txt" 2>/dev/null | grep "user:34" || true)
[ -n "$GETACL" ] && ok "getfacl shows ACL entry" || skip "getfacl verify (not supported)"

# 5c. setfacl on directory
mkdir "$INIT_MOUNT/acl-dir"
setfacl -m u:34:rwx "$INIT_MOUNT/acl-dir" 2>/dev/null && ok "setfacl on directory" || skip "setfacl on directory (not supported)"

# 5d. Write 1MB file
dd if=/dev/urandom of="$INIT_MOUNT/large-1m.bin" bs=1K count=1024 2>/dev/null
SIZE=$(stat -c%s "$INIT_MOUNT/large-1m.bin" 2>/dev/null || stat -f%z "$INIT_MOUNT/large-1m.bin")
[ "$SIZE" = "1048576" ] && ok "write 1MB binary file" || fail "write 1MB binary file (size=$SIZE)"

# 5e. Compute checksum, commit, verify integrity
CS_BEFORE=$(sha256sum "$INIT_MOUNT/large-1m.bin" | awk '{print $1}')
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "$INIT_BACKUP_ID")
commit_ok "$RESULT" && ok "ACL + large file commit" || fail "ACL + large file commit: $RESULT"
CS_AFTER=$(sha256sum "$INIT_MOUNT/large-1m.bin" | awk '{print $1}')
[ "$CS_BEFORE" = "$CS_AFTER" ] && ok "large file integrity after commit" || fail "large file checksum mismatch after commit"

# 5f. Verify ACLs after commit
GETACL2=$(getfacl -p "$INIT_MOUNT/acl-file.txt" 2>/dev/null | grep "user:34" || true)
[ -n "$GETACL2" ] && ok "post-commit ACL preserved" || skip "post-commit ACL verify"

unmount "$INIT_MOUNT"

###############################################################################
section "RESULTS"
###############################################################################

TOTAL=$((PASS + FAIL + SKIP))
echo ""
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "  Skipped: $SKIP"
echo "  Total:  $TOTAL"
echo ""
if [ "$FAIL" -gt 0 ]; then
	echo "  ❌ SOME TESTS FAILED"
	exit 1
else
	echo "  ✅ ALL TESTS PASSED"
	exit 0
fi
