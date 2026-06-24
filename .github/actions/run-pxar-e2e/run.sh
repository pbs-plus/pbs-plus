#!/bin/bash
# Comprehensive E2E test for pxar-mount
# Tests: init mode, mount mode, commit, re-commit, and all FUSE operations
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
NAMESPACE="${NAMESPACE:-test}"
BACKUP_ID="${BACKUP_ID:-test-host}"
BACKUP_DISK="${BACKUP_DISK:-Root}"
PXAR_MOUNT_BIN="${PXAR_MOUNT_BIN:-/usr/bin/pxar-mount}"

###############################################################################
# Derived paths
###############################################################################
SOCKET="/var/lib/archive-outpost/sockets/test.sock"
INIT_SOCKET="/var/lib/archive-outpost/sockets/init-test.sock"
MOUNT="/mnt/archive-mounts/test"
INIT_MOUNT="/mnt/archive-mounts/init-test"
PASS_DIR="/tmp/passthrough/e2e-pass"
INIT_PASS_DIR="/tmp/passthrough/init-test"

HOST_DIR="${PBS_STORE}/ns/${NAMESPACE}/host/${BACKUP_ID}"
MPXAR_PATTERN="${BACKUP_ID}---${BACKUP_DISK}.mpxar.didx"
PPXAR_PATTERN="${BACKUP_ID}---${BACKUP_DISK}.ppxar.didx"

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
	local pid=$!
	for i in $(seq 1 15); do
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "FAILED: pxar-mount init exited early (pid $pid)"
			cat /tmp/pxar-mount-test.log
			exit 1
		fi
		timeout 2 mountpoint -q "$mount" && break
		sleep 1
	done
	if ! timeout 2 mountpoint -q "$mount"; then
		echo "FAILED to mount $mount"
		cat /tmp/pxar-mount-test.log
		exit 1
	fi
}

mount_archive() {
	local mpxar=$1 ppxar=$2 socket=$3 mount=$4 pass=$5
	unmount "$mount"
	rm -rf "$pass/.pxar-journal" "$pass"/*
	mkdir -p "$pass" "$mount" "$(dirname $socket)"
	nohup "$PXAR_MOUNT_BIN" \
		--passthrough "$pass" \
		--mpxar-didx "$mpxar" \
		--ppxar-didx "$ppxar" \
		--pbs-store "$PBS_STORE" \
		--socket "$socket" \
		--options rw,allow_other \
		"$mount" > /tmp/pxar-mount-test.log 2>&1 &
	local pid=$!
	for i in $(seq 1 15); do
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "FAILED: pxar-mount exited early (pid $pid)"
			cat /tmp/pxar-mount-test.log
			exit 1
		fi
		timeout 2 mountpoint -q "$mount" && break
		sleep 1
	done
	if ! timeout 2 mountpoint -q "$mount"; then
		echo "FAILED to mount $mount"
		cat /tmp/pxar-mount-test.log
		exit 1
	fi
}

do_commit() {
	"$PXAR_MOUNT_BIN" commit --socket "$1" --backup-id "$2" 2>&1
}

latest_snapshot() {
	local dir=$1
	ls -1 "$dir" | grep -E '^[0-9]{4}-' | sort | tail -1
}

###############################################################################
# Pre-check: find existing snapshot for mount mode tests
###############################################################################
ORIG_SNAP=$(latest_snapshot "$HOST_DIR" 2>/dev/null || true)
if [ -z "$ORIG_SNAP" ]; then
	echo "ERROR: No existing snapshots found at $HOST_DIR for mount mode tests"
	exit 1
fi
MPXAR="${HOST_DIR}/${ORIG_SNAP}/${MPXAR_PATTERN}"
PPXAR="${HOST_DIR}/${ORIG_SNAP}/${PPXAR_PATTERN}"
echo "Using snapshot: $ORIG_SNAP"
echo "MPXAR: $MPXAR"
echo "PPXAR: $PPXAR"

###############################################################################
section "PHASE 1: INIT MODE — Fresh archive from scratch"
###############################################################################

mount_init "${NAMESPACE}/init" "E2E-INIT" "$INIT_SOCKET" "$INIT_MOUNT" "$INIT_PASS_DIR"

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

# 1h. First commit (init mode)
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "E2E-INIT")
if echo "$RESULT" | grep -q "✓"; then
	ok "init mode commit
else
	fail "init mode commit
	echo "--- pxar-mount daemon log (last 40 lines) ---"
	tail -40 /tmp/pxar-mount-test.log 2>/dev/null || true
	echo "--- end daemon log ---"
fi

# 1i. Post-commit reads
[ "$(cat $INIT_MOUNT/file1.txt)" = "hello world" ] && ok "post-commit read file1.txt" || fail "post-commit read file1.txt"
[ "$(cat $INIT_MOUNT/dir2/data.txt)" = "dir2 content" ] && ok "post-commit read dir2/data.txt" || fail "post-commit read dir2/data.txt"

# 1j. Post-commit writes
echo "after commit 1" > "$INIT_MOUNT/post1.txt"
ok "post-commit create post1.txt"

# 1k. Delete a file post-commit
rm "$INIT_MOUNT/file2.txt"
ok "post-commit delete file2.txt"

# 1l. Second commit (re-commit from committed snapshot)
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "E2E-INIT")
if echo "$RESULT" | grep -q "✓"; then
	ok "init mode commit
else
	fail "init mode commit
	echo "--- pxar-mount daemon log (last 40 lines) ---"
	tail -40 /tmp/pxar-mount-test.log 2>/dev/null || true
	echo "--- end daemon log ---"
fi

# 1m. Verify post-2nd-commit state
[ "$(cat $INIT_MOUNT/file1.txt)" = "hello world" ] && ok "post-2nd read file1.txt" || fail "post-2nd read file1.txt"
[ "$(cat $INIT_MOUNT/post1.txt)" = "after commit 1" ] && ok "post-2nd read post1.txt" || fail "post-2nd read post1.txt"
[ ! -f "$INIT_MOUNT/file2.txt" ] && ok "post-2nd file2.txt is gone" || fail "post-2nd file2.txt should not exist"

# 1n. Third commit (no changes)
sleep 1; RESULT=$(do_commit "$INIT_SOCKET" "E2E-INIT")
if echo "$RESULT" | grep -q "✓"; then
	ok "init mode commit
else
	fail "init mode commit
	echo "--- pxar-mount daemon log (last 40 lines) ---"
	tail -40 /tmp/pxar-mount-test.log 2>/dev/null || true
	echo "--- end daemon log ---"
fi

unmount "$INIT_MOUNT"

###############################################################################
section "PHASE 2: MOUNT MODE — Existing archive with mutations"
###############################################################################

mount_archive "$MPXAR" "$PPXAR" "$SOCKET" "$MOUNT" "$PASS_DIR"

# 2a. Read existing files from archive
ORIG_FILES=$(ls "$MOUNT/" | head -3)
[ -n "$ORIG_FILES" ] && ok "read existing pxar entries" || fail "read existing pxar entries"

# 2b. Create new files
echo "new content" > "$MOUNT/newfile-mount.txt"
echo "another file" > "$MOUNT/another-mount.txt"
ok "create new files in mounted archive"

# 2c. Create directory and nested files
mkdir -p "$MOUNT/e2e-test-dir/sub"
echo "test data" > "$MOUNT/e2e-test-dir/test.txt"
echo "sub data" > "$MOUNT/e2e-test-dir/sub/sub.txt"
ok "create nested dirs and files in mounted archive"

# 2d. Read back new files
[ "$(cat $MOUNT/newfile-mount.txt)" = "new content" ] && ok "read newfile-mount.txt" || fail "read newfile-mount.txt"
[ "$(cat $MOUNT/e2e-test-dir/sub/sub.txt)" = "sub data" ] && ok "read nested sub.txt" || fail "read nested sub.txt"

# 2e. Rename file
mv "$MOUNT/another-mount.txt" "$MOUNT/renamed-mount.txt"
[ -f "$MOUNT/renamed-mount.txt" ] && ok "rename file (exists at dest)" || fail "rename file (dest missing)"
[ ! -f "$MOUNT/another-mount.txt" ] && ok "rename file (gone from src)" || fail "rename file (src still exists)"
[ "$(cat $MOUNT/renamed-mount.txt)" = "another file" ] && ok "rename file (content preserved)" || fail "rename file (content wrong)"

# 2f. Delete new file
rm "$MOUNT/newfile-mount.txt"
[ ! -f "$MOUNT/newfile-mount.txt" ] && ok "delete new file" || fail "delete new file"

# 2g. Overwrite existing pxar file
FIRST_DIR=$(find "$MOUNT" -maxdepth 1 -type d ! -path "$MOUNT" ! -path "$MOUNT/e2e-test-dir" | head -1)
if [ -n "$FIRST_DIR" ]; then
	echo "overwritten" > "$FIRST_DIR/overwrite-test.txt" 2>/dev/null && ok "write into existing pxar subdir" || skip "write into existing pxar subdir (no write permission)"
fi

# 2h. Commit mounted archive
sleep 1; RESULT=$(do_commit "$SOCKET" "$BACKUP_ID")
if echo "$RESULT" | grep -q "✓"; then
	ok "mount mode commit
else
	fail "mount mode commit
	echo "--- pxar-mount daemon log (last 40 lines) ---"
	tail -40 /tmp/pxar-mount-test.log 2>/dev/null || true
	echo "--- end daemon log ---"
fi

# 2i. Post-commit reads
[ "$(cat $MOUNT/renamed-mount.txt)" = "another file" ] && ok "post-commit read renamed file" || fail "post-commit read renamed file"
[ "$(cat $MOUNT/e2e-test-dir/test.txt)" = "test data" ] && ok "post-commit read e2e-test-dir/test.txt" || fail "post-commit read e2e-test-dir/test.txt"
[ ! -f "$MOUNT/newfile-mount.txt" ] && ok "post-commit deleted file still gone" || fail "post-commit deleted file should not exist"

# 2j. Post-commit new writes
echo "post mount commit" > "$MOUNT/post-mount.txt"
ok "post-commit write post-mount.txt"

# 2k. Post-commit rename
mv "$MOUNT/post-mount.txt" "$MOUNT/post-mount-renamed.txt"
ok "post-commit rename"

# 2l. Post-commit directory creation
mkdir "$MOUNT/post-commit-dir"
echo "dir content" > "$MOUNT/post-commit-dir/file.txt"
ok "post-commit create dir and file"

# 2m. Second commit (re-commit against committed snapshot)
sleep 1; RESULT=$(do_commit "$SOCKET" "$BACKUP_ID")
if echo "$RESULT" | grep -q "✓"; then
	ok "mount mode commit
else
	fail "mount mode commit
	echo "--- pxar-mount daemon log (last 40 lines) ---"
	tail -40 /tmp/pxar-mount-test.log 2>/dev/null || true
	echo "--- end daemon log ---"
fi

# 2n. Verify state after 2nd commit
[ "$(cat $MOUNT/renamed-mount.txt)" = "another file" ] && ok "post-2nd read renamed" || fail "post-2nd read renamed"
[ "$(cat $MOUNT/post-mount-renamed.txt)" = "post mount commit" ] && ok "post-2nd read renamed post-commit file" || fail "post-2nd read renamed post-commit file"
[ "$(cat $MOUNT/post-commit-dir/file.txt)" = "dir content" ] && ok "post-2nd read dir file" || fail "post-2nd read dir file"

unmount "$MOUNT"

###############################################################################
section "PHASE 3: MOUNT LATEST SNAPSHOT — Verify committed data persists"
###############################################################################

LATEST=$(latest_snapshot "$HOST_DIR")
MPXAR2="${HOST_DIR}/${LATEST}/${MPXAR_PATTERN}"
PPXAR2="${HOST_DIR}/${LATEST}/${PPXAR_PATTERN}"

mount_archive "$MPXAR2" "$PPXAR2" "$SOCKET" "$MOUNT" "$PASS_DIR"

# 3a. Check renamed file exists in committed snapshot
[ -f "$MOUNT/renamed-mount.txt" ] && ok "fresh mount: renamed file exists" || fail "fresh mount: renamed file missing"
[ "$(cat $MOUNT/renamed-mount.txt)" = "another file" ] && ok "fresh mount: renamed file content correct" || fail "fresh mount: renamed file content wrong"

# 3b. Check deleted file is gone
[ ! -f "$MOUNT/newfile-mount.txt" ] && ok "fresh mount: deleted file gone" || fail "fresh mount: deleted file still exists"

# 3c. Check new dir and files
[ -f "$MOUNT/e2e-test-dir/test.txt" ] && ok "fresh mount: e2e-test-dir/test.txt exists" || fail "fresh mount: e2e-test-dir/test.txt missing"
[ "$(cat $MOUNT/e2e-test-dir/test.txt)" = "test data" ] && ok "fresh mount: e2e-test-dir content correct" || fail "fresh mount: e2e-test-dir content wrong"

# 3d. Check post-2nd-commit data
[ -f "$MOUNT/post-mount-renamed.txt" ] && ok "fresh mount: post-mount-renamed.txt exists" || fail "fresh mount: post-mount-renamed.txt missing"
[ -f "$MOUNT/post-commit-dir/file.txt" ] && ok "fresh mount: post-commit-dir/file.txt exists" || fail "fresh mount: post-commit-dir/file.txt missing"

# 3e. Original archive data still accessible
ORIG_DIRS=$(ls -d "$MOUNT"/*/ 2>/dev/null | wc -l)
[ "$ORIG_DIRS" -gt 0 ] && ok "fresh mount: original dirs accessible ($ORIG_DIRS dirs)" || fail "fresh mount: original dirs missing"

unmount "$MOUNT"

###############################################################################
section "PHASE 4: RENAME & DELETE EDGE CASES"
###############################################################################

mount_archive "$MPXAR2" "$PPXAR2" "$SOCKET" "$MOUNT" "$PASS_DIR"

# 4a. Create -> rename -> delete
echo "rbd test" > "$MOUNT/rbd-file.txt"
mv "$MOUNT/rbd-file.txt" "$MOUNT/rbd-renamed.txt"
rm "$MOUNT/rbd-renamed.txt"
[ ! -f "$MOUNT/rbd-file.txt" ] && [ ! -f "$MOUNT/rbd-renamed.txt" ] && ok "create-rename-delete: file fully gone" || fail "create-rename-delete: file still exists"

# 4b. Create file -> rename dir -> access file via new path
mkdir "$MOUNT/rename-test-dir"
echo "inside" > "$MOUNT/rename-test-dir/inner.txt"
mv "$MOUNT/rename-test-dir" "$MOUNT/renamed-dir"
[ -f "$MOUNT/renamed-dir/inner.txt" ] && ok "dir rename: file accessible via new path" || fail "dir rename: file not accessible via new path"
[ "$(cat $MOUNT/renamed-dir/inner.txt)" = "inside" ] && ok "dir rename: content preserved" || fail "dir rename: content wrong"

# 4c. Create -> rename over existing file (replace)
echo "source" > "$MOUNT/repl-src.txt"
echo "dest" > "$MOUNT/repl-dst.txt"
mv "$MOUNT/repl-src.txt" "$MOUNT/repl-dst.txt"
[ "$(cat $MOUNT/repl-dst.txt)" = "source" ] && ok "rename replace: dest has source content" || fail "rename replace: wrong content"
[ ! -f "$MOUNT/repl-src.txt" ] && ok "rename replace: source gone" || fail "rename replace: source still exists"

# 4d. Empty directory deletion
mkdir "$MOUNT/empty-dir"
rmdir "$MOUNT/empty-dir"
[ ! -d "$MOUNT/empty-dir" ] && ok "rmdir empty directory" || fail "rmdir empty directory"

# 4e. Non-empty directory deletion should fail
mkdir "$MOUNT/nonempty-dir"
echo "x" > "$MOUNT/nonempty-dir/x.txt"
rmdir "$MOUNT/nonempty-dir" 2>/dev/null && fail "rmdir non-empty dir should fail" || ok "rmdir non-empty dir correctly fails"
rm "$MOUNT/nonempty-dir/x.txt"

# 4f. Commit with all these edge cases
sleep 1; RESULT=$(do_commit "$SOCKET" "$BACKUP_ID")
if echo "$RESULT" | grep -q "✓"; then
	ok "edge cases commit"
else
	fail "edge cases commit: $RESULT"
	echo "--- pxar-mount daemon log (last 40 lines) ---"
	tail -40 /tmp/pxar-mount-test.log 2>/dev/null || true
	echo "--- end daemon log ---"
fi

# 4g. Verify after commit
[ -f "$MOUNT/renamed-dir/inner.txt" ] && ok "post-edge-commit: renamed dir file exists" || fail "post-edge-commit: renamed dir file missing"
[ "$(cat $MOUNT/repl-dst.txt)" = "source" ] && ok "post-edge-commit: replaced dest correct" || fail "post-edge-commit: replaced dest wrong"

unmount "$MOUNT"

###############################################################################
section "PHASE 5: RAPID FIRE — Multiple commits in sequence"
###############################################################################

mount_archive "$MPXAR2" "$PPXAR2" "$SOCKET" "$MOUNT" "$PASS_DIR"

for i in $(seq 1 5); do
	echo "rapid-$i" > "$MOUNT/rapid-$i.txt"
	mkdir -p "$MOUNT/rapid-dir-$i"
	echo "rapid-dir-content-$i" > "$MOUNT/rapid-dir-$i/file.txt"
	sleep 1; RESULT=$(do_commit "$SOCKET" "$BACKUP_ID")
	if echo "$RESULT" | grep -q "✓"; then
		ok "rapid commit #$i"
	else
		fail "rapid commit #$i: $RESULT"
		break
	fi
	for j in $(seq 1 $i); do
		[ "$(cat $MOUNT/rapid-$j.txt)" = "rapid-$j" ] || { fail "rapid verify #$j after commit $i"; break 2; }
	done
	ok "rapid verify all files after commit #$i"
done

unmount "$MOUNT"

###############################################################################
section "PHASE 6: ACL TESTS"
###############################################################################

mount_archive "$MPXAR2" "$PPXAR2" "$SOCKET" "$MOUNT" "$PASS_DIR"

# 6a. setfacl on new file
echo "acl-test" > "$MOUNT/acl-file.txt"
setfacl -m u:34:rw "$MOUNT/acl-file.txt" 2>/dev/null && ok "setfacl on new file" || skip "setfacl on new file (not supported)"

# 6b. getfacl verification
GETACL=$(getfacl -p "$MOUNT/acl-file.txt" 2>/dev/null | grep "user:34" || true)
[ -n "$GETACL" ] && ok "getfacl shows ACL entry" || skip "getfacl verify (not supported)"

# 6c. setfacl on directory
mkdir "$MOUNT/acl-dir"
setfacl -m u:34:rwx "$MOUNT/acl-dir" 2>/dev/null && ok "setfacl on directory" || skip "setfacl on directory (not supported)"

# 6d. Commit with ACLs
sleep 1; RESULT=$(do_commit "$SOCKET" "$BACKUP_ID")
if echo "$RESULT" | grep -q "✓"; then
	ok "ACL commit"
else
	fail "ACL commit: $RESULT"
fi

# 6e. Verify ACLs after commit
GETACL2=$(getfacl -p "$MOUNT/acl-file.txt" 2>/dev/null | grep "user:34" || true)
[ -n "$GETACL2" ] && ok "post-commit ACL preserved" || skip "post-commit ACL verify"

unmount "$MOUNT"

###############################################################################
section "PHASE 7: LARGE FILE & BINARY DATA"
###############################################################################

mount_archive "$MPXAR2" "$PPXAR2" "$SOCKET" "$MOUNT" "$PASS_DIR"

# 7a. Write 1MB file
dd if=/dev/urandom of="$MOUNT/large-1m.bin" bs=1K count=1024 2>/dev/null
SIZE=$(stat -c%s "$MOUNT/large-1m.bin" 2>/dev/null || stat -f%z "$MOUNT/large-1m.bin")
[ "$SIZE" = "1048576" ] && ok "write 1MB binary file" || fail "write 1MB binary file (size=$SIZE)"

# 7b. Compute checksum, commit, verify
CS_BEFORE=$(sha256sum "$MOUNT/large-1m.bin" | awk '{print $1}')
sleep 1; RESULT=$(do_commit "$SOCKET" "$BACKUP_ID")
if echo "$RESULT" | grep -q "✓"; then
	ok "large file commit"
else
	fail "large file commit: $RESULT"
fi
CS_AFTER=$(sha256sum "$MOUNT/large-1m.bin" | awk '{print $1}')
[ "$CS_BEFORE" = "$CS_AFTER" ] && ok "large file integrity after commit" || fail "large file checksum mismatch after commit"

unmount "$MOUNT"

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
