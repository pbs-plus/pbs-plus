//go:build linux

package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"golang.org/x/sys/unix"
)

var objectstoreLocksDir = "/run/proxmox-backup/locks"
var objectstoreActiveOperationsDir = "/run/proxmox-backup/active-operations"

type publication struct {
	snapshotDir   string
	groupDir      string
	backupTime    int64
	createdGroup  bool
	committed     bool
	locks         []io.Closer
	releaseChunk  func() error
	releaseActive func() error
}

func beginPublication(ctx context.Context, storeName, storeRoot string, bucket Bucket, owner string, now time.Time) (_ *publication, err error) {
	if strings.ContainsAny(owner, "\r\n") || owner == "" {
		return nil, errors.New("s3 credential has no valid PBS auth ID")
	}
	p := &publication{}
	defer func() {
		if err != nil {
			_ = p.Close()
		}
	}()
	p.releaseActive, err = beginObjectstoreActiveWrite(storeName)
	if err != nil {
		return nil, fmt.Errorf("register active datastore write: %w", err)
	}
	p.groupDir = filepath.Join(proxmox.NamespacePath(storeRoot, bucket.Namespace), bucket.BackupType, bucket.BackupID)
	if err := proxmox.EnsureGroupPath(storeRoot, bucket.Namespace, bucket.BackupType, ""); err != nil {
		return nil, fmt.Errorf("ensure backup group parent: %w", err)
	}
	if err := os.Mkdir(p.groupDir, 0o755); err == nil {
		p.createdGroup = true
		if err := proxmox.ChownBackupUser(p.groupDir); err != nil {
			return nil, fmt.Errorf("chown backup group: %w", err)
		}
	} else if !os.IsExist(err) {
		return nil, fmt.Errorf("create backup group: %w", err)
	}
	groupLock, err := acquireObjectstoreBackupLock(storeName, bucket.Namespace, filepath.Join(bucket.BackupType, bucket.BackupID), p.groupDir, false, true)
	if err != nil {
		return nil, fmt.Errorf("lock backup group: %w", err)
	}
	p.locks = append(p.locks, groupLock)
	if err := ensureObjectstoreGroupOwner(p.groupDir, owner, p.createdGroup); err != nil {
		return nil, err
	}
	p.backupTime = uniqueObjectstoreSnapshotTime(p.groupDir, now)
	p.snapshotDir = filepath.Join(p.groupDir, time.Unix(p.backupTime, 0).UTC().Format(time.RFC3339))
	if err := os.Mkdir(p.snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create backup snapshot: %w", err)
	}
	if err := proxmox.ChownBackupUser(p.snapshotDir); err != nil {
		return nil, fmt.Errorf("chown backup snapshot: %w", err)
	}
	snapshotLock, err := acquireObjectstoreBackupLock(
		storeName,
		bucket.Namespace,
		filepath.Join(bucket.BackupType, bucket.BackupID, filepath.Base(p.snapshotDir)),
		p.snapshotDir,
		false,
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("lock backup snapshot: %w", err)
	}
	p.locks = append(p.locks, snapshotLock)
	p.releaseChunk, err = acquireObjectstoreChunkLock(ctx, storeRoot)
	if err != nil {
		return nil, fmt.Errorf("lock datastore against garbage collection: %w", err)
	}
	return p, nil
}

func (p *publication) Commit() {
	p.committed = true
}

func (p *publication) Close() error {
	var closeErr error
	if !p.committed && p.snapshotDir != "" {
		if err := os.RemoveAll(p.snapshotDir); err != nil && !os.IsNotExist(err) {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if p.releaseChunk != nil {
		closeErr = errors.Join(closeErr, p.releaseChunk())
		p.releaseChunk = nil
	}
	if !p.committed && p.createdGroup && p.groupDir != "" {
		_ = os.Remove(filepath.Join(p.groupDir, "owner"))
		_ = os.Remove(p.groupDir)
	}
	for _, lock := range slices.Backward(p.locks) {
		closeErr = errors.Join(closeErr, lock.Close())
	}
	p.locks = nil
	if p.releaseActive != nil {
		closeErr = errors.Join(closeErr, p.releaseActive())
		p.releaseActive = nil
	}
	return closeErr
}

func uniqueObjectstoreSnapshotTime(groupDir string, now time.Time) int64 {
	backupTime := now.UTC().Unix()
	for {
		name := time.Unix(backupTime, 0).UTC().Format(time.RFC3339)
		if _, err := os.Stat(filepath.Join(groupDir, name)); os.IsNotExist(err) {
			return backupTime
		}
		backupTime++
	}
}

func ensureObjectstoreGroupOwner(groupDir, owner string, created bool) error {
	ownerPath := filepath.Join(groupDir, "owner")
	if !created {
		data, err := os.ReadFile(ownerPath)
		if err != nil {
			return fmt.Errorf("read backup group owner: %w", err)
		}
		if existing := strings.TrimSpace(string(data)); existing != owner {
			return fmt.Errorf("backup owner check failed (%s != %s)", owner, existing)
		}
		return nil
	}
	file, err := os.OpenFile(ownerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create backup group owner: %w", err)
	}
	if err := proxmox.ChownBackupUser(ownerPath); err != nil {
		_ = file.Close()
		return fmt.Errorf("chown backup group owner: %w", err)
	}
	if _, err := fmt.Fprintln(file, owner); err != nil {
		_ = file.Close()
		return fmt.Errorf("write backup group owner: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close backup group owner: %w", err)
	}
	return nil
}

type objectstoreFileLock struct {
	file *os.File
}

func (l *objectstoreFileLock) Close() error {
	if l.file == nil {
		return nil
	}
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	err = errors.Join(err, l.file.Close())
	l.file = nil
	return err
}

func acquireObjectstoreBackupLock(storeName, namespace, relativePath, legacyPath string, shared, directory bool) (*objectstoreFileLock, error) {
	if _, err := os.Stat("/run/proxmox-backup/old-locking"); err == nil {
		return flockObjectstorePath(legacyPath, shared, true)
	}
	path := objectstoreBackupLockPath(storeName, namespace, relativePath)
	if err := ensureObjectstoreLockDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return flockObjectstorePath(path, shared, directory)
}

func ensureObjectstoreLockDir(path string) error {
	rel, err := filepath.Rel(objectstoreLocksDir, path)
	if err != nil {
		return fmt.Errorf("resolve lock directory %q: %w", path, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("lock directory %q is outside %q", path, objectstoreLocksDir)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	for dir := path; ; dir = filepath.Dir(dir) {
		if err := proxmox.ChownBackupUser(dir); err != nil {
			return fmt.Errorf("chown lock directory %q: %w", dir, err)
		}
		if dir == objectstoreLocksDir {
			return nil
		}
	}
}

func flockObjectstorePath(path string, shared, directory bool) (*objectstoreFileLock, error) {
	flags := os.O_RDWR | os.O_CREATE
	if directory {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			flags = os.O_RDONLY
		}
	}
	file, err := os.OpenFile(path, flags, 0o660)
	if err != nil {
		return nil, err
	}
	if flags&os.O_CREATE != 0 {
		_ = file.Chmod(0o660)
		if err := proxmox.ChownBackupUser(path); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	operation := unix.LOCK_EX | unix.LOCK_NB
	if shared {
		operation = unix.LOCK_SH | unix.LOCK_NB
	}
	if err := unix.Flock(int(file.Fd()), operation); err != nil {
		_ = file.Close()
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	pathInfo, err := os.Stat(path)
	if err != nil || !os.SameFile(fileInfo, pathInfo) {
		_ = file.Close()
		return nil, errors.New("lock file changed while acquiring it")
	}
	return &objectstoreFileLock{file: file}, nil
}

func objectstoreBackupLockPath(storeName, namespace, relativePath string) string {
	dir := filepath.Join(objectstoreLocksDir, storeName)
	if namespace != "" {
		dir = filepath.Join(dir, strings.ReplaceAll(namespace, "/", ":"))
	}
	escaped := escapeObjectstoreSystemdPath(relativePath)
	if len(escaped) < 255 {
		return filepath.Join(dir, escaped)
	}
	sum := sha256.Sum256([]byte(relativePath))
	return filepath.Join(dir, "hashed", escaped[:80]+"..."+escaped[len(escaped)-80:]+"-"+hex.EncodeToString(sum[:]))
}

func escapeObjectstoreSystemdPath(path string) string {
	var builder strings.Builder
	for _, char := range []byte(path) {
		switch {
		case char == '/':
			builder.WriteByte('-')
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9', char == '_', char == '.':
			builder.WriteByte(char)
		default:
			fmt.Fprintf(&builder, `\x%02x`, char)
		}
	}
	return builder.String()
}

var objectstoreChunkLocks = struct {
	sync.Mutex
	refs map[string]*objectstoreChunkLock
}{refs: make(map[string]*objectstoreChunkLock)}

type objectstoreChunkLock struct {
	file *os.File
	refs int
}

func acquireObjectstoreChunkLock(_ context.Context, storeRoot string) (func() error, error) {
	root, err := filepath.EvalSymlinks(storeRoot)
	if err != nil {
		return nil, err
	}
	objectstoreChunkLocks.Lock()
	defer objectstoreChunkLocks.Unlock()
	if held := objectstoreChunkLocks.refs[root]; held != nil {
		held.refs++
		return func() error { return releaseObjectstoreChunkLock(root) }, nil
	}
	file, err := os.OpenFile(filepath.Join(root, ".lock"), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	lock := unix.Flock_t{Type: unix.F_RDLCK, Whence: io.SeekStart}
	if err := unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock); err != nil {
		_ = file.Close()
		return nil, err
	}
	objectstoreChunkLocks.refs[root] = &objectstoreChunkLock{file: file, refs: 1}
	return func() error { return releaseObjectstoreChunkLock(root) }, nil
}

func releaseObjectstoreChunkLock(root string) error {
	objectstoreChunkLocks.Lock()
	defer objectstoreChunkLocks.Unlock()
	held := objectstoreChunkLocks.refs[root]
	if held == nil {
		return nil
	}
	held.refs--
	if held.refs > 0 {
		return nil
	}
	delete(objectstoreChunkLocks.refs, root)
	lock := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart}
	err := unix.FcntlFlock(held.file.Fd(), unix.F_SETLKW, &lock)
	return errors.Join(err, held.file.Close())
}

type objectstoreActiveOperationStats struct {
	Read  int64 `json:"read"`
	Write int64 `json:"write"`
}

type objectstoreTaskOperations struct {
	PID              uint32                          `json:"pid"`
	StartTime        uint64                          `json:"starttime"`
	ActiveOperations objectstoreActiveOperationStats `json:"active_operations"`
}

func beginObjectstoreActiveWrite(storeName string) (func() error, error) {
	if err := updateObjectstoreActiveWrites(storeName, 1); err != nil {
		return nil, err
	}
	var once sync.Once
	var releaseErr error
	return func() error {
		once.Do(func() { releaseErr = updateObjectstoreActiveWrites(storeName, -1) })
		return releaseErr
	}, nil
}

func updateObjectstoreActiveWrites(storeName string, delta int64) error {
	if err := os.MkdirAll(objectstoreActiveOperationsDir, 0o755); err != nil {
		return err
	}
	lock, err := flockObjectstorePath(filepath.Join(objectstoreActiveOperationsDir, storeName+".lock"), false, false)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	path := filepath.Join(objectstoreActiveOperationsDir, storeName)
	var entries []objectstoreTaskOperations
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &entries); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	pid := uint32(os.Getpid())
	startTime, err := objectstoreProcessStartTime(pid)
	if err != nil {
		return err
	}
	found := false
	alive := entries[:0]
	for _, entry := range entries {
		actual, err := objectstoreProcessStartTime(entry.PID)
		if err != nil || actual != entry.StartTime {
			continue
		}
		if entry.PID == pid {
			entry.ActiveOperations.Write += delta
			if entry.ActiveOperations.Write < 0 {
				return errors.New("active datastore write count underflow")
			}
			found = true
		}
		alive = append(alive, entry)
	}
	if !found {
		if delta < 0 {
			return errors.New("active datastore write entry is missing")
		}
		alive = append(alive, objectstoreTaskOperations{
			PID:              pid,
			StartTime:        startTime,
			ActiveOperations: objectstoreActiveOperationStats{Write: delta},
		})
	}
	data, err := json.Marshal(alive)
	if err != nil {
		return err
	}
	return replaceObjectstoreOwnedFile(path, data, 0o660)
}

func objectstoreProcessStartTime(pid uint32) (uint64, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.FormatUint(uint64(pid), 10), "stat"))
	if err != nil {
		return 0, err
	}
	closeParen := strings.LastIndexByte(string(data), ')')
	if closeParen < 0 {
		return 0, errors.New("invalid process stat")
	}
	fields := strings.Fields(string(data[closeParen+1:]))
	if len(fields) <= 19 {
		return 0, errors.New("process stat has no start time")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

func replaceObjectstoreOwnedFile(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s3-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chown(proxmox.BackupUID, proxmox.BackupGID); err != nil && os.Geteuid() == 0 {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
