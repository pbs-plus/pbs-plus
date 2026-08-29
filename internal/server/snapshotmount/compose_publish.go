//go:build linux

package snapshotmount

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

	"github.com/pbs-plus/pxar/datastore"
	"golang.org/x/sys/unix"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

var datastoreLocksDir = "/run/proxmox-backup/locks"
var activeOperationsDir = "/run/proxmox-backup/active-operations"

type composePublication struct {
	snapshotDir   string
	groupDir      string
	createdGroup  bool
	committed     bool
	locks         []io.Closer
	releaseChunk  func() error
	releaseActive func() error
}

func beginComposePublication(
	ctx context.Context,
	storeName, storeRoot, sourceNS, sourceType, sourceID string,
	sourceTime time.Time,
	targetNS, targetType, targetID, owner string,
) (_ *composePublication, err error) {
	if strings.ContainsAny(owner, "\r\n") || owner == "" {
		return nil, errors.New("local PBS token has no valid auth ID")
	}
	p := &composePublication{}
	defer func() {
		if err != nil {
			_ = p.Close()
		}
	}()

	p.releaseActive, err = beginActiveWrite(storeName)
	if err != nil {
		return nil, fmt.Errorf("register active datastore write: %w", err)
	}

	targetParent := groupParentDir(storeRoot, targetNS, targetType, targetID)
	if err := proxmox.EnsureGroupPath(storeRoot, targetNS, targetType, ""); err != nil {
		return nil, fmt.Errorf("ensure target group parent: %w", err)
	}
	p.groupDir = targetParent
	if err := os.Mkdir(targetParent, 0o755); err == nil {
		p.createdGroup = true
		if err := proxmox.ChownBackupUser(targetParent); err != nil {
			return nil, fmt.Errorf("chown target group: %w", err)
		}
	} else if !os.IsExist(err) {
		return nil, fmt.Errorf("create target group: %w", err)
	}

	groupLock, err := acquireBackupLock(storeName, targetNS, filepath.Join(targetType, targetID), targetParent, false, true)
	if err != nil {
		return nil, fmt.Errorf("lock target group: %w", err)
	}
	p.locks = append(p.locks, groupLock)
	if err := ensureGroupOwner(targetParent, owner, p.createdGroup); err != nil {
		return nil, err
	}

	sourceDir := filepath.Join(groupParentDir(storeRoot, sourceNS, sourceType, sourceID), DirTime(sourceTime.UTC()))
	sourceLock, err := acquireBackupLock(
		storeName,
		sourceNS,
		filepath.Join(sourceType, sourceID, DirTime(sourceTime.UTC())),
		sourceDir,
		true,
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("lock source snapshot: %w", err)
	}
	p.locks = append(p.locks, sourceLock)

	backupTime := uniqueSnapshotTime(targetParent)
	p.snapshotDir = filepath.Join(targetParent, DirTime(time.Unix(backupTime, 0).UTC()))
	if err := os.Mkdir(p.snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create target snapshot: %w", err)
	}
	if err := proxmox.ChownBackupUser(p.snapshotDir); err != nil {
		return nil, fmt.Errorf("chown target snapshot: %w", err)
	}
	snapshotLock, err := acquireBackupLock(
		storeName,
		targetNS,
		filepath.Join(targetType, targetID, filepath.Base(p.snapshotDir)),
		p.snapshotDir,
		false,
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("lock target snapshot: %w", err)
	}
	p.locks = append(p.locks, snapshotLock)

	p.releaseChunk, err = acquireChunkStoreLock(ctx, storeRoot)
	if err != nil {
		return nil, fmt.Errorf("lock datastore against garbage collection: %w", err)
	}
	return p, nil
}

func (p *composePublication) BackupTime() int64 {
	t, _ := time.Parse(time.RFC3339, filepath.Base(p.snapshotDir))
	return t.Unix()
}

func (p *composePublication) Commit() {
	p.committed = true
}

func (p *composePublication) Close() error {
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
	for _, v := range slices.Backward(p.locks) {
		closeErr = errors.Join(closeErr, v.Close())
	}
	p.locks = nil
	if p.releaseActive != nil {
		closeErr = errors.Join(closeErr, p.releaseActive())
		p.releaseActive = nil
	}
	return closeErr
}

func ensureGroupOwner(groupDir, owner string, created bool) error {
	ownerPath := filepath.Join(groupDir, "owner")
	if created {
		file, err := os.OpenFile(ownerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return fmt.Errorf("create target group owner: %w", err)
		}
		if err := proxmox.ChownBackupUser(ownerPath); err != nil {
			_ = file.Close()
			return fmt.Errorf("chown target group owner: %w", err)
		}
		if _, err := fmt.Fprintln(file, owner); err != nil {
			_ = file.Close()
			return fmt.Errorf("write target group owner: %w", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close target group owner: %w", err)
		}
		return nil
	}
	data, err := os.ReadFile(ownerPath)
	if err != nil {
		return fmt.Errorf("read target group owner: %w", err)
	}
	existing := strings.TrimSpace(string(data))
	if existing != owner {
		return fmt.Errorf("backup owner check failed (%s != %s)", owner, existing)
	}
	return nil
}

type heldFileLock struct {
	file *os.File
}

func (l *heldFileLock) Close() error {
	if l.file == nil {
		return nil
	}
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	err = errors.Join(err, l.file.Close())
	l.file = nil
	return err
}

func acquireBackupLock(storeName, namespace, relativePath, legacyPath string, shared, directory bool) (*heldFileLock, error) {
	if _, err := os.Stat("/run/proxmox-backup/old-locking"); err == nil {
		return flockPath(legacyPath, shared, true)
	}
	path := backupLockPath(storeName, namespace, relativePath)
	if err := ensureBackupLockDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return flockPath(path, shared, directory)
}

func ensureBackupLockDir(path string) error {
	rel, err := filepath.Rel(datastoreLocksDir, path)
	if err != nil {
		return fmt.Errorf("resolve lock directory %q: %w", path, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("lock directory %q is outside %q", path, datastoreLocksDir)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	for dir := path; ; dir = filepath.Dir(dir) {
		if err := proxmox.ChownBackupUser(dir); err != nil {
			return fmt.Errorf("chown lock directory %q: %w", dir, err)
		}
		if dir == datastoreLocksDir {
			return nil
		}
	}
}

func flockPath(path string, shared, directory bool) (*heldFileLock, error) {
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
	return &heldFileLock{file: file}, nil
}

func backupLockPath(storeName, namespace, relativePath string) string {
	dir := filepath.Join(datastoreLocksDir, storeName)
	if namespace != "" {
		dir = filepath.Join(dir, strings.ReplaceAll(namespace, "/", ":"))
	}
	escaped := escapeSystemdPath(relativePath)
	if len(escaped) < 255 {
		return filepath.Join(dir, escaped)
	}
	sum := sha256.Sum256([]byte(relativePath))
	return filepath.Join(dir, "hashed", escaped[:80]+"..."+escaped[len(escaped)-80:]+"-"+hex.EncodeToString(sum[:]))
}

func escapeSystemdPath(path string) string {
	var b strings.Builder
	for _, c := range []byte(path) {
		switch {
		case c == '/':
			b.WriteByte('-')
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '.':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, `\x%02x`, c)
		}
	}
	return b.String()
}

var chunkStoreLocks = struct {
	sync.Mutex
	refs map[string]*chunkStoreLock
}{refs: make(map[string]*chunkStoreLock)}

type chunkStoreLock struct {
	file *os.File
	refs int
}

func acquireChunkStoreLock(_ context.Context, storeRoot string) (func() error, error) {
	root, err := filepath.EvalSymlinks(storeRoot)
	if err != nil {
		return nil, err
	}
	chunkStoreLocks.Lock()
	defer chunkStoreLocks.Unlock()
	if held := chunkStoreLocks.refs[root]; held != nil {
		held.refs++
		return func() error { return releaseChunkStoreLock(root) }, nil
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
	chunkStoreLocks.refs[root] = &chunkStoreLock{file: file, refs: 1}
	return func() error { return releaseChunkStoreLock(root) }, nil
}

func releaseChunkStoreLock(root string) error {
	chunkStoreLocks.Lock()
	defer chunkStoreLocks.Unlock()
	held := chunkStoreLocks.refs[root]
	if held == nil {
		return nil
	}
	held.refs--
	if held.refs > 0 {
		return nil
	}
	delete(chunkStoreLocks.refs, root)
	lock := unix.Flock_t{Type: unix.F_UNLCK, Whence: io.SeekStart}
	err := unix.FcntlFlock(held.file.Fd(), unix.F_SETLKW, &lock)
	return errors.Join(err, held.file.Close())
}

type activeOperationStats struct {
	Read  int64 `json:"read"`
	Write int64 `json:"write"`
}

type taskOperations struct {
	PID              uint32               `json:"pid"`
	StartTime        uint64               `json:"starttime"`
	ActiveOperations activeOperationStats `json:"active_operations"`
}

func beginActiveWrite(storeName string) (func() error, error) {
	if err := updateActiveWrites(storeName, 1); err != nil {
		return nil, err
	}
	var once sync.Once
	var releaseErr error
	return func() error {
		once.Do(func() { releaseErr = updateActiveWrites(storeName, -1) })
		return releaseErr
	}, nil
}

func updateActiveWrites(storeName string, delta int64) error {
	if err := os.MkdirAll(activeOperationsDir, 0o755); err != nil {
		return err
	}
	lock, err := flockPath(filepath.Join(activeOperationsDir, storeName+".lock"), false, false)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	path := filepath.Join(activeOperationsDir, storeName)
	var entries []taskOperations
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &entries); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	pid := uint32(os.Getpid())
	startTime, err := processStartTime(pid)
	if err != nil {
		return err
	}
	found := false
	alive := entries[:0]
	for _, entry := range entries {
		actual, err := processStartTime(entry.PID)
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
		alive = append(alive, taskOperations{
			PID:              pid,
			StartTime:        startTime,
			ActiveOperations: activeOperationStats{Write: delta},
		})
	}
	data, err := json.Marshal(alive)
	if err != nil {
		return err
	}
	return replaceOwnedFile(path, data, 0o660)
}

func processStartTime(pid uint32) (uint64, error) {
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

func replaceOwnedFile(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".active-*")
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

func verifyComposeSource(sourceDir string, inSourceType, inSourceID string, sourceTime int64, indexPaths ...string) error {
	raw, err := os.ReadFile(filepath.Join(sourceDir, "index.json.blob"))
	if err != nil {
		return fmt.Errorf("read source manifest: %w", err)
	}
	data, err := datastore.DecodeBlob(nil, raw)
	if err != nil {
		return fmt.Errorf("decode source manifest: %w", err)
	}
	manifest, err := datastore.UnmarshalManifest(data)
	if err != nil {
		return err
	}
	if manifest.BackupType != inSourceType || manifest.BackupID != inSourceID || manifest.BackupTime != sourceTime {
		return errors.New("source manifest identity does not match selected snapshot")
	}
	for _, path := range indexPaths {
		idx, err := datastore.OpenDynamicIndex(path)
		if err != nil {
			return err
		}
		csum, size := idx.ComputeCsum()
		headerCSum := idx.IndexCsum()
		closeErr := idx.Close()
		if closeErr != nil {
			return closeErr
		}
		if csum != headerCSum {
			return fmt.Errorf("source index %s header checksum mismatch", filepath.Base(path))
		}
		if err := manifest.VerifyFile(filepath.Base(path), hex.EncodeToString(csum[:]), size); err != nil {
			return err
		}
		for _, file := range manifest.Files {
			if file.Filename == filepath.Base(path) && file.CryptMode != "" && file.CryptMode != string(datastore.CryptModeNone) {
				return fmt.Errorf("source index %s is encrypted", filepath.Base(path))
			}
		}
	}
	return nil
}

func verifyPublishedIndex(path string) error {
	idx, err := datastore.OpenDynamicIndex(path)
	if err != nil {
		return err
	}
	defer func() { _ = idx.Close() }()
	csum, _ := idx.ComputeCsum()
	if csum != idx.IndexCsum() {
		return fmt.Errorf("published index %s checksum mismatch", filepath.Base(path))
	}
	return nil
}
