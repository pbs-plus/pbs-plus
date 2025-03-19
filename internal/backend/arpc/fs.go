//go:build linux

package arpcfs

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/zeebo/xxh3"
	"go.etcd.io/bbolt"
)

// accessMsg represents one file (or directory) access event.
type accessMsg struct {
	hash  uint64
	isDir bool
}

// hashPath uses xxHash to obtain an unsigned 64-bit hash of the given path.
func hashPath(path string) uint64 {
	return xxh3.HashString(path)
}

// NewARPCFS creates an instance of ARPCFS and opens the bbolt DB.
// It also starts a background worker to batch and flush file-access events.
func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string, backupMode string) *ARPCFS {
	// Open the bbolt database (here using a file named "access.db").
	dbFile, err := os.CreateTemp("", "*.wbl")
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create temp wbl").Write()
	}
	defer dbFile.Close()

	db, err := bbolt.Open(dbFile.Name(), 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to open bolt wbl").Write()
		// In a production system you might handle error and abort.
	}
	// Make sure the bucket exists.
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("AccessLog"))
		return err
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create bucket").Write()
	}

	fs := &ARPCFS{
		basePath:      "/",
		ctx:           ctx,
		session:       session,
		JobId:         jobId,
		Hostname:      hostname,
		backupMode:    backupMode,
		db:            db,
		logCh:         make(chan accessMsg, 10000), // buffered for performance
		logWorkerDone: make(chan struct{}),
	}
	// Start background worker for batching access logs.
	go fs.logWorker()
	return fs
}

// trackAccess now uses write-behind logging via bbolt.
// It sends an accessMsg onto a buffered channel so that the foreground
// operation is fast. If the channel is full, a synchronous (fallback) write is used.
func (fs *ARPCFS) trackAccess(path string, isDir bool) {
	hashedPath := hashPath(path)
	msg := accessMsg{hash: hashedPath, isDir: isDir}
	select {
	case fs.logCh <- msg:
		// Fast path: enqueue and return.
	default:
		// Fallback: channel full. Do a synchronous bolt update.
		fs.syncLogAccess(hashedPath, isDir)
	}
}

// GetStats returns a snapshot of all access and byte-read statistics.
func (fs *ARPCFS) GetStats() Stats {
	// Get the current time in nanoseconds.
	currentTime := time.Now().UnixNano()

	// Atomically load the current counters.
	currentFileCount := atomic.LoadInt64(&fs.fileCount)
	currentFolderCount := atomic.LoadInt64(&fs.folderCount)
	totalAccessed := currentFileCount + currentFolderCount

	// Swap out the previous access statistics.
	lastATime := atomic.SwapInt64(&fs.lastAccessTime, currentTime)
	lastFileCount := atomic.SwapInt64(&fs.lastFileCount, currentFileCount)
	lastFolderCount := atomic.SwapInt64(&fs.lastFolderCount, currentFolderCount)

	// Calculate the elapsed time in seconds.
	elapsed := float64(currentTime-lastATime) / 1e9
	var accessSpeed float64
	if elapsed > 0 {
		accessDelta := (currentFileCount + currentFolderCount) - (lastFileCount + lastFolderCount)
		accessSpeed = float64(accessDelta) / elapsed
	}

	// Similarly, for byte counters (if you're tracking totalBytes elsewhere).
	currentTotalBytes := atomic.LoadInt64(&fs.totalBytes)
	lastBTime := atomic.SwapInt64(&fs.lastBytesTime, currentTime)
	lastTotalBytes := atomic.SwapInt64(&fs.lastTotalBytes, currentTotalBytes)

	secDiff := float64(currentTime-lastBTime) / 1e9
	var bytesSpeed float64
	if secDiff > 0 {
		bytesSpeed = float64(currentTotalBytes-lastTotalBytes) / secDiff
	}

	return Stats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      uint64(currentTotalBytes),
		ByteReadSpeed:   bytesSpeed,
	}
}

// syncLogAccess writes a single accessLog entry immediately to bbolt (used as a fallback).
func (fs *ARPCFS) syncLogAccess(hash uint64, isDir bool) {
	err := fs.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("AccessLog"))
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, hash)
		if bucket.Get(key) == nil {
			var b byte = 'f'
			if isDir {
				b = 'd'
			}
			return bucket.Put(key, []byte{b})
		}
		return nil
	})
	if err == nil {
		if isDir {
			atomic.AddInt64(&fs.folderCount, 1)
		} else {
			atomic.AddInt64(&fs.fileCount, 1)
		}
	} else {
		syslog.L.Error(err).WithMessage("syncLogAccess failed").Write()
	}
}

// flushBatch accepts a batch of unique access events and writes them to bbolt in
// a single transaction. It checks for existence of each key to ensure each access
// is only counted once.
func (fs *ARPCFS) flushBatch(batch map[uint64]bool) {
	if len(batch) == 0 {
		return
	}
	err := fs.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("AccessLog"))
		// For each new file (or folder), check and insert.
		for hash, isDir := range batch {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, hash)
			if bucket.Get(key) == nil {
				var b byte = 'f'
				if isDir {
					b = 'd'
				}
				if err := bucket.Put(key, []byte{b}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("flushBatch error").Write()
	} else {
		// On success, update our in-memory counters.
		for _, isDir := range batch {
			if isDir {
				atomic.AddInt64(&fs.folderCount, 1)
			} else {
				atomic.AddInt64(&fs.fileCount, 1)
			}
		}
	}
}

// logWorker is a background goroutine that accumulates access messages
// and writes them in batches to bbolt. It flushes either when a batch size is reached,
// a flush interval elapses, or on shutdown.
func (fs *ARPCFS) logWorker() {
	defer close(fs.logWorkerDone)

	const flushBatchSize = 1024
	// Local batch accumulator.
	batch := make(map[uint64]bool, flushBatchSize)
	flushTicker := time.NewTicker(1 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case msg, ok := <-fs.logCh:
			if !ok {
				// Channel closed. Flush any remaining entries and exit.
				fs.flushBatch(batch)
				return
			}
			// Use the hash as key. If the same file comes in twice within the batch,
			// the later one will simply overwrite the first.
			batch[msg.hash] = msg.isDir
			if len(batch) >= flushBatchSize {
				fs.flushBatch(batch)
				batch = make(map[uint64]bool, flushBatchSize)
			}
		case <-flushTicker.C:
			// Flush if non-empty.
			if len(batch) > 0 {
				fs.flushBatch(batch)
				batch = make(map[uint64]bool, flushBatchSize)
			}
		case <-fs.ctx.Done():
			// Context canceled. Flush remaining entries and exit.
			fs.flushBatch(batch)
			return
		}
	}
}

// Unmount closes the broker channels, waits for pending flushes, and
// then closes the underlying bbolt DB.
func (fs *ARPCFS) Unmount() {
	// Close the log channel to stop the worker and flush pending writes.
	close(fs.logCh)
	<-fs.logWorkerDone
	_ = fs.db.Close()
	_ = os.RemoveAll(fs.db.Path())
}

// The remaining methods (such as GetBackupMode, Open/Stat/Attr, etc.)
// remain largely unchanged, except that calls to trackAccess now log via bbolt.

func (fs *ARPCFS) GetBackupMode() string {
	return fs.backupMode
}

func (fs *ARPCFS) Open(filename string) (ARPCFile, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return ARPCFile{}, syscall.EIO
	}

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/OpenFile", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return ARPCFile{}, err
		}
		return ARPCFile{}, syscall.EIO
	}

	err = resp.Decode(raw)
	if err != nil {
		return ARPCFile{}, syscall.EIO
	}

	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		jobId:    fs.JobId,
	}, nil
}

// Attr retrieves file attributes via RPC and then tracks the access.
func (fs *ARPCFS) Attr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return types.AgentFileInfo{}, syscall.EIO
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/Attr", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return types.AgentFileInfo{}, err
		}
		return types.AgentFileInfo{}, syscall.EIO
	}

	err = fi.Decode(raw)
	if err != nil {
		return types.AgentFileInfo{}, syscall.EIO
	}

	// Log the access using our write-behind mechanism.
	fs.trackAccess(filename, fi.IsDir)

	return fi, nil
}

// Xattr retrieves extended attributes and logs the access similarly.
func (fs *ARPCFS) Xattr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return types.AgentFileInfo{}, syscall.EIO
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/Xattr", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return types.AgentFileInfo{}, err
		}
		return types.AgentFileInfo{}, syscall.EIO
	}

	err = fi.Decode(raw)
	if err != nil {
		return types.AgentFileInfo{}, syscall.EIO
	}

	fs.trackAccess(filename, fi.IsDir)

	return fi, nil
}

// StatFS calls StatFS via RPC.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return types.StatFS{}, syscall.EIO
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute,
		fs.JobId+"/StatFS", nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to handle statfs").Write()
		if arpc.IsOSError(err) {
			return types.StatFS{}, err
		}
		return types.StatFS{}, syscall.EIO
	}

	err = fsStat.Decode(raw)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("failed to handle statfs decode").
			Write()
		return types.StatFS{}, syscall.EIO
	}

	return fsStat, nil
}

// ReadDir calls ReadDir via RPC and logs directory accesses.
func (fs *ARPCFS) ReadDir(path string) (types.ReadDirEntries, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return nil, syscall.EIO
	}

	var resp types.ReadDirEntries
	req := types.ReadDirReq{Path: path}
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/ReadDir", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return nil, err
		}
		return nil, syscall.EIO
	}

	err = resp.Decode(raw)
	if err != nil {
		return nil, syscall.EIO
	}

	// Log the directory access.
	fs.trackAccess(path, true)

	return resp, nil
}

func (fs *ARPCFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ARPCFS) Root() string {
	return fs.basePath
}
