//go:build windows
// +build windows

package nfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
	"golang.org/x/sys/windows/registry"
)

type NFSSession struct {
	Context     context.Context
	ctxCancel   context.CancelFunc
	Snapshot    *snapshots.WinVSSSnapshot
	DriveLetter string
	listener    net.Listener
	connections sync.WaitGroup
	sem         chan struct{}
	isRunning   bool
	mu          sync.Mutex // Protects isRunning
	serverURL   string
}

func getServerURL() (string, error) {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, "Software\\PBSPlus\\Config", registry.QUERY_VALUE)
	if err != nil {
		return "", fmt.Errorf("init SFTP config: %w", err)
	}
	defer baseKey.Close()

	server, _, err := baseKey.GetStringValue("ServerURL")
	if err != nil {
		return "", fmt.Errorf("get server URL: %w", err)
	}

	return server, nil
}

func NewNFSSession(ctx context.Context, snapshot *snapshots.WinVSSSnapshot, driveLetter string) *NFSSession {
	cancellableCtx, cancel := context.WithCancel(ctx)

	url, err := getServerURL()
	if err != nil {
		syslog.L.Errorf("[NewNFSSession] unable to get server url: %v", err)

		cancel()
		return nil
	}

	return &NFSSession{
		Context:     cancellableCtx,
		Snapshot:    snapshot,
		DriveLetter: driveLetter,
		ctxCancel:   cancel,
		isRunning:   true,
		serverURL:   url,
	}
}

func (s *NFSSession) Close() {
	s.mu.Lock()
	s.isRunning = false
	s.mu.Unlock()

	s.ctxCancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.connections.Wait()
	s.Snapshot.Close()
}

type NFSHandler struct {
	mu      sync.Mutex
	handles sync.Map // Map for storing file handles
	session *NFSSession
}

// Verify Handler interface implementation
var _ nfs.Handler = (*NFSHandler)(nil)

// HandleLimit returns the maximum number of handles that can be stored
func (h *NFSHandler) HandleLimit() int {
	return 1000 // Configurable limit
}

// ToHandle converts a filesystem path to an opaque handle
func (h *NFSHandler) ToHandle(fs billy.Filesystem, path []string) []byte {
	fullPath := filepath.Join(path...)
	syslog.L.Infof("[NFS.ToHandle] Converting path to handle: %s", fullPath)

	handle := []byte(fullPath)
	if len(handle) > nfs.FHSize {
		hash := sha256.Sum256(handle)
		handle = hash[:]
		syslog.L.Infof("[NFS.ToHandle] Path too long, using hash")
	}

	h.handles.Store(string(handle), fullPath)
	syslog.L.Infof("[NFS.ToHandle] Stored handle for path: %s", fullPath)
	return handle
}

// FromHandle converts an opaque handle back to a filesystem and path
func (h *NFSHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	value, ok := h.handles.Load(string(fh))
	if !ok {
		syslog.L.Errorf("[NFS.FromHandle] Handle not found: %x", fh)
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	fullPath, ok := value.(string)
	if !ok {
		syslog.L.Errorf("[NFS.FromHandle] Invalid handle value type")
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	syslog.L.Infof("[NFS.FromHandle] Converting handle to path: %s", fullPath)

	// Create filesystem for this path
	fs := &ReadOnlyFS{
		basePath: h.session.Snapshot.SnapshotPath,
		snapshot: h.session.Snapshot,
		ctx:      h.session.Context,
	}

	// If this is the root mount point ("/mount"), use the base path
	if fullPath == "/mount" || fullPath == "mount" {
		syslog.L.Infof("[NFS.FromHandle] Accessing root mount point")
		return fs, []string{}, nil
	}

	// For other paths, calculate relative path
	cleanPath := filepath.Clean(fullPath)
	relPath, err := filepath.Rel(h.session.Snapshot.SnapshotPath, cleanPath)
	if err != nil {
		syslog.L.Errorf("[NFS.FromHandle] Failed to get relative path: %v", err)
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	components := strings.Split(relPath, string(os.PathSeparator))
	syslog.L.Infof("[NFS.FromHandle] Path components: %v", components)
	return fs, components, nil
}

// InvalidateHandle removes a file handle from the cache
func (h *NFSHandler) InvalidateHandle(fs billy.Filesystem, fh []byte) error {
	h.handles.Delete(string(fh))
	return nil
}

func (h *NFSHandler) validateConnection(conn net.Conn) error {
	server, err := url.Parse(h.session.serverURL)
	if err != nil {
		return fmt.Errorf("failed to parse server IP: %w", err)
	}

	remoteAddr := conn.RemoteAddr().String()
	if !strings.Contains(remoteAddr, server.Hostname()) {
		return fmt.Errorf("unregistered client attempted to connect: %s", remoteAddr)
	}
	return nil
}

func (h *NFSHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	// Add logging to debug the mount request
	syslog.L.Infof("[NFS.Mount] Received mount request for path: %s from %s",
		string(req.Dirpath), conn.RemoteAddr().String())

	if err := h.validateConnection(conn); err != nil {
		syslog.L.Errorf("[NFS.Mount] Connection validation failed: %v", err)
		return nfs.MountStatusErrPerm, nil, nil
	}

	fs := &ReadOnlyFS{
		basePath: h.session.Snapshot.SnapshotPath,
		snapshot: h.session.Snapshot,
		ctx:      h.session.Context,
	}

	syslog.L.Infof("[NFS.Mount] Serving snapshot path: %s", h.session.Snapshot.SnapshotPath)
	syslog.L.Infof("[NFS.Mount] Root path: %s", fs.Root())

	return nfs.MountStatusOk, fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (h *NFSHandler) Change(fs billy.Filesystem) billy.Change {
	return nil
}

func (h *NFSHandler) FSStat(ctx context.Context, fs billy.Filesystem, stat *nfs.FSStat) error {
	stat.TotalSize = 1 << 40
	stat.FreeSize = 0
	stat.AvailableSize = 0
	stat.TotalFiles = 1 << 20
	stat.FreeFiles = 0
	stat.AvailableFiles = 0
	stat.CacheHint = time.Minute

	return nil
}

// ReadOnlyFile implements billy.File interface
type ReadOnlyFile struct {
	path string
	file *os.File
}

func NewReadOnlyFile(path string, file *os.File) *ReadOnlyFile {
	return &ReadOnlyFile{
		path: path,
		file: file,
	}
}

func (f *ReadOnlyFile) Name() string {
	return f.path
}

func (f *ReadOnlyFile) Read(p []byte) (n int, err error) {
	return f.file.Read(p)
}

func (f *ReadOnlyFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.file.ReadAt(p, off)
}

func (f *ReadOnlyFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *ReadOnlyFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("file is read-only")
}

func (f *ReadOnlyFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("file is read-only")
}

func (f *ReadOnlyFile) Close() error {
	return f.file.Close()
}

func (f *ReadOnlyFile) Truncate(size int64) error {
	return fmt.Errorf("file is read-only")
}

func (f *ReadOnlyFile) Lock() error {
	return nil
}

func (f *ReadOnlyFile) Unlock() error {
	return nil
}

// ReadOnlyFS implements billy.Filesystem interface
var _ billy.Filesystem = (*ReadOnlyFS)(nil) // Verify interface compliance
type ReadOnlyFS struct {
	basePath string
	snapshot *snapshots.WinVSSSnapshot
	ctx      context.Context
}

func (fs *ReadOnlyFS) Create(filename string) (billy.File, error) {
	return nil, fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Open(filename string) (billy.File, error) {
	syslog.L.Infof("[NFS.Open] Opening file: %s", filename)

	// Handle root path
	if filename == "" || filename == "/" || filename == "\\" {
		syslog.L.Infof("[NFS.Open] Attempted to open root directory")
		return nil, fmt.Errorf("cannot open directory")
	}

	fullPath := filepath.Join(fs.basePath, filepath.Clean(filename))
	syslog.L.Infof("[NFS.Open] Full path: %s", fullPath)

	if skipFile(fullPath, fs.snapshot) {
		syslog.L.Infof("[NFS.Open] File skipped: %s", fullPath)
		return nil, os.ErrNotExist
	}

	file, err := os.Open(fullPath)
	if err != nil {
		syslog.L.Errorf("[NFS.Open] Failed to open file: %v", err)
		return nil, err
	}

	return NewReadOnlyFile(filename, file), nil
}

func (fs *ReadOnlyFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("filesystem is read-only")
	}
	return fs.Open(filename)
}

func (fs *ReadOnlyFS) Stat(filename string) (os.FileInfo, error) {
	fullPath := filepath.Join(fs.basePath, filepath.Clean(filename))
	if skipFile(fullPath, fs.snapshot) {
		return nil, os.ErrNotExist
	}
	return os.Stat(fullPath)
}

func (fs *ReadOnlyFS) ReadDir(path string) ([]os.FileInfo, error) {
	syslog.L.Infof("[NFS.ReadDir] Reading directory: %s", path)

	// Handle root path
	dirPath := path
	if path == "" || path == "/" || path == "\\" {
		dirPath = fs.basePath
	} else {
		dirPath = filepath.Join(fs.basePath, filepath.Clean(path))
	}

	syslog.L.Infof("[NFS.ReadDir] Full directory path: %s", dirPath)

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		syslog.L.Errorf("[NFS.ReadDir] Failed to read directory: %v", err)
		return nil, err
	}

	var fileInfos []os.FileInfo
	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())
		if skipFile(entryPath, fs.snapshot) {
			syslog.L.Infof("[NFS.ReadDir] Skipping file: %s", entryPath)
			continue
		}

		info, err := entry.Info()
		if err != nil {
			syslog.L.Errorf("[NFS.ReadDir] Failed to get file info: %v", err)
			continue
		}
		fileInfos = append(fileInfos, &CustomFileInfo{
			FileInfo:   info,
			filePath:   entryPath,
			snapshotId: fs.snapshot.Id,
		})
	}

	syslog.L.Infof("[NFS.ReadDir] Found %d entries in %s", len(fileInfos), dirPath)
	return fileInfos, nil
}

func (fs *ReadOnlyFS) Rename(oldpath, newpath string) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Remove(filename string) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ReadOnlyFS) MkdirAll(filename string, perm os.FileMode) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Lstat(filename string) (os.FileInfo, error) {
	fullPath := filepath.Join(fs.basePath, filepath.Clean(filename))
	if skipFile(fullPath, fs.snapshot) {
		return nil, os.ErrNotExist
	}
	return os.Lstat(fullPath)
}

func (fs *ReadOnlyFS) Symlink(target, link string) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Readlink(link string) (string, error) {
	fullPath := filepath.Join(fs.basePath, filepath.Clean(link))
	if skipFile(fullPath, fs.snapshot) {
		return "", os.ErrNotExist
	}
	return os.Readlink(fullPath)
}

func (fs *ReadOnlyFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Chmod(name string, mode os.FileMode) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Lchown(name string, uid, gid int) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Chown(name string, uid, gid int) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Root() string {
	return fs.basePath
}

// Additional required methods from billy.Filesystem interface
func (fs *ReadOnlyFS) Mkdir(filename string, perm os.FileMode) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *ReadOnlyFS) Chroot(path string) (billy.Filesystem, error) {
	fullPath := filepath.Join(fs.basePath, filepath.Clean(path))
	return &ReadOnlyFS{
		basePath: fullPath,
		snapshot: fs.snapshot,
		ctx:      fs.ctx,
	}, nil
}

func (s *NFSSession) Serve() error {
	port, err := utils.DriveLetterPort([]rune(s.DriveLetter)[0])
	if err != nil {
		return fmt.Errorf("unable to determine port number: %v", err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	s.listener = listener
	defer listener.Close()

	handler := &NFSHandler{
		session: s,
	}

	cachingHandler := nfshelper.NewCachingHandler(handler, 1000)

	syslog.L.Infof("[NFS.Serve] Serving NFS on port %s", port)

	return nfs.Serve(listener, cachingHandler)
}
