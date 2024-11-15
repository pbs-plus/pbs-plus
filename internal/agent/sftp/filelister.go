//go:build windows
// +build windows

package sftp

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/sftp"
)

var cacheMutex sync.RWMutex
var sizeCache map[string]map[string]int64
var cacheOnce sync.Once

// CustomFileInfo wraps an os.FileInfo and overrides the Size method to return -1.
type CustomFileInfo struct {
	os.FileInfo
	filePath   string
	snapshotId string
}

func initializeSizeCache() {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	if sizeCache == nil {
		sizeCache = make(map[string]map[string]int64)
	}
}

func (f *CustomFileInfo) Size() int64 {
	cacheOnce.Do(initializeSizeCache)

	cacheMutex.RLock()
	if _, ok := sizeCache[f.filePath]; ok {
		if cachedSize, ok := sizeCache[f.filePath][f.snapshotId]; ok {
			return cachedSize
		}
	}
	cacheMutex.RUnlock()

	cacheMutex.Lock()
	sizeCache[f.filePath] = make(map[string]int64)
	cacheMutex.Unlock()

	file, err := os.Open(f.filePath)
	if err != nil {
		return 0
	}
	defer file.Close()

	byteCount, err := io.Copy(io.Discard, file)
	if err != nil {
		return 0
	}

	cacheMutex.Lock()
	sizeCache[f.filePath][f.snapshotId] = byteCount
	cacheMutex.Unlock()

	return byteCount
}

type FileLister struct {
	files []os.FileInfo
}

func (fl *FileLister) ListAt(fileList []os.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(fl.files)) {
		return 0, io.EOF
	}

	n := copy(fileList, fl.files[offset:])
	if n < len(fileList) {
		return n, io.EOF
	}
	return n, nil
}

func (h *SftpHandler) FileLister(dirPath string) (*FileLister, error) {
	dirEntries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	fileInfos := make([]os.FileInfo, 0, len(dirEntries))
	for _, entry := range dirEntries {
		select {
		case <-h.ctx.Done():
			return &FileLister{files: fileInfos}, nil
		default:
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}

			fullPath := filepath.Join(dirPath, entry.Name())
			if h.skipFile(fullPath) {
				continue
			}
			// Wrap the original os.FileInfo to override its Size method.
			fileInfos = append(fileInfos, &CustomFileInfo{FileInfo: info, filePath: fullPath, snapshotId: h.Snapshot.Id})
		}
	}

	return &FileLister{files: fileInfos}, nil
}

func (h *SftpHandler) FileStat(filename string) (*FileLister, error) {
	var stat fs.FileInfo
	var err error

	isRoot := strings.TrimPrefix(filename, h.Snapshot.SnapshotPath) == ""
	if isRoot {
		stat, err = os.Stat(filename)
		if err != nil {
			return nil, err
		}
	} else {
		stat, err = os.Lstat(filename)
		if err != nil {
			return nil, err
		}
	}

	// Wrap the original os.FileInfo to override its Size method.
	return &FileLister{files: []os.FileInfo{&CustomFileInfo{FileInfo: stat, filePath: filename, snapshotId: h.Snapshot.Id}}}, nil
}

func (h *SftpHandler) setFilePath(r *sftp.Request) {
	r.Filepath = filepath.Join(h.Snapshot.SnapshotPath, filepath.Clean(r.Filepath))
}
