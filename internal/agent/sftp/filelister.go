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

var sizeCache sync.Map // Map[filePath]*sync.Map (snapshotId -> size)
var mutexMap sync.Map  // Map[filePath]*sync.RWMutex

type CustomFileInfo struct {
	os.FileInfo
	filePath   string
	snapshotId string
}

func getMutex(filePath string) *sync.RWMutex {
	mutex, _ := mutexMap.LoadOrStore(filePath, &sync.RWMutex{})
	return mutex.(*sync.RWMutex)
}

func (f *CustomFileInfo) Size() int64 {
	metadataSize := f.FileInfo.Size()
	ext := filepath.Ext(f.filePath)
	baseName := filepath.Base(f.filePath)

	if ext != "" {
		fileExtensions := []string{
			".log",
			".dat",
		}

		scanFile := false
		for _, fileExtension := range fileExtensions {
			if strings.Contains(baseName, fileExtension) {
				scanFile = true
				break
			}
		}

		if !scanFile {
			return metadataSize
		}
	} else if ext == "" && metadataSize > 10485760 {
		return metadataSize
	}

	// Retrieve the file-specific mutex
	fileMutex := getMutex(f.filePath)

	// Check size cache with read lock
	fileMutex.RLock()
	if snapSizes, ok := sizeCache.Load(f.filePath); ok {
		if cachedSize, ok := snapSizes.(*sync.Map).Load(f.snapshotId); ok {
			fileMutex.RUnlock()
			return cachedSize.(int64)
		}
	}
	fileMutex.RUnlock()

	// Compute file size if not cached
	file, err := os.Open(f.filePath)
	if err != nil {
		return 0
	}
	defer file.Close()

	byteCount, err := io.Copy(io.Discard, file)
	if err != nil {
		return 0
	}

	// Update cache with write lock
	fileMutex.Lock()
	defer fileMutex.Unlock()

	snapSizes, _ := sizeCache.LoadOrStore(f.filePath, &sync.Map{})
	snapSizes.(*sync.Map).Store(f.snapshotId, byteCount)

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
