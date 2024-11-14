//go:build windows
// +build windows

package sftp

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/sftp"
)

// FileInfoWithUnknownSize wraps an os.FileInfo and overrides the Size method to return -1.
type FileInfoWithUnknownSize struct {
	os.FileInfo
}

// Size overrides the original Size method to always return -1.
func (f *FileInfoWithUnknownSize) Size() int64 {
	return -1
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
			fileInfos = append(fileInfos, &FileInfoWithUnknownSize{FileInfo: info})
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
	return &FileLister{files: []os.FileInfo{&FileInfoWithUnknownSize{FileInfo: stat}}}, nil
}

func (h *SftpHandler) setFilePath(r *sftp.Request) {
	r.Filepath = filepath.Join(h.Snapshot.SnapshotPath, filepath.Clean(r.Filepath))
}

