//go:build windows
// +build windows

package sftp

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"

	"github.com/pkg/sftp"
)

// FileInfoWithUnknownSize wraps an os.FileInfo and overrides the Size method to return -1.
type FileInfoWithUnknownSize struct {
	os.FileInfo
}

func (f *FileInfoWithUnknownSize) Size() int64 {
	// Convert file path to UTF-16 pointer for Windows API call
	lpFileName, err := syscall.UTF16PtrFromString(f.Name())
	if err != nil {
		return -1
	}

	// Open the file with read-only access
	handle, err := syscall.CreateFile(lpFileName, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return -1
	}
	defer syscall.CloseHandle(handle)

	// Create a file mapping
	mappingHandle, err := syscall.CreateFileMapping(handle, nil, syscall.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return -1
	}
	defer syscall.CloseHandle(mappingHandle)

	// Map a view of the file into the process's address space
	ptr, err := syscall.MapViewOfFile(mappingHandle, syscall.FILE_MAP_READ, 0, 0, 0)
	if err != nil {
		return -1
	}
	defer syscall.UnmapViewOfFile(ptr)

	// Calculate the file size by examining the mapped memory
	fileSize := int64(unsafe.Sizeof(*(*byte)(unsafe.Pointer(ptr))))

	return fileSize
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

