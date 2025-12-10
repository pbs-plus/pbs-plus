//go:build linux

package s3fs

import (
	"context"
	"io"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
)

var _ vfs.FileHandle = (*S3File)(nil)

// ReadAt reads len(buf) bytes from the file starting at byte offset off.
func (f *S3File) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	if off < 0 {
		return 0, syscall.EINVAL
	}

	ctx, cancel := context.WithTimeout(f.fs.ctx, 30*time.Second)
	defer cancel()

	// Use range request for the specific offset and length
	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, off+int64(len(buf))-1)
	if err != nil {
		return 0, err
	}

	obj, err := f.fs.client.GetObject(ctx, f.fs.bucket, f.key, opts)
	if err != nil {
		return 0, err
	}
	defer obj.Close()

	// Read the data
	n, err := io.ReadFull(obj, buf)

	// Handle partial reads at end of file
	if err == io.ErrUnexpectedEOF {
		return n, io.EOF
	}

	if err != nil {
		return n, err
	}

	atomic.AddInt64(&f.fs.totalBytes, int64(n))

	// If we read less than requested, it means we hit EOF
	if n < len(buf) {
		return n, io.EOF
	}

	return n, nil
}

func (f *S3File) Lseek(off int64, whence int) (uint64, error) {
	return 0, syscall.EOPNOTSUPP
}

// Close closes the file.
func (f *S3File) Close() error {
	return nil
}
