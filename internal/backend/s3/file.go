//go:build linux

package s3fs

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
)

// S3File with read-ahead buffer
type S3File struct {
	fs        *S3FS
	key       string
	buf       []byte
	bufOffset int64
	mu        sync.Mutex
}

func (f *S3File) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Serve from buffer if possible
	if f.buf != nil && off >= f.bufOffset &&
		off+int64(len(p)) <= f.bufOffset+int64(len(f.buf)) {
		copy(p, f.buf[off-f.bufOffset:])
		return len(p), nil
	}

	// Fetch with read-ahead
	end := off + int64(len(p))
	rangeEnd := off + readAheadSize - 1
	if rangeEnd < end {
		rangeEnd = end
	}

	opts := minio.GetObjectOptions{}
	_ = opts.SetRange(off, rangeEnd)
	obj, err := f.fs.client.GetObject(f.fs.ctx, f.fs.bucket, f.key, opts)
	if err != nil {
		return 0, err
	}
	defer obj.Close()

	buf := new(bytes.Buffer)
	n, err := io.Copy(buf, obj)
	if err != nil && err != io.EOF {
		return 0, err
	}

	f.buf = buf.Bytes()
	f.bufOffset = off

	// Ensure we don't copy more than what's available
	copyLen := len(p)
	if copyLen > len(f.buf) {
		copyLen = len(f.buf)
	}
	copy(p, f.buf[:copyLen])
	atomic.AddInt64(&f.fs.totalBytes, int64(n))
	return copyLen, nil
}

func (f *S3File) Close() error { return nil }

func (f *S3File) Lseek(off int64, _ int) (uint64, error) {
	return uint64(off), nil
}
