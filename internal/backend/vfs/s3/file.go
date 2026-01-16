//go:build linux

package s3fs

import (
	"context"
	"io"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (f *S3File) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off >= f.size {
		return 0, io.EOF
	}

	syslog.L.Debug().
		WithMessage("ReadAt called").
		WithField("key", f.key).
		WithField("offset", off).
		WithField("length", len(buf)).
		Write()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Check local read-ahead buffer
	if f.buf != nil && off >= f.bufOff && off < f.bufOff+int64(len(f.buf)) {
		n := copy(buf, f.buf[off-f.bufOff:])
		if n == len(buf) {
			return n, nil
		}
		remainingBuf, err := f.readRemote(buf[n:], off+int64(n))
		return n + remainingBuf, err
	}

	return f.readRemote(buf, off)
}

func (f *S3File) readRemote(buf []byte, off int64) (int, error) {
	fetchSize := int64(len(buf))
	if fetchSize < readAheadSize {
		fetchSize = readAheadSize
	}
	if off+fetchSize > f.size {
		fetchSize = f.size - off
	}

	ctx, cancel := context.WithTimeout(f.fs.Ctx, 30*time.Second)
	defer cancel()

	opts := minio.GetObjectOptions{}
	_ = opts.SetRange(off, off+fetchSize-1)

	obj, err := f.fs.client.GetObject(ctx, f.fs.bucket, f.key, opts)
	if err != nil {
		syslog.L.Error(err).WithField("key", f.key).WithMessage("S3 GetObject failed").Write()
		return 0, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil && err != io.EOF {
		return 0, err
	}

	f.buf = data
	f.bufOff = off

	n := copy(buf, data)
	f.fs.TotalBytes.Add(int64(n))

	syslog.L.Debug().
		WithMessage("ReadAt completed").
		WithField("key", f.key).
		WithField("bytesRead", n).
		WithField("totalBytes", f.fs.TotalBytes.Value()).
		Write()

	if n < len(buf) && off+int64(n) >= f.size {
		return n, io.EOF
	}
	return n, nil
}

func (f *S3File) Close() error {
	syslog.L.Debug().WithMessage("Close file").WithField("key", f.key).Write()
	f.mu.Lock()
	f.buf = nil
	f.mu.Unlock()
	return nil
}
