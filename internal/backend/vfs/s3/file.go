//go:build linux

package s3fs

import (
	"context"
	"io"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (f *S3File) ReadAt(ctx context.Context, buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	if off < 0 {
		return 0, syscall.EINVAL
	}

	ctx, cancel := context.WithTimeout(f.fs.Ctx, 30*time.Second)
	defer cancel()

	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, off+int64(len(buf))-1)
	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("failed to handle read request to s3").
			WithField("path", f.key).
			WithField("offset", f.offset).
			WithField("length", len(buf)).
			Write()
		return 0, err
	}

	obj, err := f.fs.client.GetObject(ctx, f.fs.bucket, f.key, opts)
	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("failed to handle read request to s3").
			WithField("path", f.key).
			WithField("offset", f.offset).
			WithField("length", len(buf)).
			Write()
		return 0, err
	}
	defer obj.Close()

	n, err := io.ReadFull(obj, buf)

	if err == io.ErrUnexpectedEOF {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("unexpected eof handled from s3 file").
			WithField("path", f.key).
			WithField("offset", f.offset).
			WithField("length", len(buf)).
			Write()
		atomic.AddInt64(&f.fs.TotalBytes, int64(n))
		return n, io.EOF
	}

	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("error occurred during s3 file reading operation").
			WithField("path", f.key).
			WithField("offset", f.offset).
			WithField("length", len(buf)).
			Write()
		return n, err
	}

	atomic.AddInt64(&f.fs.TotalBytes, int64(n))

	if n < len(buf) {
		return n, io.EOF
	}

	return n, nil
}

func (f *S3File) Lseek(ctx context.Context, off int64, whence int) (uint64, error) {
	return 0, syscall.EOPNOTSUPP
}

func (f *S3File) Close(ctx context.Context) error {
	return nil
}
