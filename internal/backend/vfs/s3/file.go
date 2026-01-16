//go:build linux

package s3fs

import (
	"io"
	"syscall"

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

	return f.readRemote(buf, off)
}

func (f *S3File) readRemote(buf []byte, off int64) (int, error) {
	if f.body != nil && off != f.currPos {
		f.body.Close()
		f.body = nil
	}

	if f.body == nil {
		opts := minio.GetObjectOptions{}
		// Request from off to the end to keep the stream alive
		_ = opts.SetRange(off, f.size-1)

		obj, err := f.fs.client.GetObject(f.fs.Ctx, f.fs.bucket, f.key, opts)
		if err != nil {
			return 0, err
		}
		f.body = obj
		f.currPos = off
	}

	bytesToRead := len(buf)
	if off+int64(bytesToRead) > f.size {
		bytesToRead = int(f.size - off)
	}

	n, err := io.ReadAtLeast(f.body, buf[:bytesToRead], bytesToRead)

	f.currPos += int64(n)
	f.fs.TotalBytes.Add(int64(n))

	return n, err
}

func (f *S3File) Close() error {
	syslog.L.Debug().WithMessage("Close file").WithField("key", f.key).Write()
	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	if f.body != nil {
		err = f.body.Close()
		f.body = nil
	}
	return err
}
