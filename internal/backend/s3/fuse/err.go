//go:build linux

package fuse

import (
	"errors"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/minio/minio-go/v7"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func s3ErrorToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	syslog.L.Error(err).WithMessage("s3 error").Write()

	// Handle MinIO specific errors
	var minioErr minio.ErrorResponse
	if errors.As(err, &minioErr) {
		switch minioErr.Code {
		case "NoSuchBucket":
			return syscall.ENOENT
		case "NoSuchKey":
			return syscall.ENOENT
		case "AccessDenied":
			return syscall.EACCES
		case "InvalidBucketName":
			return syscall.EINVAL
		case "BucketNotEmpty":
			return syscall.ENOTEMPTY
		default:
			return syscall.EIO
		}
	}

	// Handle common error strings
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "does not exist"):
		return syscall.ENOENT
	case strings.Contains(errStr, "access denied") || strings.Contains(errStr, "forbidden"):
		return syscall.EACCES
	case strings.Contains(errStr, "invalid"):
		return syscall.EINVAL
	case strings.Contains(errStr, "timeout"):
		return syscall.ETIMEDOUT
	case strings.Contains(errStr, "connection"):
		return syscall.ECONNREFUSED
	default:
		// Try the default conversion first
		if errno := fs.ToErrno(err); errno != syscall.EIO {
			return errno
		}
		return syscall.EIO
	}
}
