//go:build linux

package fuse

import (
	"errors"
	"strings"
	"syscall"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/rclone/rclone/fs"
)

func s3ErrorToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	// Handle rclone specific errors
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return syscall.ENOENT
	}
	if errors.Is(err, fs.ErrorDirNotFound) {
		return syscall.ENOENT
	}
	if errors.Is(err, fs.ErrorPermissionDenied) {
		return syscall.EACCES
	}
	if errors.Is(err, fs.ErrorNotAFile) {
		return syscall.EISDIR
	}
	if errors.Is(err, fs.ErrorIsFile) {
		return syscall.ENOTDIR
	}
	if errors.Is(err, fs.ErrorCantUploadEmptyFiles) {
		return syscall.EINVAL
	}

	// Handle common error strings as fallback
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "not found"):
		return syscall.ENOENT
	case strings.Contains(errStr, "access denied") ||
		strings.Contains(errStr, "forbidden") ||
		strings.Contains(errStr, "unauthorized"):
		return syscall.EACCES
	case strings.Contains(errStr, "invalid") || strings.Contains(errStr, "bad request"):
		return syscall.EINVAL
	case strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded"):
		return syscall.ETIMEDOUT
	case strings.Contains(errStr, "connection") || strings.Contains(errStr, "network"):
		return syscall.ECONNREFUSED
	case strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "rate limit"):
		return syscall.EAGAIN
	case strings.Contains(errStr, "canceled") || strings.Contains(errStr, "context canceled"):
		return syscall.ECANCELED
	case strings.Contains(errStr, "service unavailable"):
		return syscall.EAGAIN
	default:
		// Try the default conversion first
		if errno := gofs.ToErrno(err); errno != syscall.EIO {
			return errno
		}
		return syscall.EIO
	}
}
