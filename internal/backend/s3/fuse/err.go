//go:build linux

package fuse

import (
	"errors"
	"net/http"
	"strings"
	"syscall"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/hanwen/go-fuse/v2/fs"
)

func s3ErrorToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	// Handle AWS SDK v2 specific errors
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
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
		case "InvalidRequest":
			return syscall.EINVAL
		case "RequestTimeout":
			return syscall.ETIMEDOUT
		case "SlowDown":
			return syscall.EAGAIN
		case "ServiceUnavailable":
			return syscall.EAGAIN
		case "InternalError":
			return syscall.EIO
		default:
			return syscall.EIO
		}
	}

	// Handle specific S3 service exception types
	var noSuchBucket *types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return syscall.ENOENT
	}

	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return syscall.ENOENT
	}

	var bucketAlreadyExists *types.BucketAlreadyExists
	if errors.As(err, &bucketAlreadyExists) {
		return syscall.EEXIST
	}

	var bucketAlreadyOwnedByYou *types.BucketAlreadyOwnedByYou
	if errors.As(err, &bucketAlreadyOwnedByYou) {
		return syscall.EEXIST
	}

	var invalidObjectState *types.InvalidObjectState
	if errors.As(err, &invalidObjectState) {
		return syscall.EINVAL
	}

	var objectAlreadyInActiveTierError *types.ObjectAlreadyInActiveTierError
	if errors.As(err, &objectAlreadyInActiveTierError) {
		return syscall.EEXIST
	}

	var objectNotInActiveTierError *types.ObjectNotInActiveTierError
	if errors.As(err, &objectNotInActiveTierError) {
		return syscall.ENOENT
	}

	// Handle HTTP response errors - CORRECTED
	var responseError *awshttp.ResponseError
	if errors.As(err, &responseError) {
		switch responseError.HTTPStatusCode() {
		case http.StatusNotFound:
			return syscall.ENOENT
		case http.StatusForbidden:
			return syscall.EACCES
		case http.StatusUnauthorized:
			return syscall.EACCES
		case http.StatusBadRequest:
			return syscall.EINVAL
		case http.StatusRequestTimeout:
			return syscall.ETIMEDOUT
		case http.StatusTooManyRequests:
			return syscall.EAGAIN
		case http.StatusInternalServerError:
			return syscall.EIO
		case http.StatusBadGateway:
			return syscall.ECONNREFUSED
		case http.StatusServiceUnavailable:
			return syscall.EAGAIN
		case http.StatusGatewayTimeout:
			return syscall.ETIMEDOUT
		default:
			return syscall.EIO
		}
	}

	// Handle operation errors (network issues, etc.)
	var opError *smithy.OperationError
	if errors.As(err, &opError) {
		// Check if it's a network/connection error
		if strings.Contains(opError.Error(), "connection") {
			return syscall.ECONNREFUSED
		}
		if strings.Contains(opError.Error(), "timeout") {
			return syscall.ETIMEDOUT
		}
		if strings.Contains(opError.Error(), "canceled") {
			return syscall.ECANCELED
		}
		return syscall.EIO
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
		if errno := fs.ToErrno(err); errno != syscall.EIO {
			return errno
		}
		return syscall.EIO
	}
}

// Helper function to check if an error indicates the object/path doesn't exist
func isNotFoundError(err error) bool {
	errno := s3ErrorToErrno(err)
	return errno == syscall.ENOENT
}

// Helper function to check if an error indicates access is denied
func isAccessDeniedError(err error) bool {
	errno := s3ErrorToErrno(err)
	return errno == syscall.EACCES
}

// Helper function to check if an error is retryable
func isRetryableError(err error) bool {
	errno := s3ErrorToErrno(err)
	return errno == syscall.EAGAIN || errno == syscall.ETIMEDOUT || errno == syscall.ECONNREFUSED
}
