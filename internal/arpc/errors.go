package arpc

import (
	"errors"
	"os"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/valyala/bytebufferpool"
)

// Error implements the error interface
func (se *SerializableError) Error() string {
	return utils.ToString(se.Message)
}

// WrapError identifies and wraps standard Go errors for serialization
func WrapError(err error) SerializableError {
	if err == nil {
		return SerializableError{}
	}

	// Start with a generic error wrapper
	serErr := SerializableError{
		ErrorType:     utils.ToBytes("unknown"),
		Message:       utils.ToBytes(err.Error()),
		OriginalError: err,
	}

	// Extract path information from PathError
	if pathErr, ok := err.(*os.PathError); ok {
		serErr.Op = utils.ToBytes(pathErr.Op)
		serErr.Path = utils.ToBytes(pathErr.Path)

		// Identify the underlying error type
		if errors.Is(pathErr.Err, os.ErrNotExist) {
			serErr.ErrorType = utils.ToBytes("os.ErrNotExist")
		} else if errors.Is(pathErr.Err, os.ErrPermission) {
			serErr.ErrorType = utils.ToBytes("os.ErrPermission")
		} else {
			serErr.ErrorType = utils.ToBytes("os.PathError")
		}
		return serErr
	}

	// Check for specific error types
	if os.IsNotExist(err) {
		serErr.ErrorType = utils.ToBytes("os.ErrNotExist")
	} else if os.IsPermission(err) {
		serErr.ErrorType = utils.ToBytes("os.ErrPermission")
	} else if os.IsTimeout(err) {
		serErr.ErrorType = utils.ToBytes("os.ErrTimeout")
	} else if errors.Is(err, os.ErrClosed) {
		serErr.ErrorType = utils.ToBytes("os.ErrClosed")
	}
	// Add more error types as needed

	return serErr
}

func WrapErrorBytes(err error) *bytebufferpool.ByteBuffer {
	errWrapped := WrapError(err)
	errBytes, _ := marshalWithPool(&errWrapped)
	if errBytes == nil {
		return nil
	}

	return errBytes
}

// UnwrapError reconstructs the original error type from the serialized data
func UnwrapError(serErr SerializableError) error {
	switch utils.ToString(serErr.ErrorType) {
	case "os.ErrNotExist":
		// Create a PathError with os.ErrNotExist and the correct path
		op := utils.ToString(serErr.Op)
		if op == "" {
			op = "open" // Default op
		}
		return &os.PathError{
			Op:   op,
			Path: utils.ToString(serErr.Path),
			Err:  os.ErrNotExist,
		}
	case "os.ErrPermission":
		op := utils.ToString(serErr.Op)
		if op == "" {
			op = "open"
		}
		return &os.PathError{Op: op, Path: utils.ToString(serErr.Path), Err: os.ErrPermission}
	case "os.PathError":
		// Generic PathError
		op := utils.ToString(serErr.Op)
		if op == "" {
			op = "open"
		}
		return &os.PathError{Op: op, Path: utils.ToString(serErr.Path), Err: errors.New("unknown error")}
	case "os.ErrTimeout":
		return os.ErrDeadlineExceeded
	case "os.ErrClosed":
		return os.ErrClosed
	default:
		// Return a simple error with the original message
		return errors.New(utils.ToString(serErr.Message))
	}
}
