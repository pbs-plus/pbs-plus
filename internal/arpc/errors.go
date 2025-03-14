package arpc

import (
	"errors"
	"os"
)

// Error implements the error interface for SerializableError.
func (se *SerializableError) Error() string {
	return se.Message
}

func IsOSError(err error) bool {
	if os.IsNotExist(err) {
		return true
	} else if os.IsPermission(err) {
		return true
	} else if os.IsTimeout(err) {
		return true
	} else if errors.Is(err, os.ErrClosed) {
		return true
	}

	return false
}

// WrapError identifies and wraps standard Go errors for serialization.
func WrapError(err error) *SerializableError {
	if err == nil {
		return nil
	}

	// Start with a generic error wrapper
	serErr := SerializableError{
		ErrorType:     "unknown",
		Message:       err.Error(),
		OriginalError: err,
	}

	// Extract path information from PathError
	if pathErr, ok := err.(*os.PathError); ok {
		serErr.Op = pathErr.Op
		serErr.Path = pathErr.Path

		// Identify the underlying error type
		if errors.Is(pathErr.Err, os.ErrNotExist) {
			serErr.ErrorType = "os.ErrNotExist"
		} else if errors.Is(pathErr.Err, os.ErrPermission) {
			serErr.ErrorType = "os.ErrPermission"
		} else {
			serErr.ErrorType = "os.PathError"
		}
		return &serErr
	}

	// Check for specific error types
	if os.IsNotExist(err) {
		serErr.ErrorType = "os.ErrNotExist"
	} else if os.IsPermission(err) {
		serErr.ErrorType = "os.ErrPermission"
	} else if os.IsTimeout(err) {
		serErr.ErrorType = "os.ErrTimeout"
	} else if errors.Is(err, os.ErrClosed) {
		serErr.ErrorType = "os.ErrClosed"
	}
	// Add more error types as needed

	return &serErr
}

// UnwrapError reconstructs the original error type from the serialized data.
func UnwrapError(serErr SerializableError) error {
	switch serErr.ErrorType {
	case "os.ErrNotExist":
		// Create a PathError with os.ErrNotExist and the correct path
		op := serErr.Op
		if op == "" {
			op = "open" // Default op
		}
		return &os.PathError{
			Op:   op,
			Path: serErr.Path,
			Err:  os.ErrNotExist,
		}
	case "os.ErrPermission":
		op := serErr.Op
		if op == "" {
			op = "open"
		}
		return &os.PathError{Op: op, Path: serErr.Path, Err: os.ErrPermission}
	case "os.PathError":
		// Generic PathError
		op := serErr.Op
		if op == "" {
			op = "open"
		}
		return &os.PathError{Op: op, Path: serErr.Path, Err: errors.New("unknown error")}
	case "os.ErrTimeout":
		return os.ErrDeadlineExceeded
	case "os.ErrClosed":
		return os.ErrClosed
	default:
		// Return a simple error with the original message
		return errors.New(serErr.Message)
	}
}
