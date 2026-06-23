package arpc

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func (se *SerializableError) Error() string {
	return se.Message
}

func WrapError(err error) *SerializableError {
	if err == nil {
		return nil
	}

	serErr := SerializableError{
		ErrorType:     "unknown",
		Message:       err.Error(),
		OriginalError: err,
	}

	if pathErr, ok := err.(*os.PathError); ok {
		serErr.Op = pathErr.Op
		serErr.Path = pathErr.Path
		serErr.Message = fmt.Sprintf("%s %s: %s", pathErr.Op, pathErr.Path, pathErr.Err)

		if errors.Is(pathErr.Err, os.ErrNotExist) {
			serErr.ErrorType = "os.ErrNotExist"
		} else if errors.Is(pathErr.Err, os.ErrPermission) {
			serErr.ErrorType = "os.ErrPermission"
		} else if errors.Is(pathErr.Err, os.ErrProcessDone) {
			serErr.ErrorType = "os.ErrProcessDone"
		} else if errno, ok := pathErr.Err.(syscall.Errno); ok {
			serErr.ErrorType = fmt.Sprintf("syscall.Errno(%d)", errno)
		} else {
			serErr.ErrorType = "os.PathError"
		}
		return &serErr
	}

	if errno, ok := err.(syscall.Errno); ok {
		serErr.ErrorType = fmt.Sprintf("syscall.Errno(%d)", errno)
		serErr.Message = errno.Error()
		return &serErr
	}

	if os.IsNotExist(err) {
		serErr.ErrorType = "os.ErrNotExist"
	} else if os.IsPermission(err) {
		serErr.ErrorType = "os.ErrPermission"
	} else if os.IsTimeout(err) {
		serErr.ErrorType = "os.ErrTimeout"
	} else if errors.Is(err, os.ErrClosed) {
		serErr.ErrorType = "os.ErrClosed"
	} else if errors.Is(err, os.ErrProcessDone) {
		serErr.ErrorType = "os.ErrProcessDone"
	}

	return &serErr
}

func UnwrapError(serErr SerializableError) error {
	switch serErr.ErrorType {
	case "os.ErrNotExist":
		op := serErr.Op
		if op == "" {
			op = "open"
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
	case "os.ErrTimeout":
		return os.ErrDeadlineExceeded
	case "os.ErrClosed":
		return os.ErrClosed
	case "os.ErrProcessDone":
		return os.ErrProcessDone
	default:
		if serErr.Op != "" || serErr.Path != "" {
			op := serErr.Op
			if op == "" {
				op = "open"
			}
			return &os.PathError{Op: op, Path: serErr.Path, Err: errors.New(serErr.Message)}
		}
		return errors.New(serErr.Message)
	}
}
