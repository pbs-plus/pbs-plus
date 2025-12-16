package arpc

import (
	"errors"
	"os"

	"github.com/quic-go/quic-go"
)

// QUIC error codes
const (
	quicErrServeNoRouter       quic.StreamErrorCode      = 0x1101
	quicErrServePanic          quic.StreamErrorCode      = 0x1102
	quicErrClientCertRequired  quic.ApplicationErrorCode = 0x1105
	quicErrInitPipeFailed      quic.ApplicationErrorCode = 0x1106
	quicErrHeadersInitFailed   quic.ApplicationErrorCode = 0x1107
	quicErrHeadersWriteFailed  quic.ApplicationErrorCode = 0x1108
	quicErrHeadersCloseFailed  quic.ApplicationErrorCode = 0x1109
	quicErrClosePipe           quic.ApplicationErrorCode = 0x110A
	quicErrOpenStreamFailed    quic.ApplicationErrorCode = 0x110C
	quicErrMarshalPayload      quic.StreamErrorCode      = 0x1001
	quicErrEncodeRequest       quic.StreamErrorCode      = 0x1002
	quicErrWriteRequest        quic.StreamErrorCode      = 0x1003
	quicErrDecodeResponse      quic.StreamErrorCode      = 0x1004
	quicErrInvalidRawHandler   quic.StreamErrorCode      = 0x1005
	quicErrRawReadySignalWrite quic.StreamErrorCode      = 0x1006
	quicErrRPCStatus           quic.StreamErrorCode      = 0x1007
	quicErrRawNotSupported     quic.StreamErrorCode      = 0x1008
	quicErrWriteResponse       quic.StreamErrorCode      = 0x1205
	quicErrRawHandshakeFail    quic.StreamErrorCode      = 0x1206
)

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

		if errors.Is(pathErr.Err, os.ErrNotExist) {
			serErr.ErrorType = "os.ErrNotExist"
		} else if errors.Is(pathErr.Err, os.ErrPermission) {
			serErr.ErrorType = "os.ErrPermission"
		} else if errors.Is(pathErr.Err, os.ErrProcessDone) {
			serErr.ErrorType = "os.ErrProcessDone"
		} else {
			serErr.ErrorType = "os.PathError"
		}
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
		op := serErr.Op
		if op == "" {
			op = "open"
		}
		return &os.PathError{Op: op, Path: serErr.Path, Err: errors.New("unknown error")}
	case "os.ErrTimeout":
		return os.ErrDeadlineExceeded
	case "os.ErrClosed":
		return os.ErrClosed
	case "os.ErrProcessDone":
		return os.ErrProcessDone
	default:
		return errors.New(serErr.Message)
	}
}
