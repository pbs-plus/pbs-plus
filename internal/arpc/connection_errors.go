package arpc

import (
	"fmt"
	"log"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

// ErrorCode represents a structured error code.
type ErrorCode string

const (
	ErrCodeNetworkIssue       ErrorCode = "E001"
	ErrCodeTimeout            ErrorCode = "E002"
	ErrCodeConnectionFailed   ErrorCode = "E003"
	ErrCodeCircuitBreakerOpen ErrorCode = "E004"
	ErrCodeUnknown            ErrorCode = "E999"
)

// errorCodeMap maps error patterns to error codes and human-friendly messages.
var errorCodeMap = map[string]struct {
	Code    ErrorCode
	Message string
}{
	"read/write on closed pipe": {
		Code:    ErrCodeNetworkIssue,
		Message: "A network issue occurred.",
	},
	"context deadline exceeded": {
		Code:    ErrCodeTimeout,
		Message: "The operation timed out.",
	},
	"connection reset by peer": {
		Code:    ErrCodeNetworkIssue,
		Message: "The connection was reset.",
	},
	"broken pipe": {
		Code:    ErrCodeNetworkIssue,
		Message: "A network issue occurred.",
	},
	"connection failed and circuit breaker is open": {
		Code:    ErrCodeCircuitBreakerOpen,
		Message: "Too much retries after a network issue. Attempting again in a few minutes.",
	},
}

// GeneralizeErrorLogWithCode filters and simplifies error messages, returning an error code and message.
func generalizeErrorLogWithCode(err error) (ErrorCode, string) {
	if err == nil {
		return "", ""
	}

	// Check if the error matches any of the predefined patterns
	for pattern, info := range errorCodeMap {
		if strings.Contains(err.Error(), pattern) {
			return info.Code, info.Message
		}
	}

	// Return a generic error code and the original error message if no match is found
	return ErrCodeUnknown, fmt.Sprintf("An error occurred: %s", err.Error())
}

// LogErrorWithCode logs the error with its code and human-friendly message.
func LogConnError(err error) {
	if err == nil {
		return
	}

	// Generalize the error message and get the error code
	code, message := generalizeErrorLogWithCode(err)

	// Log the error code and message
	if syslog.L != nil {
		syslog.L.Errorf("[%s] %s", code, message)
	} else {
		log.Printf("[%s] %s", code, message)
	}
}
