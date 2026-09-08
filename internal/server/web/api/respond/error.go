//go:build linux

package respond

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

type ErrorResponse struct {
	Message string            `json:"message"`
	Errors  map[string]string `json:"errors,omitempty"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

var (
	ErrBadRequest       = errors.New("bad request")
	ErrUnauthorized     = errors.New("unauthorized")
	ErrForbidden        = errors.New("forbidden")
	ErrNotFound         = errors.New("not found")
	ErrMethodNotAllowed = errors.New("invalid HTTP method")
	ErrConflict         = errors.New("conflict")
)

func statusFromErr(err error) int {
	if errors.Is(err, ErrBadRequest) {
		return http.StatusBadRequest
	}

	if errors.Is(err, ErrUnauthorized) {
		return http.StatusUnauthorized
	}

	if errors.Is(err, ErrForbidden) {
		return http.StatusForbidden
	}

	if errors.Is(err, ErrMethodNotAllowed) {
		return http.StatusMethodNotAllowed
	}

	if errors.Is(err, ErrNotFound) ||
		errors.Is(err, coredb.ErrBackupNotFound) ||
		errors.Is(err, coredb.ErrTargetNotFound) ||
		errors.Is(err, coredb.ErrRestoreNotFound) ||
		errors.Is(err, coredb.ErrTokenNotFound) ||
		errors.Is(err, coredb.ErrSecretNotFound) ||
		errors.Is(err, coredb.ErrAgentHostNotFound) ||
		errors.Is(err, mtfdb.ErrNotFound) ||
		errors.Is(err, mtfdb.ErrInvalidID) ||
		errors.Is(err, mtfdb.ErrInvalidMapping) {
		return http.StatusNotFound
	}

	if errors.Is(err, ErrConflict) || errors.Is(err, jobs.ErrOneInstance) {
		return http.StatusConflict
	}

	if errors.Is(err, context.Canceled) {
		return 499
	}

	if errors.Is(err, jobs.ErrManagerClosed) {
		return http.StatusInternalServerError
	}

	return http.StatusInternalServerError
}

func WriteErrorResponse(w http.ResponseWriter, err error) {
	writeError(w, statusFromErr(err), err, nil)
}

func Error(w http.ResponseWriter, status int, err error) {
	writeError(w, status, err, nil)
}

func Errorf(w http.ResponseWriter, status int, format string, args ...any) {
	writeError(w, status, fmt.Errorf(format, args...), nil)
}

func FieldErrors(w http.ResponseWriter, status int, err error, fields map[string]string) {
	writeError(w, status, err, fields)
}

func BadRequest(w http.ResponseWriter, format string, args ...any) {
	writeError(w, http.StatusBadRequest, fmt.Errorf(format, args...), nil)
}

func NotFound(w http.ResponseWriter, format string, args ...any) {
	writeError(w, http.StatusNotFound, fmt.Errorf(format, args...), nil)
}

func MethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusMethodNotAllowed,
		fmt.Errorf("invalid HTTP method: %s not allowed on %s", r.Method, r.URL.Path), nil)
}

func writeError(w http.ResponseWriter, status int, err error, fields map[string]string) {
	if err == nil {
		err = errors.New(http.StatusText(status))
	}
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	log.Error(err, "")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if encErr := json.NewEncoder(w).Encode(&ErrorResponse{
		Message: err.Error(),
		Errors:  fields,
		Status:  status,
		Success: false,
	}); encErr != nil {
		log.Error(encErr, "")
	}
}
