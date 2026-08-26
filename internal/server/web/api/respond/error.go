//go:build linux

package respond

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

type ErrorResponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
	Success bool   `json:"success"`
}

func statusFromErr(err error) int {
	if errors.Is(err, coredb.ErrBackupNotFound) ||
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

	if errors.Is(err, jobs.ErrOneInstance) {
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
	statusCode := statusFromErr(err)
	log.Error(err, "")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(&ErrorResponse{
		Message: err.Error(),
		Status:  statusCode,
		Success: false,
	}); err != nil {
		log.Error(err, "")
	}
}
