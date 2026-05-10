//go:build linux

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type ErrorResponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
	Success bool   `json:"success"`
}

func statusFromErr(err error) int {
	if errors.Is(err, database.ErrBackupNotFound) ||
		errors.Is(err, database.ErrTargetNotFound) ||
		errors.Is(err, database.ErrRestoreNotFound) ||
		errors.Is(err, database.ErrTokenNotFound) ||
		errors.Is(err, database.ErrSecretNotFound) ||
		errors.Is(err, database.ErrAgentHostNotFound) {
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
	syslog.L.Error(err).Write()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(&ErrorResponse{
		Message: err.Error(),
		Status:  statusCode,
		Success: false,
	})
}
