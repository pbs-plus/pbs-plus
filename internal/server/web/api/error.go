//go:build linux

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
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
		errors.Is(err, database.ErrAgentHostNotFound) ||
		errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrInvalidID) ||
		errors.Is(err, store.ErrInvalidMapping) {
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
