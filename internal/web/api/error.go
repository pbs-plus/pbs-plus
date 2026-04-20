//go:build linux

package api

import (
	"encoding/json"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type ErrorResponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
	Success bool   `json:"success"`
}

func WriteErrorResponse(w http.ResponseWriter, err error) {
	syslog.L.Error(err).Write()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&ErrorResponse{
		Message: err.Error(),
		Status:  http.StatusInternalServerError,
		Success: false,
	})
}
