//go:build linux

package controllers

import (
	"encoding/json"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

type ErrorReponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
	Success bool   `json:"success"`
}

func WriteErrorResponse(w http.ResponseWriter, err error) {
	s, logErr := syslog.InitializeLogger()
	if logErr == nil {
		s.Error(err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&ErrorReponse{
		Message: err.Error(),
		Status:  http.StatusInternalServerError,
		Success: false,
	})
}
