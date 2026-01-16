//go:build linux

package controllers

import (
	"encoding/json"
	"net/http"

	extHttp "gitlab.com/go-extension/http"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type ErrorReponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
	Success bool   `json:"success"`
}

func WriteErrorResponse(w http.ResponseWriter, err error) {
	syslog.L.Error(err)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&ErrorReponse{
		Message: err.Error(),
		Status:  http.StatusInternalServerError,
		Success: false,
	})
}

func WriteErrorResponseExt(w extHttp.ResponseWriter, err error) {
	syslog.L.Error(err)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&ErrorReponse{
		Message: err.Error(),
		Status:  http.StatusInternalServerError,
		Success: false,
	})
}
