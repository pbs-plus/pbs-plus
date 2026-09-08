//go:build linux

package respond

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func TestStatusFromErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"backup not found → 404", coredb.ErrBackupNotFound, http.StatusNotFound},
		{"target not found → 404", coredb.ErrTargetNotFound, http.StatusNotFound},
		{"restore not found → 404", coredb.ErrRestoreNotFound, http.StatusNotFound},
		{"token not found → 404", coredb.ErrTokenNotFound, http.StatusNotFound},
		{"secret not found → 404", coredb.ErrSecretNotFound, http.StatusNotFound},
		{"agent host not found → 404", coredb.ErrAgentHostNotFound, http.StatusNotFound},
		{"bad request → 400", fmt.Errorf("wrapped: %w", ErrBadRequest), http.StatusBadRequest},
		{"unauthorized → 401", ErrUnauthorized, http.StatusUnauthorized},
		{"forbidden → 403", ErrForbidden, http.StatusForbidden},
		{"method not allowed → 405", ErrMethodNotAllowed, http.StatusMethodNotAllowed},
		{"one instance → 409", jobs.ErrOneInstance, http.StatusConflict},
		{"context canceled → 499", context.Canceled, 499},
		{"manager closed → 500", jobs.ErrManagerClosed, http.StatusInternalServerError},
		{"unknown error → 500", errors.New("unknown"), http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := statusFromErr(tt.err)
			if got != tt.want {
				t.Errorf("statusFromErr(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

func TestEnvelopeMatchesWireStatus(t *testing.T) {
	tests := []struct {
		name    string
		write   func(w http.ResponseWriter, r *http.Request)
		want    int
		message string
	}{
		{
			name:    "Error",
			write:   func(w http.ResponseWriter, r *http.Request) { Error(w, http.StatusBadRequest, errors.New("boom")) },
			want:    http.StatusBadRequest,
			message: "boom",
		},
		{
			name:    "BadRequest",
			write:   func(w http.ResponseWriter, r *http.Request) { BadRequest(w, "missing %s", "name") },
			want:    http.StatusBadRequest,
			message: "missing name",
		},
		{
			name:    "NotFound",
			write:   func(w http.ResponseWriter, r *http.Request) { NotFound(w, "no such outpost") },
			want:    http.StatusNotFound,
			message: "no such outpost",
		},
		{
			name:    "MethodNotAllowed",
			write:   MethodNotAllowed,
			want:    http.StatusMethodNotAllowed,
			message: "invalid HTTP method: DELETE not allowed on /api2/extjs/config/d2d-outposts",
		},
		{
			name:    "WriteErrorResponse infers status",
			write:   func(w http.ResponseWriter, r *http.Request) { WriteErrorResponse(w, coredb.ErrTargetNotFound) },
			want:    http.StatusNotFound,
			message: coredb.ErrTargetNotFound.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.write(rec, httptest.NewRequest(http.MethodDelete, "/api2/extjs/config/d2d-outposts", nil))

			if rec.Code != tt.want {
				t.Errorf("wire status = %d, want %d", rec.Code, tt.want)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}

			var got ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
			}
			if got.Success {
				t.Error("success = true, want false")
			}
			if got.Status != tt.want {
				t.Errorf("envelope status = %d, want %d", got.Status, tt.want)
			}
			if got.Message != tt.message {
				t.Errorf("message = %q, want %q", got.Message, tt.message)
			}
		})
	}
}

func TestFieldErrorsCarriesFormFields(t *testing.T) {
	rec := httptest.NewRecorder()
	FieldErrors(rec, http.StatusBadRequest, errors.New("invalid outpost"), map[string]string{
		"valid-users": "host is not joined to a domain",
	})

	var got ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if got.Errors["valid-users"] != "host is not joined to a domain" {
		t.Errorf("errors = %v, want valid-users entry", got.Errors)
	}
}
