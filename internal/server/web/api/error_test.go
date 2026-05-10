//go:build linux

package api

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

func TestStatusFromErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"backup not found → 404", database.ErrBackupNotFound, http.StatusNotFound},
		{"target not found → 404", database.ErrTargetNotFound, http.StatusNotFound},
		{"restore not found → 404", database.ErrRestoreNotFound, http.StatusNotFound},
		{"token not found → 404", database.ErrTokenNotFound, http.StatusNotFound},
		{"secret not found → 404", database.ErrSecretNotFound, http.StatusNotFound},
		{"agent host not found → 404", database.ErrAgentHostNotFound, http.StatusNotFound},
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
