//go:build linux

package web

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateAgentHostnameMatch(t *testing.T) {
	tests := []struct {
		name         string
		authHostname string
		bodyHostname string
		wantErr      bool
	}{
		{"match", "agent-a", "agent-a", false},
		{"mismatch", "agent-a", "agent-b", true},
		{"empty_auth", "", "agent-a", true},
		{"empty_body", "agent-a", "", true},
		{"both_empty", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAgentHostnameMatch(tt.authHostname, tt.bodyHostname)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for auth=%q body=%q", tt.authHostname, tt.bodyHostname)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for auth=%q body=%q: %v", tt.authHostname, tt.bodyHostname, err)
			}
		})
	}
}

func TestAuthHeaderPropagation(t *testing.T) {
	var capturedHeader string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeader = r.Header.Get("X-PBS-Authenticated-Agent")
		w.WriteHeader(http.StatusOK)
	})

	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set("X-PBS-Authenticated-Agent", "agent-test")
			next.ServeHTTP(w, r)
		})
	}

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	rec := httptest.NewRecorder()
	middleware(handler).ServeHTTP(rec, req)

	if capturedHeader != "agent-test" {
		t.Errorf("expected 'agent-test', got %q", capturedHeader)
	}
}

func TestHandlerRejectsHostnameMismatch(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHostname := r.Header.Get("X-PBS-Authenticated-Agent")
		bodyHostname := "agent-b"

		if authHostname != "" && authHostname != bodyHostname {
			http.Error(w, fmt.Sprintf("hostname mismatch: %q != %q", authHostname, bodyHostname), http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	// Mismatch
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	req.Header.Set("X-PBS-Authenticated-Agent", "agent-a")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden, got %d", rec.Code)
	}

	// Match
	req2 := httptest.NewRequest(http.MethodPost, "/test", nil)
	req2.Header.Set("X-PBS-Authenticated-Agent", "agent-b")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec2.Code)
	}

	// No auth header (passes through — only validate if set)
	req3 := httptest.NewRequest(http.MethodPost, "/test", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Errorf("expected 200 OK when no auth header, got %d", rec3.Code)
	}
}
