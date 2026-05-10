//go:build linux

package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"log/slog"
)

func TestRequestID_SetsHeader(t *testing.T) {
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := GetRequestID(r.Context())
		if id == "" {
			t.Error("expected non-empty request ID in context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rid := rec.Header().Get("X-Request-ID"); rid == "" {
		t.Error("expected X-Request-ID header in response")
	}
}

func TestRequestID_UniquePerRequest(t *testing.T) {
	ids := make(map[string]bool)
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := GetRequestID(r.Context())
		if ids[id] {
			t.Errorf("duplicate request ID: %s", id)
		}
		ids[id] = true
		w.WriteHeader(http.StatusOK)
	}))

	for range 10 {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	if len(ids) != 10 {
		t.Errorf("expected 10 unique IDs, got %d", len(ids))
	}
}

func TestGetRequestID_EmptyWithoutMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := GetRequestID(r.Context()); id != "" {
			t.Errorf("expected empty request ID without middleware, got %q", id)
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
}

func TestRecovery_CatchesPanic(t *testing.T) {
	handler := Recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	// Should not panic
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 status, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal server error") {
		t.Errorf("expected 'internal server error' in body, got %q", rec.Body.String())
	}
}

func TestRecovery_NormalRequestPassthrough(t *testing.T) {
	handler := Recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("expected 'ok', got %q", rec.Body.String())
	}
}

func TestRequestLogger_LogsRequest(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	output := buf.String()
	if !strings.Contains(output, "method=POST") {
		t.Errorf("log should contain method, got: %s", output)
	}
	if !strings.Contains(output, "path=/api/test") {
		t.Errorf("log should contain path, got: %s", output)
	}
	if !strings.Contains(output, "status=418") {
		t.Errorf("log should contain status 418, got: %s", output)
	}
	if !strings.Contains(output, "duration_ms=") {
		t.Errorf("log should contain duration, got: %s", output)
	}
}

func TestStatusRecorder_CapturesStatus(t *testing.T) {
	rec := &statusRecorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	rec.WriteHeader(http.StatusNotFound)

	if rec.status != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.status)
	}
}

func TestStatusRecorder_DefaultStatus(t *testing.T) {
	rec := &statusRecorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	rec.Write([]byte("hello"))

	if rec.status != http.StatusOK {
		t.Errorf("expected default 200, got %d", rec.status)
	}
}
