//go:build linux

package objectstore

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/signer"
)

func TestHandlerS3Contract(t *testing.T) {
	handler, err := NewHandler(testConfig(), time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	handler.resolveDatastore = func(string) (string, error) { return t.TempDir(), nil }
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantBody   []string
		notBody    string
	}{
		{name: "list buckets", method: http.MethodGet, target: "/", wantStatus: http.StatusOK, wantBody: []string{"<Name>mariadb</Name>", "backup@pbs!s3"}, notBody: "<Name>private</Name>"},
		{name: "head bucket", method: http.MethodHead, target: "/mariadb", wantStatus: http.StatusOK},
		{name: "bucket location", method: http.MethodGet, target: "/mariadb?location", wantStatus: http.StatusOK, wantBody: []string{"<LocationConstraint", "us-west-2"}},
		{name: "unknown bucket", method: http.MethodHead, target: "/missing", wantStatus: http.StatusNotFound, wantBody: []string{"<Code>NoSuchBucket</Code>"}},
		{name: "ungranted bucket", method: http.MethodHead, target: "/private", wantStatus: http.StatusForbidden, wantBody: []string{"<Code>AccessDenied</Code>"}},
		{name: "bucket listing pending", method: http.MethodGet, target: "/mariadb", wantStatus: http.StatusNotImplemented, wantBody: []string{"<Code>NotImplemented</Code>"}},
		{name: "missing object", method: http.MethodGet, target: "/mariadb/dump.sql", wantStatus: http.StatusNotFound, wantBody: []string{"<Code>NoSuchKey</Code>"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := signedRequest(t, test.method, "http://s3.test"+test.target, testAccessKey, testSecretKey, "us-west-2")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			result := response.Result()
			defer result.Body.Close()
			body, err := io.ReadAll(result.Body)
			if err != nil {
				t.Fatal(err)
			}
			if result.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", result.StatusCode, test.wantStatus, body)
			}
			for _, want := range test.wantBody {
				if !strings.Contains(string(body), want) {
					t.Fatalf("body %q does not contain %q", body, want)
				}
			}
			if test.notBody != "" && strings.Contains(string(body), test.notBody) {
				t.Fatalf("body %q contains %q", body, test.notBody)
			}
		})
	}
}

func TestHandlerRejectsInvalidSignatures(t *testing.T) {
	handler, err := NewHandler(testConfig(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		request func(*testing.T) *http.Request
	}{
		{name: "unsigned", request: func(t *testing.T) *http.Request {
			return httptest.NewRequest(http.MethodHead, "http://s3.test/mariadb", nil)
		}},
		{name: "unknown access key", request: func(t *testing.T) *http.Request {
			return signedRequest(t, http.MethodHead, "http://s3.test/mariadb", "unknown-key", testSecretKey, "us-west-2")
		}},
		{name: "wrong secret", request: func(t *testing.T) *http.Request {
			return signedRequest(t, http.MethodHead, "http://s3.test/mariadb", testAccessKey, "another-secret-key", "us-west-2")
		}},
		{name: "wrong region", request: func(t *testing.T) *http.Request {
			return signedRequest(t, http.MethodHead, "http://s3.test/mariadb", testAccessKey, testSecretKey, "eu-west-1")
		}},
		{name: "tampered path", request: func(t *testing.T) *http.Request {
			request := signedRequest(t, http.MethodHead, "http://s3.test/mariadb", testAccessKey, testSecretKey, "us-west-2")
			request.URL.Path = "/private"
			return request
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, test.request(t))
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusForbidden, response.Body.String())
			}
		})
	}
}

func signedRequest(t *testing.T, method, target, accessKey, secretKey, region string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, target, nil)
	return signer.SignV4(*request, accessKey, secretKey, "", region)
}
