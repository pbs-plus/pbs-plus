//go:build linux

package mountapi

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
)

func encodeDatastore(name string) string {
	return url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(name)))
}

func formRequest(datastore string, values url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api2/extjs/config/d2d-mount/"+encodeDatastore(datastore), strings.NewReader(values.Encode()))
	r.SetPathValue("datastore", encodeDatastore(datastore))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func TestParseMountFormFullParams(t *testing.T) {
	values := url.Values{
		"ns":          {"ns/a"},
		"backup-type": {"host"},
		"backup-id":   {"id1"},
		"backup-time": {"2026-01-02T03:04:05Z"},
		"file-name":   {"root.mpxar.didx"},
		"mode":        {"rw"},
		"mount-path":  {"/mnt/custom"},
	}
	r := formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	f, err := parseMountForm(r)
	if err != nil {
		t.Fatal(err)
	}
	if !f.hasBackupParams() || f.Mode != "rw" || f.MountPath != "/mnt/custom" {
		t.Fatalf("form = %+v", f)
	}
}

func TestParseMountFormMountPathOnly(t *testing.T) {
	r := formRequest("ds1", url.Values{"mount-path": {"/mnt/custom"}, "force": {"1"}})
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	f, err := parseMountForm(r)
	if err != nil {
		t.Fatal(err)
	}
	if f.hasBackupParams() {
		t.Fatalf("form = %+v", f)
	}
	if !f.Force {
		t.Fatal("force not parsed")
	}
}

func TestParseMountFormRejectsBadInput(t *testing.T) {
	cases := []url.Values{
		{"mount-path": {"/tmp/elsewhere"}},
		{"mount-path": {"/mnt"}},
		{"mode": {"readwrite"}},
		{"backup-time": {"not-a-time"}},
	}
	for _, values := range cases {
		r := formRequest("ds1", values)
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if _, err := parseMountForm(r); err == nil {
			t.Errorf("parseMountForm(%v) succeeded, want error", values)
		}
	}
}

func TestCommitHandlerNoSession(t *testing.T) {
	app := &application.Runtime{}
	r := formRequest("ds1", url.Values{"mount-path": {"/mnt/gone"}})
	w := httptest.NewRecorder()
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	ExtJsCommitHandler(app).ServeHTTP(w, r)
	if w.Code < 400 {
		t.Fatalf("status = %d, want >= 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "no mount session") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestInitHandlerRequiresBackupParams(t *testing.T) {
	req := formRequest("ds1", url.Values{"backup-type": {"host"}})
	rec := httptest.NewRecorder()

	ExtJsInitHandler(nil)(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing backup-id, got %d: %s", rec.Code, rec.Body.String())
	}
}
