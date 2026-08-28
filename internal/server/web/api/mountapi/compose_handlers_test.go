//go:build linux

package mountapi

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func composeRequest(datastore string, values url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api2/extjs/config/d2d-compose/"+encodeDatastore(datastore), strings.NewReader(values.Encode()))
	r.SetPathValue("datastore", encodeDatastore(datastore))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func validComposeValues() url.Values {
	return url.Values{
		"ns":          {"ns/a"},
		"backup-type": {"host"},
		"backup-id":   {"src"},
		"backup-time": {"2026-01-02T03:04:05Z"},
		"file-name":   {"root.mpxar.didx"},
		"target-ns":   {"composed"},
		"target-type": {"host"},
		"target-id":   {"dst"},
		"paths":       {base64.StdEncoding.EncodeToString([]byte("/etc")) + "," + base64.StdEncoding.EncodeToString([]byte("/var/log/app.log"))},
	}
}

func TestParseComposeFormValid(t *testing.T) {
	r := composeRequest("ds1", validComposeValues())
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	f, err := parseComposeForm(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Paths) != 2 || f.Paths[0] != "/etc" || f.Paths[1] != "/var/log/app.log" {
		t.Fatalf("paths = %v", f.Paths)
	}
	if f.TargetType != "host" || f.TargetID != "dst" || f.TargetNS != "composed" {
		t.Fatalf("form = %+v", f)
	}
}

func TestParseComposeFormDefaultsTargetType(t *testing.T) {
	values := validComposeValues()
	values.Del("target-type")
	r := composeRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	f, err := parseComposeForm(r)
	if err != nil {
		t.Fatal(err)
	}
	if f.TargetType != "host" {
		t.Fatalf("target type = %q", f.TargetType)
	}
}

func TestParseComposeFormRejectsBadInput(t *testing.T) {
	badTime := validComposeValues()
	badTime.Set("backup-time", "not-a-time")
	ppxar := validComposeValues()
	ppxar.Set("file-name", "root.ppxar.didx")
	relativePath := validComposeValues()
	relativePath.Set("paths", base64.StdEncoding.EncodeToString([]byte("etc")))
	traversal := validComposeValues()
	traversal.Set("paths", base64.StdEncoding.EncodeToString([]byte("/etc/../root")))
	nonBase64 := validComposeValues()
	nonBase64.Set("paths", "!!not-base64!!")
	noPaths := validComposeValues()
	noPaths.Del("paths")
	emptyPaths := validComposeValues()
	emptyPaths.Set("paths", ",,")
	badTargetNS := validComposeValues()
	badTargetNS.Set("target-ns", "bad ns")

	cases := map[string]url.Values{
		"bad-time":      badTime,
		"ppxar":         ppxar,
		"relative":      relativePath,
		"traversal":     traversal,
		"non-base64":    nonBase64,
		"no-paths":      noPaths,
		"empty-paths":   emptyPaths,
		"bad-target-ns": badTargetNS,
	}
	for name, values := range cases {
		r := composeRequest("ds1", values)
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if _, err := parseComposeForm(r); err == nil {
			t.Errorf("%s: parseComposeForm succeeded, want error", name)
		}
	}
}

func TestParseComposeFormDedupesPaths(t *testing.T) {
	values := validComposeValues()
	enc := base64.StdEncoding.EncodeToString([]byte("/etc"))
	values.Set("paths", enc+","+enc)
	r := composeRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	f, err := parseComposeForm(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Paths) != 1 {
		t.Fatalf("paths = %v, want 1", f.Paths)
	}
}

func TestComposeHandlerRejectsBadInput(t *testing.T) {
	values := validComposeValues()
	values.Set("file-name", "root.ppxar.didx")
	req := composeRequest("ds1", values)
	rec := httptest.NewRecorder()

	ExtJsComposeHandler(nil)(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}
