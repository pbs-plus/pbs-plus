//go:build linux

package mountapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
)

func profileRequest(method, path string, values url.Values) (*http.Request, *httptest.ResponseRecorder) {
	var body strings.Reader
	if values != nil {
		body = *strings.NewReader(values.Encode())
	}
	r := httptest.NewRequest(method, path, &body)
	if values != nil {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	return r, httptest.NewRecorder()
}

func profileRequestID(method, id string, values url.Values) (*http.Request, *httptest.ResponseRecorder) {
	r, w := profileRequest(method, "/api2/extjs/config/d2d-mount-profiles/"+url.PathEscape(id), values)
	r.SetPathValue("id", id)
	return r, w
}

func decodeList(t *testing.T, w *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	var resp struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	return resp.Data
}

func TestProfileCRUD(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })
	app := &application.Runtime{}

	create := url.Values{
		"datastore": {"ds1"}, "ns": {"a"},
		"mode": {"rw"}, "mount-path": {"/mnt/p"}, "schedule": {"02:00"}, "auto-mount": {"1"}, "replace": {"1"},
	}
	r, w := profileRequest(http.MethodPost, "/api2/extjs/config/d2d-mount-profiles", create)
	ExtJsMountProfilesHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d body = %s", w.Code, w.Body.String())
	}
	var created struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create: %v body=%s", err, w.Body.String())
	}
	if created.Data["mode"] != "rw" || created.Data["schedule"] != "02:00" || created.Data["auto-mount"] != true || created.Data["replace"] != true {
		t.Fatalf("create data = %v", created.Data)
	}
	id, _ := created.Data["id"].(string)
	if id == "" {
		t.Fatal("no id in create response")
	}

	r, w = profileRequest(http.MethodPost, "/api2/extjs/config/d2d-mount-profiles", create)
	ExtJsMountProfilesHandler(app).ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Fatal("duplicate create accepted")
	}

	r, w = profileRequest(http.MethodGet, "/api2/extjs/config/d2d-mount-profiles", nil)
	ExtJsMountProfilesHandler(app).ServeHTTP(w, r)
	if len(decodeList(t, w)) != 1 {
		t.Fatalf("list body = %s", w.Body.String())
	}

	update := url.Values{
		"datastore": {"ds1"}, "ns": {"a"},
		"mode": {"ro"}, "mount-path": {""}, "schedule": {""}, "auto-mount": {"0"}, "replace": {"0"},
	}
	r, w = profileRequestID(http.MethodPut, id, update)
	ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d body = %s", w.Code, w.Body.String())
	}
	p, ok, err := snapshotmount.LoadProfile(id)
	if err != nil || !ok {
		t.Fatalf("load ok=%v err=%v", ok, err)
	}
	if p.Mode != "ro" || p.AutoMount || p.Replace || p.MountPath != "" || p.Schedule != "" || p.CreatedAt == 0 {
		t.Fatalf("updated profile = %+v", p)
	}

	badUpdate := url.Values{"mode": {"readwrite"}, "datastore": {"ds1"}, "ns": {"a"}}
	r, w = profileRequestID(http.MethodPut, id, badUpdate)
	ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Fatal("invalid update accepted")
	}

	r, w = profileRequestID(http.MethodDelete, id, nil)
	ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d", w.Code)
	}
	if _, ok, _ := snapshotmount.LoadProfile(id); ok {
		t.Fatal("profile survived delete")
	}

	r, w = profileRequestID(http.MethodDelete, id, nil)
	ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Fatal("delete of missing profile accepted")
	}
}

func TestProfileCreateValidation(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })
	app := &application.Runtime{}

	cases := []url.Values{
		{"datastore": {"../x"}},
		{"datastore": {"ds1"}, "ns": {"bad ns"}},
		{"datastore": {"ds1"}, "share-name": {"arch"}},
		{"datastore": {"ds1"}, "outpost": {"edge"}, "mount-path": {"/mnt/x"}},
		{"datastore": {"ds1"}, "mount-path": {"/tmp/x"}},
		{"datastore": {"ds1"}, "mode": {"rwx"}},
	}
	for _, values := range cases {
		r, w := profileRequest(http.MethodPost, "/api2/extjs/config/d2d-mount-profiles", values)
		ExtJsMountProfilesHandler(app).ServeHTTP(w, r)
		if w.Code == http.StatusOK {
			t.Errorf("create with %v accepted", values)
		}
	}
}
