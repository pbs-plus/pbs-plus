//go:build linux

package s3repoapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/s3repo"
)

func request(method, path string, values url.Values) (*http.Request, *httptest.ResponseRecorder) {
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

func singleRequest(method, name string, values url.Values) (*http.Request, *httptest.ResponseRecorder) {
	r, w := request(method, "/api2/extjs/config/d2d-s3-repositories/"+name, values)
	r.SetPathValue("name", name)
	return r, w
}

func TestRepositoryHandlers(t *testing.T) {
	old := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() {
		s3repo.StopAll()
		conf.StatePrefix = old
	})

	config := `{"region":"us-east-1","tls":false,"buckets":[{"name":"mariadb","datastore":"backup","backup_type":"host","backup_id":"mariadb"},{"name":"postgres","datastore":"backup","namespace":"databases","backup_type":"host","backup_id":"postgres"}],"credentials":[{"access_key":"operator","secret_key":"operator-secret","auth_id":"backup@pbs!s3","grants":[{"bucket":"mariadb","read":true,"write":true,"delete":true}]},{"access_key":"postgres-operator","secret_key":"postgres-secret","auth_id":"backup@pbs!postgres","grants":[{"bucket":"postgres","read":true,"write":true}]}]}`

	r, w := request(http.MethodPost, "/api2/extjs/config/d2d-s3-repositories", url.Values{"name": {"edge-s3"}, "listen-addr": {"127.0.0.1:0"}, "config": {config}})
	ListHandler()(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d body=%s", w.Code, w.Body.String())
	}

	r, w = request(http.MethodGet, "/api2/extjs/config/d2d-s3-repositories", nil)
	ListHandler()(w, r)
	var list struct {
		Data []repositoryView `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if len(list.Data) != 1 {
		t.Fatalf("list = %+v", list.Data)
	}
	view := list.Data[0]
	if view.Config.Region != "us-east-1" || len(view.Config.Buckets) != 2 || view.Config.Buckets[0].Name != "mariadb" {
		t.Fatalf("view config = %+v", view.Config)
	}
	if len(view.Config.Credentials) != 2 || view.Config.Credentials[0].SecretKey != "" {
		t.Fatalf("secrets leaked: %+v", view.Config.Credentials)
	}
	if !view.Running || len(view.Endpoints) != 2 || !strings.HasPrefix(view.Endpoints[0], "http://") {
		t.Fatalf("view runtime = running %v endpoints %v error %q", view.Running, view.Endpoints, view.Error)
	}

	redacted := strings.ReplaceAll(config, `"secret_key":"operator-secret"`, `"secret_key":""`)
	redacted = strings.ReplaceAll(redacted, `"secret_key":"postgres-secret"`, `"secret_key":""`)
	r, w = singleRequest(http.MethodPut, "edge-s3", url.Values{"listen-addr": {"127.0.0.1:0"}, "config": {redacted}})
	SingleHandler()(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d body=%s", w.Code, w.Body.String())
	}
	loaded, ok, err := s3repo.Load("edge-s3")
	if err != nil || !ok || loaded.Config.Credentials[0].SecretKey != "operator-secret" || loaded.Config.Credentials[1].SecretKey != "postgres-secret" {
		t.Fatalf("updated credentials = %+v, ok=%v, err=%v", loaded.Config.Credentials, ok, err)
	}

	for name, values := range map[string]url.Values{
		"malformed json": {"listen-addr": {"127.0.0.1:0"}, "config": {"{not json"}},
		"missing config": {"listen-addr": {"127.0.0.1:0"}},
		"rename":         {"name": {"other"}, "listen-addr": {"127.0.0.1:0"}, "config": {config}},
	} {
		r, w = singleRequest(http.MethodPut, "edge-s3", values)
		SingleHandler()(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d body=%s", name, w.Code, w.Body.String())
		}
	}

	r, w = singleRequest(http.MethodDelete, "edge-s3", nil)
	SingleHandler()(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d", w.Code)
	}
	if _, ok, _ := s3repo.Load("edge-s3"); ok {
		t.Fatal("repository survived delete")
	}
}
