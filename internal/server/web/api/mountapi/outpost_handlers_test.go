//go:build linux

package mountapi

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
)

func TestParseMountFormOutpost(t *testing.T) {
	values := url.Values{
		"backup-type": {"host"},
		"backup-id":   {"id1"},
		"backup-time": {"2026-01-02T03:04:05Z"},
		"file-name":   {"root.mpxar.didx"},
		"mode":        {"ro"},
		"outpost":     {"edge-nfs"},
	}
	r := formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	f, err := parseMountForm(r)
	if err != nil {
		t.Fatal(err)
	}
	if f.Outpost != "edge-nfs" || f.MountPath != "" {
		t.Fatalf("form = %+v", f)
	}

	values.Set("share-name", "restore-latest")
	r = formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	if f, err = parseMountForm(r); err != nil || f.ShareName != "restore-latest" {
		t.Fatalf("share-name form = %+v err = %v", f, err)
	}
	values.Set("share-name", "bad name")
	r = formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMountForm(r); err == nil {
		t.Fatal("invalid share name should be rejected")
	}
	values.Del("share-name")
	values.Set("outpost", "")
	values.Set("share-name", "restore-latest")
	r = formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMountForm(r); err == nil {
		t.Fatal("share-name without outpost should be rejected")
	}

	values.Set("mount-path", "/mnt/custom")
	r = formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMountForm(r); err == nil {
		t.Fatal("outpost combined with mount-path should be rejected")
	}

	values.Del("mount-path")
	values.Set("outpost", "Bad_Name")
	r = formRequest("ds1", values)
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	if _, err := parseMountForm(r); err == nil {
		t.Fatal("invalid outpost name should be rejected")
	}
}

func TestOutpostHandlersCRUD(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() {
		outpost.StopAll()
		conf.StatePrefix = old
	})

	app := (*application.Runtime)(nil)

	create := url.Values{"name": {"edge"}, "type": {"nfs"}, "listen-addr": {"127.0.0.1:0"}}
	r, w := profileRequest(http.MethodPost, "/api2/extjs/config/d2d-outposts", create)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d body=%s", w.Code, w.Body.String())
	}

	r, w = profileRequest(http.MethodGet, "/api2/extjs/config/d2d-outposts", nil)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d", w.Code)
	}
	list := decodeList(t, w)
	if len(list) != 1 || list[0]["name"] != "edge" || list[0]["running"] != true {
		t.Fatalf("list = %v", list)
	}

	r, w = profileRequest(http.MethodPost, "/api2/extjs/config/d2d-outposts", create)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("duplicate create status = %d", w.Code)
	}

	update := url.Values{"name": {"edge"}, "type": {"nfs"}, "listen-addr": {"127.0.0.1:0"}}
	r, w = profileRequest(http.MethodPut, "/api2/extjs/config/d2d-outposts/edge", update)
	r.SetPathValue("name", "edge")
	ExtJsOutpostSingleHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d body=%s", w.Code, w.Body.String())
	}

	r, w = profileRequest(http.MethodDelete, "/api2/extjs/config/d2d-outposts/edge", nil)
	r.SetPathValue("name", "edge")
	ExtJsOutpostSingleHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", w.Code, w.Body.String())
	}
	if _, ok, _ := outpost.LoadOutpost("edge"); ok {
		t.Fatal("outpost survived delete")
	}
}

func TestOutpostCreateRejectsInvalid(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() { conf.StatePrefix = old })

	app := (*application.Runtime)(nil)
	bad := url.Values{"name": {"edge"}, "type": {"carrier-pigeon"}, "listen-addr": {"127.0.0.1:0"}}
	r, w := profileRequest(http.MethodPost, "/api2/extjs/config/d2d-outposts", bad)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown type status = %d", w.Code)
	}

	bad = url.Values{"name": {"edge"}, "type": {"nfs"}, "listen-addr": {"not-an-addr"}}
	r, w = profileRequest(http.MethodPost, "/api2/extjs/config/d2d-outposts", bad)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad listen status = %d", w.Code)
	}
}

func TestOutpostHandlersS3(t *testing.T) {
	dir := t.TempDir()
	old := conf.StatePrefix
	conf.StatePrefix = dir
	t.Cleanup(func() {
		outpost.StopAll()
		conf.StatePrefix = old
	})

	app := (*application.Runtime)(nil)
	config := `{"region":"us-east-1","buckets":[{"name":"mariadb","datastore":"backup","backup_type":"host","backup_id":"mariadb"}],"credentials":[{"access_key":"operator","secret_key":"operator-secret","auth_id":"backup@pbs!s3","grants":[{"bucket":"mariadb","read":true,"write":true,"delete":true}]}]}`

	create := url.Values{"name": {"edge-s3"}, "type": {"s3"}, "listen-addr": {"127.0.0.1:0"}, "s3": {config}}
	r, w := profileRequest(http.MethodPost, "/api2/extjs/config/d2d-outposts", create)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d body=%s", w.Code, w.Body.String())
	}

	r, w = profileRequest(http.MethodGet, "/api2/extjs/config/d2d-outposts", nil)
	ExtJsOutpostsHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", w.Code, w.Body.String())
	}
	view := decodeList(t, w)
	if len(view) != 1 {
		t.Fatalf("view = %v", view)
	}
	if view[0]["type"] != "s3" {
		t.Fatalf("view type = %v", view[0]["type"])
	}
	s3, ok := view[0]["s3"].(map[string]any)
	if !ok || s3["region"] != "us-east-1" {
		t.Fatalf("view s3 = %v", view[0]["s3"])
	}
	buckets, _ := s3["buckets"].([]any)
	if len(buckets) != 1 || buckets[0].(map[string]any)["name"] != "mariadb" {
		t.Fatalf("view buckets = %v", s3["buckets"])
	}

	badJSON := url.Values{"name": {"edge-s3"}, "type": {"s3"}, "listen-addr": {"127.0.0.1:0"}, "s3": {"{not json"}}
	r, w = profileRequest(http.MethodPut, "/api2/extjs/config/d2d-outposts/edge-s3", badJSON)
	r.SetPathValue("name", "edge-s3")
	ExtJsOutpostSingleHandler(app)(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed s3 json status = %d body=%s", w.Code, w.Body.String())
	}

	missingConfig := url.Values{"name": {"edge-s3"}, "type": {"s3"}, "listen-addr": {"127.0.0.1:0"}}
	r, w = profileRequest(http.MethodPut, "/api2/extjs/config/d2d-outposts/edge-s3", missingConfig)
	r.SetPathValue("name", "edge-s3")
	ExtJsOutpostSingleHandler(app)(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "s3 config is required") {
		t.Fatalf("missing s3 config status = %d body=%s", w.Code, w.Body.String())
	}

	r, w = profileRequest(http.MethodDelete, "/api2/extjs/config/d2d-outposts/edge-s3", nil)
	r.SetPathValue("name", "edge-s3")
	ExtJsOutpostSingleHandler(app)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d", w.Code)
	}
	if _, ok, _ := outpost.LoadOutpost("edge-s3"); ok {
		t.Fatal("s3 outpost survived delete")
	}
}
