//go:build linux

package mountapi

import (
	"net/http"
	"net/url"
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
