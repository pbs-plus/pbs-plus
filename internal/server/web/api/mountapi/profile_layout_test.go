//go:build linux

package mountapi

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
)

func TestProfileLayoutCRUDAndValidation(t *testing.T) {
	old := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = old })
	t.Setenv("PROFILE_LAYOUT_TOKEN", "never-return-this-token")
	app := &application.Runtime{}
	create := url.Values{"datastore": {"store"}, "ns": {"Media"}, "mode": {"ro"}, "layout": {`{"template":"{resolve}/{ns.leaf}","selection":"namespace","resolver_url":"https://resolver.example/paths","resolver_token_env":"PROFILE_LAYOUT_TOKEN","windows_names":true}`}}
	r, w := profileRequest(http.MethodPost, "/profiles", create)
	ExtJsMountProfilesHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	id := (snapshotmount.Profile{Datastore: "store", Namespace: "Media"}).ID()
	get := func() snapshotmount.Profile {
		t.Helper()
		p, found, err := snapshotmount.LoadProfile(id)
		if err != nil || !found {
			t.Fatalf("load profile: %v, found %v", err, found)
		}
		return p
	}
	if l := get().Layout; l == nil || !l.WindowsNames || l.Selection != "namespace" {
		t.Fatalf("layout not saved: %+v", l)
	}
	if strings.Contains(w.Body.String(), "never-return-this-token") {
		t.Fatal("API exposed bearer token")
	}
	r, w = profileRequestID(http.MethodPut, id, url.Values{"mode": {"ro"}, "schedule": {"daily"}})
	ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusOK || get().Layout == nil {
		t.Fatalf("omitted layout was lost: %d %s", w.Code, w.Body)
	}
	r, w = profileRequestID(http.MethodPut, id, url.Values{"mode": {"ro"}, "layout": {""}})
	ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusOK || get().Layout != nil {
		t.Fatalf("layout was not cleared: %d %s", w.Code, w.Body)
	}
	for _, raw := range []string{`{"template":"../escape"}`, `{"template":"{unknown}"}`, `{"template":"{resolve}"}`, `{"selection":"all"}`, `[]`, `{`} {
		r, w = profileRequestID(http.MethodPut, id, url.Values{"mode": {"ro"}, "layout": {raw}})
		ExtJsMountProfileSingleHandler(app).ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("layout %s: status %d", raw, w.Code)
		}
	}
	r, w = profileRequest(http.MethodGet, "/preview", nil)
	ExtJsMountProfilePreviewHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("preview GET: %d", w.Code)
	}
	r, w = profileRequest(http.MethodPost, "/preview", url.Values{"layout": {"{"}})
	ExtJsMountProfilePreviewHandler(app).ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid preview: %d", w.Code)
	}
}
