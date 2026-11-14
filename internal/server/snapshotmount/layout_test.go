//go:build linux

package snapshotmount

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func layoutSnapshot(t *testing.T, root, ns, id, at string) {
	t.Helper()
	dir := filepath.Join(groupParentDir(root, ns, "host", id), at)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "root.mpxar.didx"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLayoutLegacyAndSelection(t *testing.T) {
	root := t.TempDir()
	layoutSnapshot(t, root, "Media/D-100", "scanner-a", "2026-01-01T00:00:00Z")
	layoutSnapshot(t, root, "Media/D-100", "scanner-b", "2026-02-01T00:00:00Z")
	layoutSnapshot(t, root, "Media/D-200", "scanner-a", "2026-03-01T00:00:00Z")
	p := Profile{Datastore: "store", Namespace: "Media"}
	plans, err := planProfileIn(t.Context(), p, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 3 || plans[0].Paths[0] != "D-100/host-scanner-a" || plans[2].Paths[0] != "D-200" {
		t.Fatalf("legacy plan: %+v", plans)
	}
	p.Layout = &PathLayout{Template: "{ns.leaf}", Selection: "namespace"}
	plans, err = planProfileIn(t.Context(), p, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 || plans[0].Group.BackupID != "scanner-b" || plans[0].Paths[0] != "D-100" {
		t.Fatalf("newest namespace plan: %+v", plans)
	}
	p.Layout.Selection = "group"
	if _, err := planProfileIn(t.Context(), p, root); err == nil {
		t.Fatal("colliding groups accepted")
	}
}

func TestLayoutRejectsCaseFoldedOverlaps(t *testing.T) {
	root := t.TempDir()
	layoutSnapshot(t, root, "Media/A", "scanner", "2026-01-01T00:00:00Z")
	layoutSnapshot(t, root, "Media/a/B", "scanner", "2026-01-01T00:00:00Z")
	p := Profile{Datastore: "store", Namespace: "Media", Layout: &PathLayout{Template: "{ns.rel}", WindowsNames: true}}
	if _, err := planProfileIn(t.Context(), p, root); err == nil {
		t.Fatal("case-folded parent/child overlap accepted")
	}
	p.Layout.Template = "CON/{ns.leaf}"
	plans, err := planProfileIn(t.Context(), p, root)
	if err != nil {
		t.Fatal(err)
	}
	if plans[0].Paths[0] != "_CON/A" {
		t.Fatalf("unsafe literal path: %v", plans[0].Paths)
	}
}

func TestLayoutHTTPFanoutAndFallback(t *testing.T) {
	root := t.TempDir()
	layoutSnapshot(t, root, "Media/D-100", "scanner", "2026-01-01T00:00:00Z")
	values := []string{"Project B", "Project A", "Project A"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Error("invalid resolver request")
		}
		var got map[string]any
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		if got["namespace"] != "Media/D-100" || got["archive"] != "root.mpxar.didx" {
			t.Errorf("request: %+v", got)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"values": values}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	p := Profile{Datastore: "store", Namespace: "Media", Layout: &PathLayout{Template: "{resolve}/{ns.leaf}", ResolverURL: server.URL}}
	plans, err := planProfileIn(t.Context(), p, root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plans[0].Paths, []string{"Project A/D-100", "Project B/D-100"}) {
		t.Fatalf("paths: %v", plans[0].Paths)
	}
	values = []string{}
	plans, err = planProfileIn(t.Context(), p, root)
	if err != nil {
		t.Fatal(err)
	}
	if plans[0].Paths[0] != "Uncategorized/D-100" {
		t.Fatalf("fallback: %v", plans[0].Paths)
	}
	p.Layout.Fallback = "Unassigned/{id}"
	plans, err = planProfileIn(t.Context(), p, root)
	if err != nil || plans[0].Paths[0] != "Unassigned/scanner" {
		t.Fatalf("custom fallback: %+v %v", plans, err)
	}
}

func TestLayoutResolverFailuresAndLimits(t *testing.T) {
	for _, body := range []string{`not json`, `{}`, `{"values":null}`, `{"values":[""]}`, `{"values":["` + strings.Repeat("a", 256) + `"]}`, strings.Repeat(" ", 65537), `{"values":[` + strings.Repeat(`"x",`, 64) + `"x"]}`} {
		t.Run(body[:min(12, len(body))], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if _, err := w.Write([]byte(body)); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			p := Profile{Layout: &PathLayout{ResolverURL: server.URL}}
			if _, err := resolveLayout(t.Context(), p, PlannedMount{}); err == nil {
				t.Fatal("invalid resolver response accepted")
			}
		})
	}
	for _, status := range []int{http.StatusInternalServerError, http.StatusFound} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "http://127.0.0.1:1/private")
			w.WriteHeader(status)
		}))
		p := Profile{Layout: &PathLayout{ResolverURL: server.URL}}
		if _, err := resolveLayout(t.Context(), p, PlannedMount{}); err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("resolver status error: %v", err)
		}
		server.Close()
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := resolveLayout(ctx, Profile{Layout: &PathLayout{ResolverURL: "http://127.0.0.1:1/?secret=hidden"}}, PlannedMount{}); err == nil || strings.Contains(err.Error(), "hidden") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestLayoutBearerEnvironment(t *testing.T) {
	t.Setenv("LAYOUT_TEST_TOKEN", "private-token")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("missing bearer token")
		}
		if _, err := w.Write([]byte(`{"values":[]}`)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	old := layoutHTTPClient
	layoutHTTPClient = server.Client()
	t.Cleanup(func() { layoutHTTPClient = old })
	p := Profile{Layout: &PathLayout{Template: "{resolve}/{ns.leaf}", ResolverURL: server.URL, ResolverTokenEnv: "LAYOUT_TEST_TOKEN"}}
	if err := validateLayout(p); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLayout(t.Context(), p, PlannedMount{}); err != nil {
		t.Fatal(err)
	}
	p.Layout.ResolverURL = "http://example.invalid"
	if err := validateLayout(p); err == nil {
		t.Fatal("bearer token over HTTP accepted")
	}
}

func TestLayoutValidationAndSanitizing(t *testing.T) {
	p := Profile{Layout: &PathLayout{Template: "{ns.leaf}", WindowsNames: true}}
	m := PlannedMount{Group: NamespaceGroup{Namespace: "Media/CON.txt", BackupID: "scanner"}}
	got, err := renderLayout(p.Layout.Template, layoutVariables(p, m, ""))
	if err != nil || got != "_CON.txt" {
		t.Fatalf("Windows-safe path = %q, %v", got, err)
	}
	for _, template := range []string{"../escape", "/absolute", "a/" + "/b", "a/../b", "{unknown}", "{id", "a\\b", "a/.", "a/hidden\x00"} {
		if _, err := renderLayout(template, layoutVariables(p, m, "")); err == nil {
			t.Errorf("accepted template %q", template)
		}
	}
	p.Mode = ModeRW
	if err := validateLayout(p); err == nil {
		t.Fatal("read-write layout accepted")
	}
	for _, l := range []PathLayout{
		{Template: "{resolve}"},
		{Template: "{id}", ResolverURL: "http://example.invalid"},
		{Template: "{resolve}", ResolverURL: "file:///tmp/config"},
		{Template: "{resolve}", ResolverURL: "https://user:pass@example.invalid"},
		{Template: "{resolve}", ResolverURL: "https://example.invalid", Fallback: "{resolve}"},
		{Template: "{id}", Selection: "all"},
	} {
		if err := validateLayout(Profile{Layout: &l}); err == nil {
			t.Errorf("accepted layout %+v", l)
		}
	}
}

func TestLocalAliasesUseOneMountAndDoNotReplaceFiles(t *testing.T) {
	root := t.TempDir()
	s := Session{SubPath: "Project A/D-100", MountPoint: filepath.Join(root, "Project A/D-100"), Aliases: []string{"Project B/D-100"}}
	if err := os.MkdirAll(s.MountPoint, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.MountPoint, "contents"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := localAliases(s, false); err != nil {
		t.Fatal(err)
	}
	if err := localAliases(s, false); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, s.Aliases[0], "contents")); err != nil || string(data) != "source" {
		t.Fatalf("alias: %s %v", data, err)
	}
	if err := localAliases(s, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, s.Aliases[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("alias not removed: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, s.Aliases[0]), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := localAliases(s, false); err == nil {
		t.Fatal("overwrote existing directory")
	}
}
