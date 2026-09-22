package targetplugin

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetcherIndex(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/index.toml":
			if request.Header.Get("If-None-Match") == `"v1"` {
				writer.WriteHeader(http.StatusNotModified)
				return
			}
			writer.Header().Set("ETag", `"v1"`)
			writer.Header().Set("Last-Modified", "Tue, 14 Nov 2023 22:13:20 GMT")
			_, _ = io.WriteString(writer, "format_version = 1\n")
		case "/index.toml.sig":
			_, _ = io.WriteString(writer, "c2lnbmF0dXJl")
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fetcher := Fetcher{Client: server.Client()}
	document, err := fetcher.Index(t.Context(), server.URL+"/index.toml", PluginRepositoryCache{})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	if string(document.Index) != "format_version = 1\n" || string(document.Signature) != "c2lnbmF0dXJl" {
		t.Fatalf("document = %#v", document)
	}
	if document.ETag != `"v1"` || document.LastModified == "" {
		t.Fatalf("validators = %q, %q", document.ETag, document.LastModified)
	}

	_, err = fetcher.Index(t.Context(), server.URL+"/index.toml", PluginRepositoryCache{ETag: document.ETag})
	if !errors.Is(err, ErrRepositoryUnchanged) {
		t.Fatalf("conditional Index error = %v, want ErrRepositoryUnchanged", err)
	}
}

func TestFetcherRejectsOversizedAndFailedResponses(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/huge.toml":
			writer.Header().Set("ETag", `"huge"`)
			for range (MaxRepositoryIndexBytes / 1024) + 2 {
				_, _ = writer.Write(make([]byte, 1024))
			}
		case "/index.toml":
			_, _ = io.WriteString(writer, "format_version = 1\n")
		default:
			writer.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fetcher := Fetcher{Client: server.Client()}
	tests := []struct {
		name      string
		path      string
		wantError string
	}{
		{name: "oversized index", path: "/huge.toml", wantError: "exceeds"},
		{name: "missing signature", path: "/index.toml", wantError: "unexpected status"},
		{name: "server error", path: "/missing.toml", wantError: "unexpected status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := fetcher.Index(t.Context(), server.URL+test.path, PluginRepositoryCache{})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Index error = %v, want %q", err, test.wantError)
			}
		})
	}
	if _, err := fetcher.Index(t.Context(), "http://plugins.example.test/index.toml", PluginRepositoryCache{}); err == nil ||
		!strings.Contains(err.Error(), "absolute HTTPS") {
		t.Fatalf("plaintext Index error = %v", err)
	}
}

func TestFetcherManifestAndArtifact(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/plugins/manifest.toml":
			_, _ = io.WriteString(writer, "plugin_id = \"org.pbs-plus.test\"\n")
		case "/plugins/linux-amd64":
			_, _ = io.WriteString(writer, "artifact-bytes")
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fetcher := Fetcher{Client: server.Client()}
	indexURL := server.URL + "/plugins/index.toml"
	manifest, err := fetcher.Manifest(t.Context(), indexURL, "manifest.toml")
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if string(manifest) != "plugin_id = \"org.pbs-plus.test\"\n" {
		t.Fatalf("manifest = %q", manifest)
	}

	stream, err := fetcher.Artifact(t.Context(), indexURL, server.URL+"/plugins/linux-amd64")
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	defer stream.Close()
	artifact, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(artifact) != "artifact-bytes" {
		t.Fatalf("artifact = %q", artifact)
	}
	if _, err := fetcher.Artifact(t.Context(), indexURL, "missing"); err == nil ||
		!strings.Contains(err.Error(), "unexpected status") {
		t.Fatalf("missing Artifact error = %v", err)
	}
}

func TestResolveRepositoryURL(t *testing.T) {
	base := "https://plugins.example.test/repo/index.toml"
	tests := []struct {
		name      string
		reference string
		want      string
		wantError string
	}{
		{name: "relative file", reference: "artifacts/plugin", want: "https://plugins.example.test/repo/artifacts/plugin"},
		{name: "rooted path", reference: "/artifacts/plugin", want: "https://plugins.example.test/artifacts/plugin"},
		{name: "absolute https", reference: "https://cdn.example.test/plugin", want: "https://cdn.example.test/plugin"},
		{name: "plaintext", reference: "http://cdn.example.test/plugin", wantError: "absolute HTTPS"},
		{name: "protocol relative", reference: "/" + "/cdn.example.test/plugin", wantError: "not HTTPS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := ResolveRepositoryURL(base, test.reference)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("ResolveRepositoryURL error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveRepositoryURL: %v", err)
			}
			if resolved != test.want {
				t.Fatalf("resolved = %q, want %q", resolved, test.want)
			}
		})
	}
}
