//go:build linux

package objectstore

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMCRepro(t *testing.T) {
	if _, err := exec.LookPath("/tmp/mc"); err != nil {
		t.Skip("/tmp/mc not available")
	}
	handler, _, _ := newRoundTripHandler(t)
	handler.config.Region = "us-east-1"
	capture := &captureMiddleware{next: handler}
	server := httptest.NewServer(capture)
	t.Cleanup(server.Close)

	alias := fmt.Sprintf("http://%s", server.Listener.Addr())
	run := func(args ...string) (string, error) {
		cmd := exec.Command("/tmp/mc", args...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := run("alias", "set", "repro", alias, testAccessKey, testSecretKey); err != nil {
		t.Fatalf("alias set: %v\n%s", err, out)
	}
	t.Cleanup(func() { _, _ = run("alias", "remove", "repro") })

	if err := os.WriteFile("/tmp/mc-small.txt", []byte("mc-repro-payload\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := run("cp", "/tmp/mc-small.txt", "repro/mariadb/small.txt"); err != nil {
		t.Fatalf("mc cp: %v\n%s", err, out)
	}
	if out, err := run("cat", "repro/mariadb/small.txt"); err != nil {
		t.Fatalf("mc cat: %v\n%s", err, out)
	} else if out != "mc-repro-payload\n" {
		t.Fatalf("mc cat = %q", out)
	}
	if out, err := run("ls", "repro/mariadb"); err != nil {
		t.Fatalf("mc ls: %v\n%s", err, out)
	} else if !strings.Contains(out, "small.txt") {
		t.Fatalf("mc ls = %q", out)
	}
	out, err := run("rm", "repro/mariadb/small.txt")
	if err != nil {
		t.Fatalf("mc rm: %v\n%s", err, out)
	}
	if out, err := run("ls", "repro/mariadb"); err != nil || strings.Contains(out, "small.txt") {
		t.Fatalf("mc ls after rm = %q err=%v", out, err)
	}
}

type captureMiddleware struct {
	next     http.Handler
	response string
}

func (c *captureMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
	c.next.ServeHTTP(recorder, r)
	if recorder.status >= 300 && c.response == "" {
		c.response = fmt.Sprintf("%s %s -> %d: %s", r.Method, r.URL.Path, recorder.status, string(recorder.body[:min(len(recorder.body), 400)]))
	}
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	body   []byte
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	r.body = append(r.body, p...)
	return r.ResponseWriter.Write(p)
}
