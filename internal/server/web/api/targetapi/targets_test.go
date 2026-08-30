//go:build linux

package targetapi

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestApplyTargetFormDatabase(t *testing.T) {
	form := url.Values{
		"name":                           {"reports-db"},
		"kind":                           {"postgresql"},
		"database_host":                  {"db.internal"},
		"database_port":                  {"5544"},
		"database_username":              {"backup"},
		"database_tls_mode":              {"verify-full"},
		"database_ca_certificate":        {"/etc/pbs-plus/db-ca.pem"},
		"database_default_client_dir":    {"/usr/lib/postgresql/17/bin"},
		"database_default_client_family": {"postgresql"},
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := req.ParseForm(); err != nil {
		t.Fatal(err)
	}

	var target coredb.Target
	if err := applyTargetForm(&target, req, true); err != nil {
		t.Fatal(err)
	}
	if target.Type != coredb.TargetTypePostgreSQL || target.DatabasePort != 5544 || target.DatabaseHost != "db.internal" || target.DatabaseDefaultClientDir != "/usr/lib/postgresql/17/bin" {
		t.Fatalf("target = %#v", target)
	}
}

func TestTargetResponsePreservesLegacyType(t *testing.T) {
	encoded, err := json.Marshal(newTargetResponse(coredb.Target{
		Name:   "remote-root",
		Type:   coredb.TargetTypeFilesystem,
		Access: coredb.FilesystemAccessAgent,
	}))
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got["target_type"] != "agent" || got["kind"] != "filesystem" || got["access"] != "agent" {
		t.Fatalf("target response = %s", encoded)
	}
}
