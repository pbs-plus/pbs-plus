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

func TestApplyTargetFormS3(t *testing.T) {
	form := url.Values{
		"name":          {"archive"},
		"kind":          {"s3"},
		"s3_endpoint":   {"s3.us-east-2.amazonaws.com"},
		"s3_region":     {"us-east-2"},
		"s3_access_key": {"backup-user"},
		"s3_bucket":     {"pbs-archive"},
		"s3_use_ssl":    {"true"},
		"s3_path_style": {"true"},
		"s3_secret_key": {"not-persisted-by-form-parser"},
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
	parsed, err := coredb.ParseS3Url(target.Path)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Endpoint != "s3.us-east-2.amazonaws.com" || parsed.Region != "us-east-2" || parsed.AccessKey != "backup-user" || parsed.Bucket != "pbs-archive" || !parsed.UseSSL || !parsed.IsPathStyle {
		t.Fatalf("parsed S3 target = %#v", parsed)
	}
}

func TestTargetResponseS3Fields(t *testing.T) {
	response := newTargetResponse(coredb.Target{
		Type: coredb.TargetTypeS3,
		S3Info: &coredb.S3Url{
			Endpoint: "minio.example.com:9000", Region: "us-east-1", AccessKey: "backup", Bucket: "archive", UseSSL: true, IsPathStyle: true,
		},
	})
	if response.S3Endpoint != "minio.example.com:9000" || response.S3Region != "us-east-1" || response.S3AccessKey != "backup" || response.S3Bucket != "archive" || !response.S3UseSSL || !response.S3PathStyle {
		t.Fatalf("S3 target response = %#v", response)
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
