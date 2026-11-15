//go:build linux

package s3repo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

func TestMigrateLegacyOutposts(t *testing.T) {
	old := conf.StatePrefix
	conf.StatePrefix = t.TempDir()
	t.Cleanup(func() { conf.StatePrefix = old })

	legacyDir := filepath.Join(conf.StatePrefix, "outposts")
	if err := os.MkdirAll(legacyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"edge-s3.json":  `{"name":"edge-s3","type":"s3","listen_addr":"0.0.0.0:9000","s3":{"region":"us-east-1","buckets":[{"name":"mariadb","datastore":"backup","backup_type":"host","backup_id":"mariadb"}],"credentials":[{"access_key":"operator","secret_key":"operator-secret","auth_id":"backup@pbs!s3","grants":[{"bucket":"mariadb","read":true}]}]},"created_at":1700000000}`,
		"edge-nfs.json": `{"name":"edge-nfs","type":"nfs","listen_addr":"0.0.0.0:2049","created_at":1700000000}`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(legacyDir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := MigrateLegacyOutposts(); err != nil {
		t.Fatal(err)
	}

	repo, ok, err := Load("edge-s3")
	if err != nil || !ok {
		t.Fatalf("Load() ok=%v err=%v", ok, err)
	}
	if repo.ListenAddr != "0.0.0.0:9000" || repo.CreatedAt != 1700000000 || repo.Config.Credentials[0].SecretKey != "operator-secret" {
		t.Fatalf("migrated = %+v", repo)
	}
	if _, err := os.Stat(filepath.Join(legacyDir, "edge-s3.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy s3 outpost not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(legacyDir, "edge-nfs.json")); err != nil {
		t.Fatalf("nfs outpost touched: %v", err)
	}

	if err := MigrateLegacyOutposts(); err != nil {
		t.Fatalf("second run: %v", err)
	}
}
