//go:build linux

package objectstore

import (
	"strings"
	"testing"
)

const (
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key-value"
)

func testConfig() Config {
	return Config{
		Region: "us-west-2",
		Buckets: []Bucket{
			{Name: "mariadb", Datastore: "backup", Namespace: "databases", BackupType: "host", BackupID: "mariadb"},
			{Name: "private", Datastore: "backup", BackupType: "host", BackupID: "private"},
		},
		Credentials: []Credential{{
			AccessKey: testAccessKey,
			SecretKey: testSecretKey,
			AuthID:    "backup@pbs!s3",
			Grants:    []Grant{{Bucket: "mariadb", Read: true, Write: true}},
		}},
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "valid"},
		{name: "default region", mutate: func(c *Config) { c.Region = "" }},
		{name: "invalid bucket", mutate: func(c *Config) { c.Buckets[0].Name = "Bad_Bucket" }, want: "bucket"},
		{name: "duplicate bucket", mutate: func(c *Config) { c.Buckets[1].Name = c.Buckets[0].Name }, want: "more than once"},
		{name: "invalid namespace", mutate: func(c *Config) { c.Buckets[0].Namespace = "../../etc" }, want: "namespace"},
		{name: "short secret", mutate: func(c *Config) { c.Credentials[0].SecretKey = "short" }, want: "at least 8"},
		{name: "unknown grant", mutate: func(c *Config) { c.Credentials[0].Grants[0].Bucket = "missing" }, want: "unknown bucket"},
		{name: "empty grant", mutate: func(c *Config) { c.Credentials[0].Grants[0] = Grant{Bucket: "mariadb"} }, want: "no permissions"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig()
			if test.mutate != nil {
				test.mutate(&config)
			}
			err := config.Validate()
			if test.want == "" && err != nil {
				t.Fatalf("Validate() = %v", err)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("Validate() = %v, want %q", err, test.want)
			}
		})
	}
}

func TestConfigRegionName(t *testing.T) {
	if got := (Config{}).RegionName(); got != DefaultRegion {
		t.Fatalf("RegionName() = %q, want %q", got, DefaultRegion)
	}
}
