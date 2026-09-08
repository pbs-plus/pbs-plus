//go:build linux

// Package objectstore serves S3 requests backed by Proxmox Backup Server snapshots.
package objectstore

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

const DefaultRegion = "us-east-1"

var authIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+(?:![A-Za-z0-9._-]+)?$`)

type Config struct {
	Region      string       `json:"region,omitempty"`
	Buckets     []Bucket     `json:"buckets"`
	Credentials []Credential `json:"credentials"`
	TLSCertFile string       `json:"tls-cert,omitempty"`
	TLSKeyFile  string       `json:"tls-key,omitempty"`
}

type Bucket struct {
	Name       string `json:"name"`
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace,omitempty"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
}

type Credential struct {
	AccessKey string  `json:"access_key"`
	SecretKey string  `json:"secret_key"`
	AuthID    string  `json:"auth_id"`
	Grants    []Grant `json:"grants"`
}

type Grant struct {
	Bucket string `json:"bucket"`
	Read   bool   `json:"read,omitempty"`
	Write  bool   `json:"write,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

func (c Config) Validate() error {
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("s3 tls-cert and tls-key must be set together")
	}
	region := c.RegionName()
	if strings.ContainsAny(region, " /\\\t\r\n") {
		return fmt.Errorf("s3 region %q is invalid", region)
	}
	if len(c.Buckets) == 0 {
		return fmt.Errorf("s3 config needs at least one bucket")
	}
	buckets := make(map[string]struct{}, len(c.Buckets))
	for _, bucket := range c.Buckets {
		if err := s3utils.CheckValidBucketNameStrict(bucket.Name); err != nil {
			return fmt.Errorf("s3 bucket %q: %w", bucket.Name, err)
		}
		if _, exists := buckets[bucket.Name]; exists {
			return fmt.Errorf("s3 bucket %q is configured more than once", bucket.Name)
		}
		buckets[bucket.Name] = struct{}{}
		if err := validate.ValidateDatastore(bucket.Datastore); err != nil {
			return fmt.Errorf("s3 bucket %q: %w", bucket.Name, err)
		}
		if err := validate.ValidateNamespace(bucket.Namespace); err != nil {
			return fmt.Errorf("s3 bucket %q: %w", bucket.Name, err)
		}
		if err := validate.ValidateBackupType(bucket.BackupType); err != nil {
			return fmt.Errorf("s3 bucket %q: %w", bucket.Name, err)
		}
		if err := validate.ValidateBackupID(bucket.BackupID); err != nil {
			return fmt.Errorf("s3 bucket %q: %w", bucket.Name, err)
		}
	}
	if len(c.Credentials) == 0 {
		return fmt.Errorf("s3 config needs at least one credential")
	}
	accessKeys := make(map[string]struct{}, len(c.Credentials))
	for _, credential := range c.Credentials {
		if len(credential.AccessKey) < 3 || len(credential.AccessKey) > 128 || strings.ContainsAny(credential.AccessKey, " /\\\t\r\n") {
			return fmt.Errorf("s3 access key is invalid")
		}
		if _, exists := accessKeys[credential.AccessKey]; exists {
			return fmt.Errorf("s3 access key %q is configured more than once", credential.AccessKey)
		}
		accessKeys[credential.AccessKey] = struct{}{}
		if len(credential.SecretKey) < 8 {
			return fmt.Errorf("s3 credential %q needs a secret key of at least 8 characters", credential.AccessKey)
		}
		if !authIDPattern.MatchString(credential.AuthID) {
			return fmt.Errorf("s3 credential %q has invalid auth id %q", credential.AccessKey, credential.AuthID)
		}
		if len(credential.Grants) == 0 {
			return fmt.Errorf("s3 credential %q needs at least one bucket grant", credential.AccessKey)
		}
		grants := make(map[string]struct{}, len(credential.Grants))
		for _, grant := range credential.Grants {
			if _, exists := buckets[grant.Bucket]; !exists {
				return fmt.Errorf("s3 credential %q grants unknown bucket %q", credential.AccessKey, grant.Bucket)
			}
			if _, exists := grants[grant.Bucket]; exists {
				return fmt.Errorf("s3 credential %q grants bucket %q more than once", credential.AccessKey, grant.Bucket)
			}
			grants[grant.Bucket] = struct{}{}
			if !grant.Read && !grant.Write && !grant.Delete {
				return fmt.Errorf("s3 credential %q grant for bucket %q has no permissions", credential.AccessKey, grant.Bucket)
			}
		}
	}
	return nil
}

func (c Config) RegionName() string {
	if c.Region == "" {
		return DefaultRegion
	}
	return c.Region
}

func (c Config) credential(accessKey string) (Credential, bool) {
	for _, credential := range c.Credentials {
		if credential.AccessKey == accessKey {
			return credential, true
		}
	}
	return Credential{}, false
}

func (c Config) bucket(name string) (Bucket, bool) {
	for _, bucket := range c.Buckets {
		if bucket.Name == name {
			return bucket, true
		}
	}
	return Bucket{}, false
}

func (c Config) bucketNames(credential Credential) []string {
	names := make([]string, 0, len(credential.Grants))
	for _, grant := range credential.Grants {
		if grant.Read || grant.Write || grant.Delete {
			names = append(names, grant.Bucket)
		}
	}
	sort.Strings(names)
	return names
}

func (c Credential) grant(bucket string) (Grant, bool) {
	for _, grant := range c.Grants {
		if grant.Bucket == bucket {
			return grant, true
		}
	}
	return Grant{}, false
}

func (c Credential) canAccess(bucket string) bool {
	grant, ok := c.grant(bucket)
	return ok && (grant.Read || grant.Write || grant.Delete)
}

func (c Credential) canRead(bucket string) bool {
	grant, _ := c.grant(bucket)
	return grant.Read
}

func (c Credential) canWrite(bucket string) bool {
	grant, _ := c.grant(bucket)
	return grant.Write
}

func (c Credential) canDelete(bucket string) bool {
	grant, _ := c.grant(bucket)
	return grant.Delete
}
