//go:build linux

package s3

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const (
	PluginID             = "org.pbs-plus.s3"
	TargetType           = "s3"
	ArchiveType          = "s3"
	ArchiveFormatVersion = 1

	schemaVersion = 1

	endpointField  = "endpoint"
	bucketField    = "bucket"
	regionField    = "region"
	prefixField    = "prefix"
	accessKeyField = "access_key"
	secretKeyField = "secret_key"
	useSSLField    = "use_ssl"
	pathStyleField = "path_style"
)

// Version is the plugin release version reported to the host.
var Version = "1.0.0"

type Plugin struct {
	mu     sync.Mutex
	mount  *s3fs.S3FS
	ctx    context.Context
	cancel context.CancelFunc
}

func New() *Plugin {
	ctx, cancel := context.WithCancel(context.Background())
	return &Plugin{ctx: ctx, cancel: cancel}
}

// Descriptor is the identity and form contract this plugin serves.
func Descriptor() targetplugin.Descriptor {
	useSSL := targetplugin.NewBooleanScalar(true)
	pathStyle := targetplugin.NewBooleanScalar(false)
	return targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        PluginID,
		Version:         Version,
		TargetTypes:     []string{TargetType},
		TargetSchema: targetplugin.FormSchema{Version: schemaVersion, Fields: []targetplugin.FormField{
			{Key: endpointField, Label: "Endpoint", Control: targetplugin.ControlText, Required: true},
			{Key: bucketField, Label: "Bucket", Control: targetplugin.ControlText, Required: true},
			{Key: regionField, Label: "Region", Control: targetplugin.ControlText},
			{Key: prefixField, Label: "Prefix", Control: targetplugin.ControlText},
			{Key: accessKeyField, Label: "Access Key", Control: targetplugin.ControlText, Required: true},
			{Key: secretKeyField, Label: "Secret Key", Control: targetplugin.ControlSecret, Required: true},
			{Key: useSSLField, Label: "Use TLS", Control: targetplugin.ControlBoolean, Default: &useSSL},
			{Key: pathStyleField, Label: "Path-style Addressing", Control: targetplugin.ControlBoolean, Default: &pathStyle},
		}},
		BackupSchema:  targetplugin.FormSchema{Version: schemaVersion},
		RestoreSchema: targetplugin.FormSchema{Version: schemaVersion},
	}
}

func (plugin *Plugin) Handlers() map[string]targetplugin.MethodHandler {
	return map[string]targetplugin.MethodHandler{
		targetplugin.MethodPluginHealth:   health,
		targetplugin.MethodTargetValidate: validate,
		targetplugin.MethodTargetProbe:    probe,
		targetplugin.MethodBackupOpen:     plugin.backupOpen,
		targetplugin.MethodRestoreOpen:    restoreOpen,
	}
}

func (plugin *Plugin) Close() {
	plugin.mu.Lock()
	mount := plugin.mount
	plugin.mount = nil
	plugin.mu.Unlock()
	if mount != nil {
		mount.Unmount(context.Background())
	}
	plugin.cancel()
}

func health(_ context.Context, payload []byte) (any, error) {
	if _, err := targetplugin.Request[targetplugin.PluginHealthRequest](payload); err != nil {
		return nil, err
	}
	return targetplugin.PluginHealthResponse{Healthy: true}, nil
}

func validate(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.TargetValidateRequest](payload)
	if err != nil {
		return nil, err
	}
	config, err := normalizeConfig(request.Target.Config)
	if err != nil {
		return nil, err
	}
	if len(request.Target.Secrets[secretKeyField]) == 0 {
		return nil, errors.New("secret key is required")
	}
	return targetplugin.TargetValidateResponse{Config: config}, nil
}

func probe(ctx context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.TargetProbeRequest](payload)
	if err != nil {
		return nil, err
	}
	config, err := normalizeConfig(request.Target.Config)
	if err != nil {
		return nil, err
	}
	secret := string(request.Target.Secrets[secretKeyField])
	if secret == "" {
		return nil, errors.New("secret key is required")
	}
	client, bucket, err := client(config, secret)
	if err != nil {
		return nil, err
	}
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return targetplugin.TargetProbeResponse{Message: err.Error()}, nil
	}
	if !exists {
		return targetplugin.TargetProbeResponse{Message: fmt.Sprintf("bucket %q does not exist", bucket)}, nil
	}
	return targetplugin.TargetProbeResponse{Available: true}, nil
}

func restoreOpen(_ context.Context, payload []byte) (any, error) {
	if _, err := targetplugin.Request[targetplugin.RestoreOpenRequest](payload); err != nil {
		return nil, err
	}
	return nil, errors.New("S3 restore is not supported")
}

func (plugin *Plugin) backupOpen(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.BackupOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	config, err := normalizeConfig(request.Job.Target.Config)
	if err != nil {
		return nil, err
	}
	secret := string(request.Job.Target.Secrets[secretKeyField])
	if secret == "" {
		return nil, errors.New("secret key is required")
	}
	endpoint, _ := config[endpointField].StringValue()
	bucket, _ := config[bucketField].StringValue()
	region, _ := config[regionField].StringValue()
	prefix, _ := config[prefixField].StringValue()
	accessKey, _ := config[accessKeyField].StringValue()
	useSSL, _ := config[useSSLField].BooleanValue()
	pathStyle, _ := config[pathStyleField].BooleanValue()

	mountpoint := filepath.Join(request.Job.Workspace, "source")
	if err := os.MkdirAll(mountpoint, 0o700); err != nil {
		return nil, fmt.Errorf("create S3 mountpoint: %w", err)
	}
	fs := s3fs.NewS3FS(plugin.ctx, coredb.Backup{ID: request.Job.JobID}, endpoint,
		accessKey, secret, bucket, region, prefix, useSSL, pathStyle)
	if fs == nil {
		return nil, errors.New("initialize S3 filesystem")
	}
	if err := s3fs.MountS3(fs, mountpoint); err != nil {
		fs.Unmount(context.Background())
		return nil, fmt.Errorf("mount S3 filesystem: %w", err)
	}
	plugin.mu.Lock()
	plugin.mount = fs
	plugin.mu.Unlock()

	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		plugin.Close()
		return nil, fmt.Errorf("create lease token: %w", err)
	}
	return targetplugin.BackupOpenResponse{
		Kind:    targetplugin.SourceDirectory,
		Path:    mountpoint,
		Archive: targetplugin.Archive{Type: ArchiveType, FormatVersion: ArchiveFormatVersion},
		HostFeatures: []targetplugin.HostFeature{
			targetplugin.FeatureSubpath,
			targetplugin.FeatureExclusions,
			targetplugin.FeatureChangeDetection,
		},
		CleanupToken: token,
	}, nil
}

func normalizeConfig(config targetplugin.Values) (targetplugin.Values, error) {
	endpoint, ok := config[endpointField].StringValue()
	if !ok || strings.TrimSpace(endpoint) == "" {
		return nil, errors.New("endpoint is required")
	}
	if strings.Contains(endpoint, "://") {
		return nil, errors.New("S3 endpoint must not include a URL scheme")
	}
	bucket, ok := config[bucketField].StringValue()
	if !ok || strings.TrimSpace(bucket) == "" {
		return nil, errors.New("bucket is required")
	}
	bucket = strings.TrimSpace(bucket)
	if err := s3utils.CheckValidBucketName(bucket); err != nil {
		return nil, fmt.Errorf("invalid S3 bucket %q: %w", bucket, err)
	}
	accessKey, ok := config[accessKeyField].StringValue()
	if !ok || strings.TrimSpace(accessKey) == "" {
		return nil, errors.New("access key is required")
	}
	useSSL, ok := config[useSSLField].BooleanValue()
	if !ok {
		useSSL = true
	}
	pathStyle, _ := config[pathStyleField].BooleanValue()
	normalized := targetplugin.Values{
		endpointField:  targetplugin.NewStringScalar(strings.TrimSpace(endpoint)),
		bucketField:    targetplugin.NewStringScalar(bucket),
		accessKeyField: targetplugin.NewStringScalar(strings.TrimSpace(accessKey)),
		useSSLField:    targetplugin.NewBooleanScalar(useSSL),
		pathStyleField: targetplugin.NewBooleanScalar(pathStyle),
	}
	if region, ok := config[regionField].StringValue(); ok && strings.TrimSpace(region) != "" {
		normalized[regionField] = targetplugin.NewStringScalar(strings.TrimSpace(region))
	}
	if prefix, ok := config[prefixField].StringValue(); ok && strings.Trim(prefix, "/") != "" {
		normalized[prefixField] = targetplugin.NewStringScalar(strings.Trim(prefix, "/"))
	}
	if _, _, err := client(normalized, "validation-placeholder"); err != nil {
		return nil, err
	}
	return normalized, nil
}

func client(config targetplugin.Values, secret string) (*minio.Client, string, error) {
	endpoint, _ := config[endpointField].StringValue()
	bucket, _ := config[bucketField].StringValue()
	region, _ := config[regionField].StringValue()
	accessKey, _ := config[accessKeyField].StringValue()
	useSSL, _ := config[useSSLField].BooleanValue()
	pathStyle, _ := config[pathStyleField].BooleanValue()
	lookup := minio.BucketLookupDNS
	if pathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secret, ""),
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, "", fmt.Errorf("create S3 client: %w", err)
	}
	return client, bucket, nil
}
