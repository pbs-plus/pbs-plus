//go:build linux

package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestDescriptorIsStable(t *testing.T) {
	descriptor := Descriptor()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	digest, err := targetplugin.SchemaDigest(descriptor)
	if err != nil {
		t.Fatalf("SchemaDigest: %v", err)
	}
	const want = "3ff22ad1e0df4f4356b789ff51ebe26574470c66a7e65f11ccb0d79e615885e4"
	if digest != want {
		t.Fatalf("schema digest = %s, want %s (changing the forms needs a schema version bump)", digest, want)
	}
}

func TestValidateNormalizesDefaults(t *testing.T) {
	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetValidateRequest{
		Operation: operation(),
		Target: targetplugin.TargetInput{
			Config: targetplugin.Values{
				endpointField:  targetplugin.NewStringScalar("s3.example"),
				bucketField:    targetplugin.NewStringScalar("backups"),
				accessKeyField: targetplugin.NewStringScalar("access"),
			},
			Secrets: targetplugin.Secrets{secretKeyField: []byte("secret")},
		},
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := New().Handlers()[targetplugin.MethodTargetValidate](context.Background(), payload)
	if err != nil {
		t.Fatalf("target.validate: %v", err)
	}
	validated := response.(targetplugin.TargetValidateResponse)
	useSSL, _ := validated.Config[useSSLField].BooleanValue()
	pathStyle, _ := validated.Config[pathStyleField].BooleanValue()
	if !useSSL || pathStyle {
		t.Fatalf("normalized config = %#v", validated.Config)
	}
}

func TestValidateRejectsInvalidTargets(t *testing.T) {
	tests := []struct {
		name     string
		config   targetplugin.Values
		secrets  targetplugin.Secrets
		wantText string
	}{
		{name: "missing endpoint", config: targetplugin.Values{
			bucketField: targetplugin.NewStringScalar("backups"), accessKeyField: targetplugin.NewStringScalar("access"),
		}, secrets: targetplugin.Secrets{secretKeyField: []byte("secret")}, wantText: "endpoint is required"},
		{name: "invalid bucket", config: targetplugin.Values{
			endpointField: targetplugin.NewStringScalar("s3.example"), bucketField: targetplugin.NewStringScalar("bad/bucket"),
			accessKeyField: targetplugin.NewStringScalar("access"),
		}, secrets: targetplugin.Secrets{secretKeyField: []byte("secret")}, wantText: "invalid S3 bucket"},
		{name: "missing access key", config: targetplugin.Values{
			endpointField: targetplugin.NewStringScalar("s3.example"), bucketField: targetplugin.NewStringScalar("backups"),
		}, secrets: targetplugin.Secrets{secretKeyField: []byte("secret")}, wantText: "access key is required"},
		{name: "missing secret key", config: targetplugin.Values{
			endpointField: targetplugin.NewStringScalar("s3.example"), bucketField: targetplugin.NewStringScalar("backups"),
			accessKeyField: targetplugin.NewStringScalar("access"),
		}, wantText: "secret key is required"},
		{name: "endpoint with scheme", config: targetplugin.Values{
			endpointField: targetplugin.NewStringScalar("https://s3.example"), bucketField: targetplugin.NewStringScalar("backups"),
			accessKeyField: targetplugin.NewStringScalar("access"),
		}, secrets: targetplugin.Secrets{secretKeyField: []byte("secret")}, wantText: "must not include a URL scheme"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := targetplugin.MarshalProtocol(targetplugin.TargetValidateRequest{
				Operation: operation(), Target: targetplugin.TargetInput{Config: test.config, Secrets: test.secrets},
			})
			if err != nil {
				t.Fatalf("MarshalProtocol: %v", err)
			}
			if _, err := New().Handlers()[targetplugin.MethodTargetValidate](context.Background(), payload); err == nil ||
				!strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("target.validate error = %v, want %q", err, test.wantText)
			}
		})
	}
}

func TestRestoreRemainsUnsupported(t *testing.T) {
	payload, err := targetplugin.MarshalProtocol(targetplugin.RestoreOpenRequest{
		Operation: operation(),
		Job: targetplugin.JobInput{
			Target: targetplugin.TargetInput{Config: targetplugin.Values{
				endpointField:  targetplugin.NewStringScalar("s3.example"),
				bucketField:    targetplugin.NewStringScalar("backups"),
				accessKeyField: targetplugin.NewStringScalar("access"),
			}},
			Workspace: t.TempDir(), JobID: "job-1", CancellationID: "run-1",
		},
		Archive: targetplugin.Archive{Type: ArchiveType, FormatVersion: ArchiveFormatVersion},
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	if _, err := New().Handlers()[targetplugin.MethodRestoreOpen](context.Background(), payload); err == nil ||
		!strings.Contains(err.Error(), "S3 restore is not supported") {
		t.Fatalf("restore.open error = %v", err)
	}
}

func TestProbeChecksBucket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetProbeRequest{
		Operation: operation(),
		Target: targetplugin.TargetInput{
			Config: targetplugin.Values{
				endpointField:  targetplugin.NewStringScalar(strings.TrimPrefix(server.URL, "http://")),
				bucketField:    targetplugin.NewStringScalar("backups"),
				accessKeyField: targetplugin.NewStringScalar("access"),
				regionField:    targetplugin.NewStringScalar("us-east-1"),
				useSSLField:    targetplugin.NewBooleanScalar(false),
				pathStyleField: targetplugin.NewBooleanScalar(true),
			},
			Secrets: targetplugin.Secrets{secretKeyField: []byte("secret")},
		},
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := New().Handlers()[targetplugin.MethodTargetProbe](context.Background(), payload)
	if err != nil {
		t.Fatalf("target.probe: %v", err)
	}
	probed := response.(targetplugin.TargetProbeResponse)
	if !probed.Available {
		t.Fatalf("probe = %#v", probed)
	}
}

func operation() targetplugin.Operation {
	return targetplugin.Operation{
		ProtocolVersion:   targetplugin.CurrentProtocolVersion,
		ID:                "operation-1",
		IdempotencyKey:    "operation-1",
		DeadlineUnixMilli: time.Now().Add(time.Minute).UnixMilli(),
		PluginVersion:     Version,
		TargetType:        TargetType,
		SchemaVersion:     schemaVersion,
		BrokerToken:       make([]byte, 32),
	}
}
