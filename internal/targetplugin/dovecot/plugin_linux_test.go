//go:build linux

package dovecot

import (
	"context"
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
	const want = "88c189e575ff3537d7ea409bdee31cb55f88fd7ad9e920c6ea49faa0cee3c5ae"
	if digest != want {
		t.Fatalf("schema digest = %s, want %s (changing the forms needs a schema version bump)", digest, want)
	}
}

func TestValidateNormalizesConnection(t *testing.T) {
	target := targetplugin.TargetInput{
		Config:  targetplugin.Values{hostField: targetplugin.NewStringScalar("mail.example")},
		Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
	}
	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetValidateRequest{Operation: operation(), Target: target})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := Handlers()[targetplugin.MethodTargetValidate](context.Background(), payload)
	if err != nil {
		t.Fatalf("target.validate: %v", err)
	}
	validated, ok := response.(targetplugin.TargetValidateResponse)
	if !ok {
		t.Fatalf("response = %T", response)
	}
	port, _ := validated.Config[portField].IntegerValue()
	if port != defaultPort {
		t.Fatalf("normalized port = %d, want %d", port, defaultPort)
	}
}

func TestValidateRejectsIncompleteTargets(t *testing.T) {
	tests := []struct {
		name     string
		target   targetplugin.TargetInput
		wantText string
	}{
		{
			name:     "missing host",
			target:   targetplugin.TargetInput{Secrets: targetplugin.Secrets{passwordField: []byte("secret")}},
			wantText: "host is required",
		},
		{
			name: "missing password",
			target: targetplugin.TargetInput{Config: targetplugin.Values{
				hostField: targetplugin.NewStringScalar("mail.example"),
			}},
			wantText: "password is required",
		},
		{
			name: "port out of range",
			target: targetplugin.TargetInput{
				Config: targetplugin.Values{
					hostField: targetplugin.NewStringScalar("mail.example"),
					portField: targetplugin.NewIntegerScalar(70000),
				},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			wantText: "invalid port",
		},
		{
			name: "relative CA certificate",
			target: targetplugin.TargetInput{
				Config: targetplugin.Values{
					hostField:   targetplugin.NewStringScalar("mail.example"),
					caCertField: targetplugin.NewStringScalar("certs/dovecot.pem"),
				},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			wantText: "CA certificate path must be absolute",
		},
		{
			name: "relative client directory",
			target: targetplugin.TargetInput{
				Config: targetplugin.Values{
					hostField:      targetplugin.NewStringScalar("mail.example"),
					clientDirField: targetplugin.NewStringScalar("dovecot/bin"),
				},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			wantText: "client directory must be absolute",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := targetplugin.MarshalProtocol(targetplugin.TargetValidateRequest{
				Operation: operation(), Target: test.target,
			})
			if err != nil {
				t.Fatalf("MarshalProtocol: %v", err)
			}
			if _, err := Handlers()[targetplugin.MethodTargetValidate](context.Background(), payload); err == nil ||
				!strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("target.validate error = %v, want %q", err, test.wantText)
			}
		})
	}
}

func TestProbeReportsUnreachableTargets(t *testing.T) {
	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetProbeRequest{
		Operation: operation(),
		Target: targetplugin.TargetInput{Config: targetplugin.Values{
			hostField: targetplugin.NewStringScalar("127.0.0.1"),
			portField: targetplugin.NewIntegerScalar(1),
		}},
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := Handlers()[targetplugin.MethodTargetProbe](context.Background(), payload)
	if err != nil {
		t.Fatalf("target.probe: %v", err)
	}
	probed, ok := response.(targetplugin.TargetProbeResponse)
	if !ok || probed.Available || probed.Message == "" {
		t.Fatalf("probe = %#v", response)
	}
}

func TestRestoreOpenRejectsForeignArchives(t *testing.T) {
	payload, err := targetplugin.MarshalProtocol(targetplugin.RestoreOpenRequest{
		Operation: operation(),
		Job: targetplugin.JobInput{
			Target: targetplugin.TargetInput{
				Config:  targetplugin.Values{hostField: targetplugin.NewStringScalar("mail.example")},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			Workspace: t.TempDir(), JobID: "job-1", CancellationID: "run-1",
		},
		Archive: targetplugin.Archive{Type: "filesystem", FormatVersion: 1},
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	if _, err := Handlers()[targetplugin.MethodRestoreOpen](context.Background(), payload); err == nil ||
		!strings.Contains(err.Error(), "was not written by this plugin") {
		t.Fatalf("restore.open error = %v", err)
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
		SchemaVersion:     targetSchemaVersion,
		BrokerToken:       make([]byte, 32),
	}
}
