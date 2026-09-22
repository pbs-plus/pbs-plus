//go:build linux

package postgresql

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
	const want = "e4270d4cbe5f980b8ee6dbb73363bba718ae4a57e470650508803fce1d784811"
	if digest != want {
		t.Fatalf("schema digest = %s, want %s (changing the forms needs a schema version bump)", digest, want)
	}
}

func TestValidateNormalizesConnection(t *testing.T) {
	handlers := Handlers()
	target := targetplugin.TargetInput{
		Config: targetplugin.Values{
			hostField:     targetplugin.NewStringScalar("db.example"),
			usernameField: targetplugin.NewStringScalar("backup"),
		},
		Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
	}
	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetValidateRequest{Operation: operation(), Target: target})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := handlers[targetplugin.MethodTargetValidate](context.Background(), payload)
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
	handlers := Handlers()
	tests := []struct {
		name     string
		target   targetplugin.TargetInput
		wantText string
	}{
		{
			name: "missing host",
			target: targetplugin.TargetInput{
				Config:  targetplugin.Values{usernameField: targetplugin.NewStringScalar("backup")},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			wantText: "host is required",
		},
		{
			name: "missing username",
			target: targetplugin.TargetInput{
				Config:  targetplugin.Values{hostField: targetplugin.NewStringScalar("db.example")},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			wantText: "username is required",
		},
		{
			name: "missing password",
			target: targetplugin.TargetInput{Config: targetplugin.Values{
				hostField:     targetplugin.NewStringScalar("db.example"),
				usernameField: targetplugin.NewStringScalar("backup"),
			}},
			wantText: "password is required",
		},
		{
			name: "port out of range",
			target: targetplugin.TargetInput{
				Config: targetplugin.Values{
					hostField:     targetplugin.NewStringScalar("db.example"),
					usernameField: targetplugin.NewStringScalar("backup"),
					portField:     targetplugin.NewIntegerScalar(70000),
				},
				Secrets: targetplugin.Secrets{passwordField: []byte("secret")},
			},
			wantText: "invalid port",
		},
		{
			name: "relative client directory",
			target: targetplugin.TargetInput{
				Config: targetplugin.Values{
					hostField:      targetplugin.NewStringScalar("db.example"),
					usernameField:  targetplugin.NewStringScalar("backup"),
					clientDirField: targetplugin.NewStringScalar("postgres/bin"),
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
			if _, err := handlers[targetplugin.MethodTargetValidate](context.Background(), payload); err == nil ||
				!strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("target.validate error = %v, want %q", err, test.wantText)
			}
		})
	}
}

func TestTargetMigrateAddsConnectionDefaults(t *testing.T) {
	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetMigrateRequest{
		Operation:         operation(),
		FromSchemaVersion: 1,
		ToSchemaVersion:   targetSchemaVersion,
		Values: targetplugin.Values{
			hostField:     targetplugin.NewStringScalar("db.example"),
			usernameField: targetplugin.NewStringScalar("backup"),
		},
		SecretFields: []string{passwordField},
	})
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := Handlers()[targetplugin.MethodTargetMigrate](context.Background(), payload)
	if err != nil {
		t.Fatalf("target.migrate: %v", err)
	}
	migrated, ok := response.(targetplugin.TargetMigrateResponse)
	if !ok {
		t.Fatalf("response = %T", response)
	}
	if port, _ := migrated.Values[portField].IntegerValue(); port != defaultPort {
		t.Fatalf("migrated port = %d, want %d", port, defaultPort)
	}
	if mode, _ := migrated.Values[tlsModeField].StringValue(); mode != "prefer" {
		t.Fatalf("migrated TLS mode = %q, want prefer", mode)
	}
}

func TestProbeReportsUnreachableTargets(t *testing.T) {
	payload, err := targetplugin.MarshalProtocol(targetplugin.TargetProbeRequest{
		Operation: operation(),
		Target: targetplugin.TargetInput{Config: targetplugin.Values{
			hostField:     targetplugin.NewStringScalar("127.0.0.1"),
			usernameField: targetplugin.NewStringScalar("backup"),
			portField:     targetplugin.NewIntegerScalar(1),
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
				Config: targetplugin.Values{
					hostField:     targetplugin.NewStringScalar("db.example"),
					usernameField: targetplugin.NewStringScalar("backup"),
				},
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
