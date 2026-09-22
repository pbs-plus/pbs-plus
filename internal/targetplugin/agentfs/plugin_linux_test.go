//go:build linux

package agentfs

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
	const want = "048e5aa74696087117e9cb96d30d02916c73045ddbee191e70db20ae82145143"
	if digest != want {
		t.Fatalf("schema digest = %s, want %s (changing the forms needs a schema version bump)", digest, want)
	}
}

func TestVolumeRootMatchesLegacyAgentPaths(t *testing.T) {
	for _, test := range []struct {
		volume          string
		operatingSystem string
		want            string
	}{
		{volume: "root", operatingSystem: "linux", want: "/"},
		{volume: "ROOT", operatingSystem: "windows", want: "/"},
		{volume: "C", operatingSystem: "windows", want: "c:\\"},
		{volume: "Data", operatingSystem: "linux", want: "data"},
	} {
		agent := agentConfig{hostname: "agent", volumeID: test.volume, operatingSystem: test.operatingSystem}
		if got := agent.volumeRoot(); got != test.want {
			t.Fatalf("volumeRoot(%q, %q) = %q, want %q", test.volume, test.operatingSystem, got, test.want)
		}
	}
}

func TestValidateNormalizesTargets(t *testing.T) {
	response, err := call[targetplugin.TargetValidateResponse](t, Handlers()[targetplugin.MethodTargetValidate],
		targetplugin.TargetValidateRequest{Operation: operation(), Target: targetplugin.TargetInput{Config: targetplugin.Values{
			hostnameField:        targetplugin.NewStringScalar("  agent.example  "),
			volumeField:          targetplugin.NewStringScalar("root"),
			operatingSystemField: targetplugin.NewStringScalar("windows"),
		}}})
	if err != nil {
		t.Fatalf("target.validate: %v", err)
	}
	if hostname, _ := response.Config[hostnameField].StringValue(); hostname != "agent.example" {
		t.Fatalf("normalized config = %#v", response.Config)
	}
}

func TestHandlersRejectUnusableTargets(t *testing.T) {
	handlers := Handlers()
	valid := targetplugin.Values{
		hostnameField:        targetplugin.NewStringScalar("agent.example"),
		volumeField:          targetplugin.NewStringScalar("root"),
		operatingSystemField: targetplugin.NewStringScalar("linux"),
	}
	for _, test := range []struct {
		name     string
		config   targetplugin.Values
		archive  targetplugin.Archive
		method   string
		wantText string
	}{
		{
			name:     "missing hostname",
			config:   targetplugin.Values{volumeField: targetplugin.NewStringScalar("root"), operatingSystemField: targetplugin.NewStringScalar("linux")},
			method:   targetplugin.MethodTargetValidate,
			wantText: "hostname is required",
		},
		{
			name: "unsupported operating system",
			config: targetplugin.Values{
				hostnameField: targetplugin.NewStringScalar("agent.example"), volumeField: targetplugin.NewStringScalar("root"),
				operatingSystemField: targetplugin.NewStringScalar("plan9"),
			},
			method:   targetplugin.MethodTargetValidate,
			wantText: "is not supported",
		},
		{
			name:     "foreign archive",
			config:   valid,
			archive:  targetplugin.Archive{Type: "filesystem", FormatVersion: 1},
			method:   targetplugin.MethodRestoreOpen,
			wantText: "was not written by this plugin",
		},
		{
			name:     "backup without a host broker",
			config:   valid,
			method:   targetplugin.MethodBackupOpen,
			wantText: "only available inside a plugin handler",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := targetplugin.TargetInput{Config: test.config}
			job := targetplugin.JobInput{Target: target, Workspace: t.TempDir(), JobID: "job-1", CancellationID: "run-1"}
			var request any
			switch test.method {
			case targetplugin.MethodRestoreOpen:
				request = targetplugin.RestoreOpenRequest{Operation: operation(), Job: job, Archive: test.archive}
			case targetplugin.MethodBackupOpen:
				request = targetplugin.BackupOpenRequest{Operation: operation(), Job: job}
			default:
				request = targetplugin.TargetValidateRequest{Operation: operation(), Target: target}
			}
			payload, err := targetplugin.MarshalProtocol(request)
			if err != nil {
				t.Fatalf("MarshalProtocol: %v", err)
			}
			if _, err := handlers[test.method](context.Background(), payload); err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("%s error = %v, want %q", test.method, err, test.wantText)
			}
		})
	}
}

func call[T any](t *testing.T, handler targetplugin.MethodHandler, request any) (T, error) {
	t.Helper()
	var decoded T
	payload, err := targetplugin.MarshalProtocol(request)
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	response, err := handler(context.Background(), payload)
	if err != nil {
		return decoded, err
	}
	encoded, err := targetplugin.MarshalProtocol(response)
	if err != nil {
		t.Fatalf("MarshalProtocol response: %v", err)
	}
	if err := targetplugin.UnmarshalProtocol(encoded, &decoded); err != nil {
		t.Fatalf("UnmarshalProtocol response: %v", err)
	}
	return decoded, nil
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
