//go:build linux

package filesystem

import (
	"context"
	"os"
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
	const want = "bda5854fb044ea679da6cb60e5cbd0f30a668627cad1b59881c6c9d10ffe576a"
	if digest != want {
		t.Fatalf("schema digest = %s, want %s (changing the forms needs a schema version bump)", digest, want)
	}
}

func TestHandlers(t *testing.T) {
	directory := t.TempDir()
	config := targetplugin.Values{pathField: targetplugin.NewStringScalar(directory)}
	job := targetplugin.JobInput{
		Target:         targetplugin.TargetInput{Config: config},
		Workspace:      directory,
		JobID:          "job-1",
		CancellationID: "run-1",
	}
	handlers := Handlers()

	validated, err := call[targetplugin.TargetValidateResponse](t, handlers[targetplugin.MethodTargetValidate],
		targetplugin.TargetValidateRequest{Operation: operation(), Target: job.Target})
	if err != nil {
		t.Fatalf("target.validate: %v", err)
	}
	if path, _ := validated.Config[pathField].StringValue(); path != directory {
		t.Fatalf("normalized config = %#v", validated.Config)
	}

	probed, err := call[targetplugin.TargetProbeResponse](t, handlers[targetplugin.MethodTargetProbe],
		targetplugin.TargetProbeRequest{Operation: operation(), Target: job.Target})
	if err != nil {
		t.Fatalf("target.probe: %v", err)
	}
	if !probed.Available || probed.Size == nil || probed.Size.Total == 0 || probed.Size.Free > probed.Size.Total {
		t.Fatalf("probe = %#v", probed)
	}

	lease, err := call[targetplugin.BackupOpenResponse](t, handlers[targetplugin.MethodBackupOpen],
		targetplugin.BackupOpenRequest{Operation: operation(), Job: job})
	if err != nil {
		t.Fatalf("backup.open: %v", err)
	}
	if lease.Kind != targetplugin.SourceDirectory || lease.Path != directory || lease.Archive.Type != ArchiveType {
		t.Fatalf("backup lease = %#v", lease)
	}
	if len(lease.HostFeatures) != 4 || len(lease.CleanupToken) == 0 {
		t.Fatalf("backup lease features = %#v", lease.HostFeatures)
	}

	destination, err := call[targetplugin.RestoreOpenResponse](t, handlers[targetplugin.MethodRestoreOpen],
		targetplugin.RestoreOpenRequest{
			Operation: operation(), Job: job,
			Archive: targetplugin.Archive{Type: ArchiveType, FormatVersion: ArchiveFormatVersion},
		})
	if err != nil {
		t.Fatalf("restore.open: %v", err)
	}
	if destination.Mode != targetplugin.RestoreModePath || destination.Path != directory {
		t.Fatalf("restore lease = %#v", destination)
	}
}

func TestHandlersRejectUnusableTargets(t *testing.T) {
	handlers := Handlers()
	file := t.TempDir() + "/file"
	if err := writeFile(file); err != nil {
		t.Fatalf("write file: %v", err)
	}

	tests := []struct {
		name     string
		config   targetplugin.Values
		archive  targetplugin.Archive
		method   string
		wantText string
	}{
		{
			name:     "relative path",
			config:   targetplugin.Values{pathField: targetplugin.NewStringScalar("data")},
			method:   targetplugin.MethodTargetValidate,
			wantText: "must be absolute",
		},
		{
			name:     "missing path",
			config:   targetplugin.Values{},
			method:   targetplugin.MethodTargetValidate,
			wantText: "target path is required",
		},
		{
			name:     "not a directory",
			config:   targetplugin.Values{pathField: targetplugin.NewStringScalar(file)},
			method:   targetplugin.MethodTargetValidate,
			wantText: "not a directory",
		},
		{
			name:     "foreign archive",
			config:   targetplugin.Values{pathField: targetplugin.NewStringScalar(t.TempDir())},
			archive:  targetplugin.Archive{Type: "database", FormatVersion: 1},
			method:   targetplugin.MethodRestoreOpen,
			wantText: "was not written by this plugin",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := targetplugin.TargetInput{Config: test.config}
			var request any
			if test.method == targetplugin.MethodRestoreOpen {
				request = targetplugin.RestoreOpenRequest{
					Operation: operation(),
					Job: targetplugin.JobInput{
						Target: target, Workspace: t.TempDir(), JobID: "job-1", CancellationID: "run-1",
					},
					Archive: test.archive,
				}
			} else {
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
		TargetType:        TargetTypeLocal,
		SchemaVersion:     schemaVersion,
		BrokerToken:       make([]byte, 32),
	}
}

func writeFile(path string) error {
	return os.WriteFile(path, []byte("payload"), 0o600)
}
