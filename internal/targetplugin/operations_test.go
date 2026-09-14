package targetplugin

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestOperationContractsValidate(t *testing.T) {
	operation := jobTestOperation()
	target := TargetInput{
		Config:  Values{"path": NewStringScalar("/data")},
		Secrets: Secrets{"password": []byte("secret")},
	}
	job := JobInput{
		Target:         target,
		Options:        Values{"xattrs": NewBooleanScalar(true)},
		Workspace:      "/run/pbs-plus/op-1",
		JobID:          "job-1",
		CancellationID: "cancel-1",
	}
	archive := Archive{Type: "pxar", FormatVersion: 1}

	tests := []struct {
		name  string
		value interface{ Validate() error }
	}{
		{name: "plugin health request", value: PluginHealthRequest{Operation: pluginTestOperation()}},
		{name: "plugin health response", value: PluginHealthResponse{Healthy: true}},
		{name: "target validate request", value: TargetValidateRequest{Operation: operation, Target: target}},
		{name: "target validate response", value: TargetValidateResponse{Config: target.Config}},
		{name: "target probe request", value: TargetProbeRequest{Operation: operation, Target: target}},
		{name: "target probe response", value: TargetProbeResponse{Available: true, Size: &Size{Total: 10, Used: 6, Free: 4}}},
		{name: "backup open request", value: BackupOpenRequest{Operation: operation, Job: job}},
		{name: "backup check request", value: BackupCheckRequest{Operation: operation, Job: job}},
		{name: "directory backup response", value: BackupOpenResponse{Kind: SourceDirectory, Path: "/data", Archive: archive, CleanupToken: []byte("lease")}},
		{name: "stream backup response", value: BackupOpenResponse{Kind: SourceRawStream, Archive: archive, CleanupToken: []byte("lease")}},
		{name: "restore open request", value: RestoreOpenRequest{Operation: operation, Job: job, Archive: archive}},
		{name: "path restore response", value: RestoreOpenResponse{Mode: RestoreModePath, Path: "/restore", CleanupToken: []byte("lease")}},
		{name: "structured restore response", value: RestoreOpenResponse{Mode: RestoreModeStructured, CleanupToken: []byte("lease")}},
		{name: "agent restore response", value: RestoreOpenResponse{Mode: RestoreModeAgent, CleanupToken: []byte("lease")}},
		{name: "host agent backup mount request", value: HostAgentBackupMountRequest{Operation: operation, Hostname: "agent", VolumeID: "disk", OperatingSystem: "linux"}},
		{name: "host agent backup mount response", value: HostAgentBackupMountResponse{Path: "/mnt/agent"}},
		{name: "host agent restore request", value: HostAgentRestoreRequest{Operation: operation, Hostname: "agent", VolumeID: "disk", OperatingSystem: "linux", DestinationPath: "/restore"}},
		{name: "restore check request", value: RestoreCheckRequest{Operation: operation, Job: job, Archive: archive, ArchivePath: "/archive"}},
		{name: "restore consume request", value: RestoreConsumeRequest{Operation: operation, Job: job, Archive: archive, ArchivePath: "/archive"}},
		{name: "target migration request", value: TargetMigrateRequest(validMigrateRequest(operation))},
		{name: "target migration response", value: TargetMigrateResponse(validMigrateResponse())},
		{name: "backup migration request", value: BackupMigrateOptionsRequest(validMigrateRequest(operation))},
		{name: "backup migration response", value: BackupMigrateOptionsResponse(validMigrateResponse())},
		{name: "restore migration request", value: RestoreMigrateOptionsRequest(validMigrateRequest(operation))},
		{name: "restore migration response", value: RestoreMigrateOptionsResponse(validMigrateResponse())},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.value.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestOperationContractsRejectInvalidValues(t *testing.T) {
	operation := jobTestOperation()
	job := validJobInput()
	archive := Archive{Type: "pxar", FormatVersion: 1}

	tests := []struct {
		name      string
		value     interface{ Validate() error }
		wantError string
	}{
		{name: "unhealthy without message", value: PluginHealthResponse{}, wantError: "requires a message"},
		{name: "missing target operation", value: TargetProbeRequest{Operation: pluginTestOperation()}, wantError: "requires a target type"},
		{name: "config secret overlap", value: TargetValidateRequest{Operation: operation, Target: TargetInput{Config: Values{"password": NewStringScalar("hidden")}, Secrets: Secrets{"password": []byte("secret")}}}, wantError: "both config and secret"},
		{name: "oversized secret", value: TargetValidateRequest{Operation: operation, Target: TargetInput{Secrets: Secrets{"password": make([]byte, maxSecretBytes+1)}}}, wantError: "secret field \"password\" exceeds"},
		{name: "invalid config scalar", value: TargetValidateResponse{Config: Values{"path": {}}}, wantError: "form scalar is unset"},
		{name: "unavailable target without message", value: TargetProbeResponse{}, wantError: "requires a message"},
		{name: "probe size", value: TargetProbeResponse{Available: true, Size: &Size{Total: 1, Used: 2}}, wantError: "exceeds total"},
		{name: "missing broker token", value: BackupOpenRequest{Operation: validTestOperation(), Job: job}, wantError: "requires a broker token"},
		{name: "relative workspace", value: BackupOpenRequest{Operation: operation, Job: JobInput{Target: job.Target, Workspace: "relative", JobID: "job", CancellationID: "cancel"}}, wantError: "absolute clean path"},
		{name: "directory without path", value: BackupOpenResponse{Kind: SourceDirectory, Archive: archive, CleanupToken: []byte("lease")}, wantError: "source path is required"},
		{name: "stream with path", value: BackupOpenResponse{Kind: SourceRawStream, Path: "/data", Archive: archive, CleanupToken: []byte("lease")}, wantError: "must not contain a path"},
		{name: "duplicate feature", value: BackupOpenResponse{Kind: SourceDirectory, Path: "/data", Archive: archive, HostFeatures: []HostFeature{FeatureXattrs, FeatureXattrs}, CleanupToken: []byte("lease")}, wantError: "duplicate host feature"},
		{name: "unknown restore mode", value: RestoreOpenResponse{Mode: "unknown", CleanupToken: []byte("lease")}, wantError: "unsupported restore mode"},
		{name: "path restore without path", value: RestoreOpenResponse{Mode: RestoreModePath, CleanupToken: []byte("lease")}, wantError: "destination path is required"},
		{name: "structured restore with path", value: RestoreOpenResponse{Mode: RestoreModeStructured, Path: "/restore", CleanupToken: []byte("lease")}, wantError: "must not contain a path"},
		{name: "missing cleanup token", value: RestoreOpenResponse{Mode: RestoreModePath, Path: "/restore"}, wantError: "cleanup token is required"},
		{name: "oversized cleanup token", value: RestoreOpenResponse{Mode: RestoreModePath, Path: "/restore", CleanupToken: make([]byte, maxLeaseTokenBytes+1)}, wantError: "cleanup token exceeds"},
		{name: "agent mount relative path", value: HostAgentBackupMountResponse{Path: "mnt/agent"}, wantError: "absolute clean path"},
		{name: "agent restore missing hostname", value: HostAgentRestoreRequest{Operation: operation, VolumeID: "disk", OperatingSystem: "linux", DestinationPath: "/restore"}, wantError: "agent hostname is required"},
		{name: "dirty archive path", value: RestoreCheckRequest{Operation: operation, Job: job, Archive: archive, ArchivePath: "/archive/../data"}, wantError: "absolute clean path"},
		{name: "same migration version", value: MigrateRequest{Operation: operation, FromSchemaVersion: 1, ToSchemaVersion: 1}, wantError: "must differ"},
		{name: "migration value secret overlap", value: MigrateRequest{Operation: operation, FromSchemaVersion: 1, ToSchemaVersion: 2, Values: Values{"password": NewStringScalar("hidden")}, SecretFields: []string{"password"}}, wantError: "both migration value and secret"},
		{name: "secret renamed and deleted", value: MigrateResponse{RenameSecrets: map[string]string{"old": "new"}, DeleteSecrets: []string{"old"}}, wantError: "both renamed and deleted"},
		{name: "duplicate rename destination", value: MigrateResponse{RenameSecrets: map[string]string{"old-a": "new", "old-b": "new"}}, wantError: "multiple secrets are renamed"},
		{name: "too many secret edits", value: MigrateResponse{DeleteSecrets: repeatedFieldNames(maxFormFields + 1)}, wantError: "more than 128 secret edits"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.value.Validate()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Validate error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestBackupOpenRequestCBORFixture(t *testing.T) {
	operation := jobTestOperation()
	operation.DeadlineUnixMilli = 1700000000000
	request := BackupOpenRequest{
		Operation: operation,
		Job: JobInput{
			Target: TargetInput{
				Config:  Values{"path": NewStringScalar("/data")},
				Secrets: Secrets{"password": []byte("secret")},
			},
			Options:        Values{"xattrs": NewBooleanScalar(true)},
			Workspace:      "/run/op-1",
			JobID:          "job-1",
			CancellationID: "cancel-1",
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	encoded, err := MarshalProtocol(request)
	if err != nil {
		t.Fatalf("MarshalProtocol: %v", err)
	}
	const want = "a2636a6f62a5666a6f625f6964656a6f622d3166746172676574a266636f6e666967a16470617468652f646174616773656372657473a16870617373776f726446736563726574676f7074696f6e73a166786174747273f569776f726b7370616365692f72756e2f6f702d316f63616e63656c6c6174696f6e5f69646863616e63656c2d31696f7065726174696f6ea8626964646f702d316b7461726765745f7479706564746573746c62726f6b65725f746f6b656e582000000000000000000000000000000000000000000000000000000000000000006e706c7567696e5f76657273696f6e65312e302e306e736368656d615f76657273696f6e016f6964656d706f74656e63795f6b65796772657472792d317070726f746f636f6c5f76657273696f6e0173646561646c696e655f756e69785f6d696c6c691b0000018bcfe56800"
	if got := hex.EncodeToString(encoded); got != want {
		t.Fatalf("backup request fixture = %s, want %s", got, want)
	}
}

func jobTestOperation() Operation {
	operation := validTestOperation()
	operation.BrokerToken = make([]byte, brokerTokenBytes)
	return operation
}

func pluginTestOperation() Operation {
	operation := validTestOperation()
	operation.TargetType = ""
	operation.SchemaVersion = 0
	return operation
}

func validJobInput() JobInput {
	return JobInput{
		Target:         TargetInput{Config: Values{"path": NewStringScalar("/data")}},
		Workspace:      "/run/pbs-plus/op-1",
		JobID:          "job-1",
		CancellationID: "cancel-1",
	}
}

func validMigrateRequest(operation Operation) MigrateRequest {
	return MigrateRequest{
		Operation:         operation,
		FromSchemaVersion: 1,
		ToSchemaVersion:   2,
		Values:            Values{"path": NewStringScalar("/data")},
		SecretFields:      []string{"password"},
	}
}

func repeatedFieldNames(count int) []string {
	fields := make([]string, count)
	for index := range fields {
		fields[index] = "field"
	}
	return fields
}

func validMigrateResponse() MigrateResponse {
	return MigrateResponse{
		Values:        Values{"path": NewStringScalar("/data")},
		RenameSecrets: map[string]string{"password": "credential"},
	}
}
