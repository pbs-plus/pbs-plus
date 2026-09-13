package targetplugin

import (
	"errors"
	"fmt"
	"path/filepath"
)

const (
	maxSecretBytes       = 1 << 20
	maxPathBytes         = 4096
	maxJobIDBytes        = 128
	maxCancellationBytes = 128
	maxArchiveTypeBytes  = 64
	maxLeaseTokenBytes   = 1024
	maxProbeDetails      = 32
)

// Values contains typed non-secret form values.
type Values map[string]Scalar

// Secrets contains resolved secret values keyed by form field.
type Secrets map[string][]byte

// TargetInput contains every target value needed by one plugin operation.
type TargetInput struct {
	Config  Values  `cbor:"config,omitempty"`
	Secrets Secrets `cbor:"secrets,omitempty"`
}

// EventLevel classifies one plugin diagnostic.
type EventLevel string

const (
	EventDebug   EventLevel = "debug"
	EventInfo    EventLevel = "info"
	EventWarning EventLevel = "warning"
	EventError   EventLevel = "error"
)

// HostEvent is a bounded, user-safe plugin diagnostic or progress update.
type HostEvent struct {
	Operation Operation  `cbor:"operation"`
	Level     EventLevel `cbor:"level"`
	Message   string     `cbor:"message"`
	Completed uint64     `cbor:"completed,omitempty"`
	Total     uint64     `cbor:"total,omitempty"`
}

type PluginHealthRequest struct {
	Operation Operation `cbor:"operation"`
}

// PluginHealthResponse reports whether a plugin can accept operations.
type PluginHealthResponse struct {
	Healthy bool   `cbor:"healthy"`
	Message string `cbor:"message,omitempty"`
}

// TargetValidateRequest validates and normalizes one target configuration.
type TargetValidateRequest struct {
	Operation Operation   `cbor:"operation"`
	Target    TargetInput `cbor:"target"`
}

// TargetValidateResponse returns normalized non-secret target configuration.
type TargetValidateResponse struct {
	Config Values `cbor:"config,omitempty"`
}

// TargetProbeRequest probes one fully resolved target.
type TargetProbeRequest struct {
	Operation Operation   `cbor:"operation"`
	Target    TargetInput `cbor:"target"`
}

// TargetProbeResponse reports availability and optional capacity details.
type TargetProbeResponse struct {
	Available bool   `cbor:"available"`
	Message   string `cbor:"message,omitempty"`
	Size      *Size  `cbor:"size,omitempty"`
	Details   Values `cbor:"details,omitempty"`
}

// Size reports target capacity in bytes.
type Size struct {
	Total uint64 `cbor:"total"`
	Used  uint64 `cbor:"used"`
	Free  uint64 `cbor:"free"`
}

// JobInput contains inputs shared by backup and restore operations.
type JobInput struct {
	Target         TargetInput `cbor:"target"`
	Options        Values      `cbor:"options,omitempty"`
	Workspace      string      `cbor:"workspace"`
	JobID          string      `cbor:"job_id"`
	CancellationID string      `cbor:"cancellation_id"`
}

// Archive identifies plugin-owned snapshot data.
type Archive struct {
	Type          string `cbor:"type"`
	FormatVersion uint32 `cbor:"format_version"`
}

// SourceKind identifies how the host reads a backup lease.
type SourceKind string

const (
	SourceDirectory SourceKind = "directory"
	SourceRawStream SourceKind = "raw_stream"
)

// HostFeature identifies backup behavior supported by a source lease.
type HostFeature string

const (
	FeatureSubpath         HostFeature = "subpath"
	FeatureExclusions      HostFeature = "exclusions"
	FeatureXattrs          HostFeature = "xattrs"
	FeatureChangeDetection HostFeature = "change_detection"
)

// BackupOpenRequest prepares one readable backup source.
type BackupOpenRequest struct {
	Operation Operation `cbor:"operation"`
	Job       JobInput  `cbor:"job"`
}

// BackupOpenResponse leases one directory or raw stream to the host.
type BackupOpenResponse struct {
	Kind         SourceKind    `cbor:"kind"`
	Path         string        `cbor:"path,omitempty"`
	Archive      Archive       `cbor:"archive"`
	HostFeatures []HostFeature `cbor:"host_features,omitempty"`
	CleanupToken []byte        `cbor:"cleanup_token"`
}

// BackupCheckRequest performs explicit backup preflight without creating a lease.
type BackupCheckRequest BackupOpenRequest

// BackupCheckResponse acknowledges a successful backup preflight.
type BackupCheckResponse struct{}

// RestoreOpenRequest prepares a writable path destination.
type RestoreOpenRequest struct {
	Operation Operation `cbor:"operation"`
	Job       JobInput  `cbor:"job"`
	Archive   Archive   `cbor:"archive"`
}

// RestoreOpenResponse leases one writable directory to the host.
type RestoreOpenResponse struct {
	Path         string `cbor:"path"`
	CleanupToken []byte `cbor:"cleanup_token"`
}

// RestoreCheckRequest validates an extracted structured archive.
type RestoreCheckRequest struct {
	Operation   Operation `cbor:"operation"`
	Job         JobInput  `cbor:"job"`
	Archive     Archive   `cbor:"archive"`
	ArchivePath string    `cbor:"archive_path"`
}

// RestoreCheckResponse acknowledges a compatible structured archive.
type RestoreCheckResponse struct{}

// RestoreConsumeRequest applies an extracted structured archive.
type RestoreConsumeRequest RestoreCheckRequest

// RestoreConsumeResponse acknowledges a completed structured restore.
type RestoreConsumeResponse struct{}

// MigrateRequest carries one canonical-CBOR schema transition.
type MigrateRequest struct {
	Operation         Operation `cbor:"operation"`
	FromSchemaVersion uint32    `cbor:"from_schema_version"`
	ToSchemaVersion   uint32    `cbor:"to_schema_version"`
	Values            Values    `cbor:"values,omitempty"`
	SecretFields      []string  `cbor:"secret_fields,omitempty"`
}

// MigrateResponse returns migrated values and secret-key edits without plaintext.
type MigrateResponse struct {
	Values        Values            `cbor:"values,omitempty"`
	RenameSecrets map[string]string `cbor:"rename_secrets,omitempty"`
	DeleteSecrets []string          `cbor:"delete_secrets,omitempty"`
}

// TargetMigrateRequest is the target configuration migration request.
type TargetMigrateRequest MigrateRequest

// TargetMigrateResponse is the target configuration migration response.
type TargetMigrateResponse MigrateResponse

// BackupMigrateOptionsRequest is the backup option migration request.
type BackupMigrateOptionsRequest MigrateRequest

// BackupMigrateOptionsResponse is the backup option migration response.
type BackupMigrateOptionsResponse MigrateResponse

// RestoreMigrateOptionsRequest is the restore option migration request.
type RestoreMigrateOptionsRequest MigrateRequest

// RestoreMigrateOptionsResponse is the restore option migration response.
type RestoreMigrateOptionsResponse MigrateResponse

// Validate checks operation identity for an explicit health check.
func (request PluginHealthRequest) Validate() error {
	return request.Operation.Validate()
}

// Validate checks a bounded health diagnostic.
func (response PluginHealthResponse) Validate() error {
	if !response.Healthy && response.Message == "" {
		return errors.New("unhealthy plugin requires a message")
	}
	if response.Message == "" {
		return nil
	}
	return validateText("plugin health message", response.Message, maxProtocolErrorBytes)
}

// Validate checks broker authorization, level, bounded message, and progress bounds.
func (event HostEvent) Validate() error {
	if err := validateBrokerOperation(event.Operation); err != nil {
		return err
	}
	switch event.Level {
	case EventDebug, EventInfo, EventWarning, EventError:
	default:
		return fmt.Errorf("invalid event level %q", event.Level)
	}
	if event.Message == "" {
		return errors.New("event message is required")
	}
	if err := validateText("event message", event.Message, maxProtocolErrorBytes); err != nil {
		return err
	}
	if event.Total != 0 && event.Completed > event.Total {
		return errors.New("event progress exceeds total")
	}
	return nil
}

// Validate checks target values and operation identity.
func (request TargetValidateRequest) Validate() error {
	return validateTargetRequest(request.Operation, request.Target)
}

// Validate checks normalized non-secret target values.
func (response TargetValidateResponse) Validate() error {
	return response.Config.validate("config")
}

// Validate checks target values and operation identity.
func (request TargetProbeRequest) Validate() error {
	return validateTargetRequest(request.Operation, request.Target)
}

// Validate checks bounded probe diagnostics and capacity values.
func (response TargetProbeResponse) Validate() error {
	if !response.Available && response.Message == "" {
		return errors.New("unavailable target requires a message")
	}
	if response.Message != "" {
		if err := validateText("probe message", response.Message, maxProtocolErrorBytes); err != nil {
			return err
		}
	}
	if response.Size != nil {
		if response.Size.Used > response.Size.Total || response.Size.Free > response.Size.Total {
			return errors.New("probe size exceeds total bytes")
		}
	}
	if len(response.Details) > maxProbeDetails {
		return fmt.Errorf("probe contains more than %d details", maxProbeDetails)
	}
	return response.Details.validate("probe detail")
}

// Validate checks all inputs required to open a backup source.
func (request BackupOpenRequest) Validate() error {
	if err := validateJobOperation(request.Operation); err != nil {
		return err
	}
	return request.Job.validate()
}

// Validate checks an explicit backup preflight request.
func (request BackupCheckRequest) Validate() error {
	return BackupOpenRequest(request).Validate()
}

// Validate checks backup lease shape, metadata, and cleanup capability.
func (response BackupOpenResponse) Validate() error {
	switch response.Kind {
	case SourceDirectory:
		if err := validateAbsolutePath("backup source path", response.Path); err != nil {
			return err
		}
	case SourceRawStream:
		if response.Path != "" {
			return errors.New("raw stream backup source must not contain a path")
		}
	default:
		return fmt.Errorf("unsupported backup source kind %q", response.Kind)
	}
	if err := response.Archive.Validate(); err != nil {
		return err
	}
	if err := validateHostFeatures(response.HostFeatures); err != nil {
		return err
	}
	return validateCleanupToken(response.CleanupToken)
}

// Validate checks all inputs required to open a restore destination.
func (request RestoreOpenRequest) Validate() error {
	if err := validateJobOperation(request.Operation); err != nil {
		return err
	}
	if err := request.Job.validate(); err != nil {
		return err
	}
	return request.Archive.Validate()
}

// Validate checks the writable path and cleanup capability.
func (response RestoreOpenResponse) Validate() error {
	if err := validateAbsolutePath("restore destination path", response.Path); err != nil {
		return err
	}
	return validateCleanupToken(response.CleanupToken)
}

// Validate checks all inputs required to inspect a structured archive.
func (request RestoreCheckRequest) Validate() error {
	if err := validateJobOperation(request.Operation); err != nil {
		return err
	}
	if err := request.Job.validate(); err != nil {
		return err
	}
	if err := request.Archive.Validate(); err != nil {
		return err
	}
	return validateAbsolutePath("restore archive path", request.ArchivePath)
}

// Validate checks all inputs required to consume a structured archive.
func (request RestoreConsumeRequest) Validate() error {
	return RestoreCheckRequest(request).Validate()
}

// Validate checks a schema transition and secret field presence.
func (request MigrateRequest) Validate() error {
	if err := validateTargetOperation(request.Operation); err != nil {
		return err
	}
	if request.FromSchemaVersion == 0 || request.ToSchemaVersion == 0 {
		return errors.New("migration schema versions are required")
	}
	if request.FromSchemaVersion == request.ToSchemaVersion {
		return errors.New("migration schema versions must differ")
	}
	if err := request.Values.validate("migration value"); err != nil {
		return err
	}
	if err := validateFieldNames("migration secret field", request.SecretFields); err != nil {
		return err
	}
	for _, field := range request.SecretFields {
		if _, ok := request.Values[field]; ok {
			return fmt.Errorf("field %q is both migration value and secret", field)
		}
	}
	return nil
}

// Validate checks migrated values and non-conflicting secret edits.
func (response MigrateResponse) Validate() error {
	if err := response.Values.validate("migration value"); err != nil {
		return err
	}
	if len(response.RenameSecrets)+len(response.DeleteSecrets) > maxFormFields {
		return fmt.Errorf("migration contains more than %d secret edits", maxFormFields)
	}
	seen := make(map[string]struct{}, len(response.RenameSecrets)+len(response.DeleteSecrets))
	destinations := make(map[string]struct{}, len(response.RenameSecrets))
	for from, to := range response.RenameSecrets {
		if err := validateIdentifier("renamed secret field", from, maxFormKeyBytes); err != nil {
			return err
		}
		if err := validateIdentifier("new secret field", to, maxFormKeyBytes); err != nil {
			return err
		}
		if from == to {
			return fmt.Errorf("secret field %q is renamed to itself", from)
		}
		if _, ok := destinations[to]; ok {
			return fmt.Errorf("multiple secrets are renamed to field %q", to)
		}
		seen[from] = struct{}{}
		destinations[to] = struct{}{}
	}
	for _, field := range response.DeleteSecrets {
		if err := validateIdentifier("deleted secret field", field, maxFormKeyBytes); err != nil {
			return err
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("secret field %q is both renamed and deleted", field)
		}
		seen[field] = struct{}{}
	}
	return nil
}

// Validate checks a target configuration migration request.
func (request TargetMigrateRequest) Validate() error {
	return MigrateRequest(request).Validate()
}

// Validate checks a target configuration migration response.
func (response TargetMigrateResponse) Validate() error {
	return MigrateResponse(response).Validate()
}

// Validate checks a backup option migration request.
func (request BackupMigrateOptionsRequest) Validate() error {
	return MigrateRequest(request).Validate()
}

// Validate checks a backup option migration response.
func (response BackupMigrateOptionsResponse) Validate() error {
	return MigrateResponse(response).Validate()
}

// Validate checks a restore option migration request.
func (request RestoreMigrateOptionsRequest) Validate() error {
	return MigrateRequest(request).Validate()
}

// Validate checks a restore option migration response.
func (response RestoreMigrateOptionsResponse) Validate() error {
	return MigrateResponse(response).Validate()
}

// Validate checks stable archive identity and format version.
func (archive Archive) Validate() error {
	if err := validateIdentifier("archive type", archive.Type, maxArchiveTypeBytes); err != nil {
		return err
	}
	if archive.FormatVersion == 0 {
		return errors.New("archive format version is required")
	}
	return nil
}

func validateTargetRequest(operation Operation, target TargetInput) error {
	if err := validateTargetOperation(operation); err != nil {
		return err
	}
	return target.validate()
}

func validateTargetOperation(operation Operation) error {
	if err := operation.Validate(); err != nil {
		return err
	}
	if operation.TargetType == "" {
		return errors.New("target operation requires a target type")
	}
	return nil
}

func validateJobOperation(operation Operation) error {
	if err := validateTargetOperation(operation); err != nil {
		return err
	}
	return requireBrokerToken(operation)
}

func validateBrokerOperation(operation Operation) error {
	if err := operation.Validate(); err != nil {
		return err
	}
	return requireBrokerToken(operation)
}

func requireBrokerToken(operation Operation) error {
	if len(operation.BrokerToken) != brokerTokenBytes {
		return errors.New("operation requires a broker token")
	}
	return nil
}

func (target TargetInput) validate() error {
	if err := target.Config.validate("config"); err != nil {
		return err
	}
	if len(target.Secrets) > maxFormFields {
		return fmt.Errorf("target contains more than %d secrets", maxFormFields)
	}
	for key, value := range target.Secrets {
		if err := validateIdentifier("secret field", key, maxFormKeyBytes); err != nil {
			return err
		}
		if _, ok := target.Config[key]; ok {
			return fmt.Errorf("field %q is both config and secret", key)
		}
		if len(value) > maxSecretBytes {
			return fmt.Errorf("secret field %q exceeds %d bytes", key, maxSecretBytes)
		}
	}
	return nil
}

func (job JobInput) validate() error {
	if err := job.Target.validate(); err != nil {
		return err
	}
	if err := job.Options.validate("job option"); err != nil {
		return err
	}
	if err := validateAbsolutePath("operation workspace", job.Workspace); err != nil {
		return err
	}
	if err := validateText("job ID", job.JobID, maxJobIDBytes); err != nil {
		return err
	}
	return validateText("cancellation ID", job.CancellationID, maxCancellationBytes)
}

func (values Values) validate(label string) error {
	if len(values) > maxFormFields {
		return fmt.Errorf("%s map contains more than %d fields", label, maxFormFields)
	}
	for key, value := range values {
		if err := validateIdentifier(label+" field", key, maxFormKeyBytes); err != nil {
			return err
		}
		if err := value.validate(); err != nil {
			return fmt.Errorf("%s field %q: %w", label, key, err)
		}
	}
	return nil
}

func validateAbsolutePath(label, path string) error {
	if err := validateText(label, path, maxPathBytes); err != nil {
		return err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("%s must be an absolute clean path", label)
	}
	return nil
}

func validateCleanupToken(token []byte) error {
	if len(token) == 0 {
		return errors.New("lease cleanup token is required")
	}
	if len(token) > maxLeaseTokenBytes {
		return fmt.Errorf("lease cleanup token exceeds %d bytes", maxLeaseTokenBytes)
	}
	return nil
}

func validateHostFeatures(features []HostFeature) error {
	if len(features) > 4 {
		return errors.New("backup lease contains more than 4 host features")
	}
	seen := make(map[HostFeature]struct{}, len(features))
	for _, feature := range features {
		switch feature {
		case FeatureSubpath, FeatureExclusions, FeatureXattrs, FeatureChangeDetection:
		default:
			return fmt.Errorf("unsupported host feature %q", feature)
		}
		if _, ok := seen[feature]; ok {
			return fmt.Errorf("duplicate host feature %q", feature)
		}
		seen[feature] = struct{}{}
	}
	return nil
}

func validateFieldNames(label string, fields []string) error {
	if len(fields) > maxFormFields {
		return fmt.Errorf("%s list contains more than %d fields", label, maxFormFields)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := validateIdentifier(label, field, maxFormKeyBytes); err != nil {
			return err
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("duplicate %s %q", label, field)
		}
		seen[field] = struct{}{}
	}
	return nil
}
