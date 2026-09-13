package targetplugin

import (
	"errors"
	"fmt"

	"github.com/Masterminds/semver"
)

const CurrentProtocolVersion uint16 = 1

const (
	MethodDescribe              = "plugin.describe"
	MethodPluginHealth          = "plugin.health"
	MethodTargetValidate        = "target.validate"
	MethodTargetProbe           = "target.probe"
	MethodTargetMigrate         = "target.migrate"
	MethodBackupOpen            = "backup.open"
	MethodBackupCheck           = "backup.check"
	MethodBackupMigrateOptions  = "backup.migrate_options"
	MethodRestoreOpen           = "restore.open"
	MethodRestoreConsume        = "restore.consume"
	MethodRestoreCheck          = "restore.check"
	MethodRestoreMigrateOptions = "restore.migrate_options"
	MethodHostEvent             = "host.event"
	MethodHostScratch           = "host.scratch"
	MethodHostAgentBackupMount  = "host.agent_backup_mount"
	MethodHostAgentRestore      = "host.agent_restore"
	MethodHostLeaseClose        = "host.lease_close"
)

const SocketFDEnv = "PBS_PLUS_PLUGIN_FD"

const (
	maxPluginIDLength       = 255
	maxVersionLength        = 64
	maxTargetTypeLength     = 255
	maxTargetTypes          = 32
	maxOperationIDLength    = 128
	maxIdempotencyKeyLength = 255
	brokerTokenBytes        = 32
	maxProtocolErrorBytes   = 4096
)

// ErrorCode identifies a stable plugin failure class.
type ErrorCode string

const (
	ErrorInvalidRequest   ErrorCode = "invalid_request"
	ErrorUnauthorized     ErrorCode = "unauthorized"
	ErrorNotFound         ErrorCode = "not_found"
	ErrorConflict         ErrorCode = "conflict"
	ErrorUnsupported      ErrorCode = "unsupported"
	ErrorUnavailable      ErrorCode = "temporarily_unavailable"
	ErrorDeadlineExceeded ErrorCode = "deadline_exceeded"
	ErrorCancelled        ErrorCode = "cancelled"
	ErrorInternal         ErrorCode = "internal"
)

// Operation identifies one retryable plugin call.
type Operation struct {
	ProtocolVersion   uint16 `cbor:"protocol_version"`
	ID                string `cbor:"id"`
	IdempotencyKey    string `cbor:"idempotency_key"`
	DeadlineUnixMilli int64  `cbor:"deadline_unix_milli"`
	PluginVersion     string `cbor:"plugin_version"`
	TargetType        string `cbor:"target_type,omitempty"`
	SchemaVersion     uint32 `cbor:"schema_version,omitempty"`
	BrokerToken       []byte `cbor:"broker_token,omitempty"`
}

// ProtocolError is a bounded, user-safe plugin failure.
type ProtocolError struct {
	Code             ErrorCode `cbor:"code"`
	Message          string    `cbor:"message"`
	RetryAfterMillis uint64    `cbor:"retry_after_millis,omitempty"`
}

// Validate checks required operation identity and target schema metadata.
func (operation Operation) Validate() error {
	if operation.ProtocolVersion != CurrentProtocolVersion {
		return fmt.Errorf("unsupported plugin protocol %d", operation.ProtocolVersion)
	}
	if err := validateText("operation ID", operation.ID, maxOperationIDLength); err != nil {
		return err
	}
	if err := validateText("idempotency key", operation.IdempotencyKey, maxIdempotencyKeyLength); err != nil {
		return err
	}
	if operation.DeadlineUnixMilli <= 0 {
		return errors.New("operation deadline is required")
	}
	if operation.PluginVersion == "" {
		return errors.New("operation plugin version is required")
	}
	if len(operation.PluginVersion) > maxVersionLength {
		return fmt.Errorf("operation plugin version exceeds %d bytes", maxVersionLength)
	}
	if _, err := semver.NewVersion(operation.PluginVersion); err != nil {
		return fmt.Errorf("invalid operation plugin version: %w", err)
	}
	if len(operation.BrokerToken) != 0 && len(operation.BrokerToken) != brokerTokenBytes {
		return fmt.Errorf("operation broker token must be %d bytes", brokerTokenBytes)
	}
	if operation.TargetType == "" {
		if operation.SchemaVersion != 0 {
			return errors.New("operation schema version requires a target type")
		}
		return nil
	}
	if err := validateIdentifier("operation target type", operation.TargetType, maxTargetTypeLength); err != nil {
		return err
	}
	if operation.SchemaVersion == 0 {
		return errors.New("operation target schema version is required")
	}
	return nil
}

// Validate checks the stable error code and bounded diagnostic.
func (protocolError ProtocolError) Validate() error {
	switch protocolError.Code {
	case ErrorInvalidRequest, ErrorUnauthorized, ErrorNotFound, ErrorConflict, ErrorUnsupported,
		ErrorUnavailable, ErrorDeadlineExceeded, ErrorCancelled, ErrorInternal:
	default:
		return fmt.Errorf("unsupported plugin error code %q", protocolError.Code)
	}
	if err := validateText("plugin error message", protocolError.Message, maxProtocolErrorBytes); err != nil {
		return err
	}
	if protocolError.RetryAfterMillis != 0 && protocolError.Code != ErrorUnavailable {
		return errors.New("retry delay is only valid for temporarily unavailable errors")
	}
	return nil
}

// DescribeRequest identifies the protocol version offered by the host.
type DescribeRequest struct {
	ProtocolVersion uint16 `cbor:"protocol_version"`
}

// Descriptor identifies an installed plugin and the target types it provides.
type Descriptor struct {
	ProtocolVersion uint16   `cbor:"protocol_version"`
	PluginID        string   `cbor:"plugin_id"`
	Version         string   `cbor:"version"`
	TargetTypes     []string `cbor:"target_types"`
}

// Validate checks descriptor identity and protocol compatibility.
func (d Descriptor) Validate() error {
	if d.ProtocolVersion != CurrentProtocolVersion {
		return fmt.Errorf("unsupported plugin protocol %d", d.ProtocolVersion)
	}
	if err := validateIdentifier("plugin ID", d.PluginID, maxPluginIDLength); err != nil {
		return err
	}
	if d.Version == "" {
		return errors.New("plugin version is required")
	}
	if len(d.Version) > maxVersionLength {
		return fmt.Errorf("plugin version exceeds %d bytes", maxVersionLength)
	}
	if _, err := semver.NewVersion(d.Version); err != nil {
		return fmt.Errorf("invalid plugin version: %w", err)
	}
	if len(d.TargetTypes) == 0 {
		return errors.New("plugin must provide at least one target type")
	}
	if len(d.TargetTypes) > maxTargetTypes {
		return fmt.Errorf("plugin provides more than %d target types", maxTargetTypes)
	}

	seen := make(map[string]struct{}, len(d.TargetTypes))
	for _, targetType := range d.TargetTypes {
		if err := validateIdentifier("target type", targetType, maxTargetTypeLength); err != nil {
			return err
		}
		if _, ok := seen[targetType]; ok {
			return fmt.Errorf("duplicate target type %q", targetType)
		}
		seen[targetType] = struct{}{}
	}
	return nil
}

func validateIdentifier(label, value string, maxLength int) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	if len(value) > maxLength {
		return fmt.Errorf("%s exceeds %d bytes", label, maxLength)
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '-' || char == '_' {
			continue
		}
		return fmt.Errorf("%s contains invalid character %q", label, char)
	}
	return nil
}
