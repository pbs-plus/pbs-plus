package targetplugin

import (
	"errors"
	"fmt"

	"github.com/Masterminds/semver"
)

const (
	CurrentProtocolVersion uint16 = 1
	MethodDescribe                = "plugin.describe"
	SocketFDEnv                   = "PBS_PLUS_PLUGIN_FD"
)

const (
	maxPluginIDLength   = 255
	maxVersionLength    = 64
	maxTargetTypeLength = 255
	maxTargetTypes      = 32
)

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
