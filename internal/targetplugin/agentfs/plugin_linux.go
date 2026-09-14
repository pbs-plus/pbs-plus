//go:build linux

package agentfs

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const (
	PluginID             = "org.pbs-plus.agentfs"
	TargetType           = "agent"
	ArchiveType          = "agentfs"
	ArchiveFormatVersion = 1

	schemaVersion = 1

	hostnameField        = "hostname"
	volumeField          = "volume_id"
	operatingSystemField = "operating_system"
)

// Version is the plugin release version reported to the host.
var Version = "1.0.0"

// Descriptor is the identity and form contract this plugin serves.
func Descriptor() targetplugin.Descriptor {
	linux := targetplugin.NewStringScalar("linux")
	return targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        PluginID,
		Version:         Version,
		TargetTypes:     []string{TargetType},
		TargetSchema: targetplugin.FormSchema{Version: schemaVersion, Fields: []targetplugin.FormField{
			{
				Key: hostnameField, Label: "Agent Hostname", Control: targetplugin.ControlText, Required: true,
				Help: "Hostname the agent registered with this server.",
			},
			{
				Key: volumeField, Label: "Volume", Control: targetplugin.ControlText, Required: true,
				Help: "Agent volume identifier, or root for the whole filesystem.",
			},
			{
				Key: operatingSystemField, Label: "Operating System", Control: targetplugin.ControlSelect,
				Required: true, Default: &linux,
				Options: []targetplugin.SelectOption{
					{Label: "Linux", Value: targetplugin.NewStringScalar("linux")},
					{Label: "Windows", Value: targetplugin.NewStringScalar("windows")},
					{Label: "macOS", Value: targetplugin.NewStringScalar("darwin")},
				},
			},
		}},
		BackupSchema:  targetplugin.FormSchema{Version: schemaVersion},
		RestoreSchema: targetplugin.FormSchema{Version: schemaVersion},
	}
}

// Handlers are the protocol methods this plugin answers.
func Handlers() map[string]targetplugin.MethodHandler {
	return map[string]targetplugin.MethodHandler{
		targetplugin.MethodPluginHealth:   health,
		targetplugin.MethodTargetValidate: validate,
		targetplugin.MethodTargetProbe:    probe,
		targetplugin.MethodBackupOpen:     backupOpen,
		targetplugin.MethodRestoreOpen:    restoreOpen,
	}
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
	agent, err := agentTarget(request.Target.Config)
	if err != nil {
		return nil, err
	}
	return targetplugin.TargetValidateResponse{Config: targetplugin.Values{
		hostnameField:        targetplugin.NewStringScalar(agent.hostname),
		volumeField:          targetplugin.NewStringScalar(agent.volumeID),
		operatingSystemField: targetplugin.NewStringScalar(agent.operatingSystem),
	}}, nil
}

// probe reports intent only: v1 has no broker for agent session state.
func probe(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.TargetProbeRequest](payload)
	if err != nil {
		return nil, err
	}
	agent, err := agentTarget(request.Target.Config)
	if err != nil {
		return nil, err
	}
	return targetplugin.TargetProbeResponse{
		Available: true,
		Message:   fmt.Sprintf("reachability of %s (%s) is verified by the agent session when the job runs", agent.hostname, agent.volumeID),
	}, nil
}

func backupOpen(ctx context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.BackupOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	agent, err := agentTarget(request.Job.Target.Config)
	if err != nil {
		return nil, err
	}
	var mount targetplugin.HostAgentBackupMountResponse
	if err := targetplugin.CallHost(ctx, targetplugin.MethodHostAgentBackupMount, targetplugin.HostAgentBackupMountRequest{
		Operation:       request.Operation,
		Hostname:        agent.hostname,
		VolumeID:        agent.volumeID,
		OperatingSystem: agent.operatingSystem,
	}, &mount); err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
		return nil, err
	}
	return targetplugin.BackupOpenResponse{
		Kind:    targetplugin.SourceDirectory,
		Path:    mount.Path,
		Archive: targetplugin.Archive{Type: ArchiveType, FormatVersion: ArchiveFormatVersion},
		HostFeatures: []targetplugin.HostFeature{
			targetplugin.FeatureSubpath,
			targetplugin.FeatureExclusions,
			targetplugin.FeatureXattrs,
			targetplugin.FeatureChangeDetection,
		},
		CleanupToken: token,
	}, nil
}

func restoreOpen(ctx context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.RestoreOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	if request.Archive.Type != ArchiveType || request.Archive.FormatVersion != ArchiveFormatVersion {
		return nil, fmt.Errorf("archive %s v%d was not written by this plugin", request.Archive.Type, request.Archive.FormatVersion)
	}
	agent, err := agentTarget(request.Job.Target.Config)
	if err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
		return nil, err
	}
	if err := targetplugin.CallHost(ctx, targetplugin.MethodHostAgentRestore, targetplugin.HostAgentRestoreRequest{
		Operation:       request.Operation,
		Hostname:        agent.hostname,
		VolumeID:        agent.volumeID,
		OperatingSystem: agent.operatingSystem,
		DestinationPath: agent.volumeRoot(),
	}, nil); err != nil {
		return nil, err
	}
	return targetplugin.RestoreOpenResponse{Mode: targetplugin.RestoreModeAgent, CleanupToken: token}, nil
}

type agentConfig struct {
	hostname        string
	volumeID        string
	operatingSystem string
}

// volumeRoot reproduces the legacy agent path mapping so pre-plugin snapshots restore where they always did.
func (agent agentConfig) volumeRoot() string {
	volume := strings.ToLower(agent.volumeID)
	switch {
	case volume == "root":
		return "/"
	case agent.operatingSystem == "windows":
		return volume + ":\\"
	default:
		return volume
	}
}

func agentTarget(config targetplugin.Values) (agentConfig, error) {
	hostname, err := text(config, hostnameField)
	if err != nil {
		return agentConfig{}, err
	}
	volume, err := text(config, volumeField)
	if err != nil {
		return agentConfig{}, err
	}
	operatingSystem, err := text(config, operatingSystemField)
	if err != nil {
		return agentConfig{}, err
	}
	switch operatingSystem {
	case "linux", "windows", "darwin":
	default:
		return agentConfig{}, fmt.Errorf("operating system %q is not supported", operatingSystem)
	}
	return agentConfig{hostname: hostname, volumeID: volume, operatingSystem: operatingSystem}, nil
}

func text(config targetplugin.Values, field string) (string, error) {
	value, ok := config[field]
	if !ok {
		return "", fmt.Errorf("%s is required", field)
	}
	result, ok := value.StringValue()
	if !ok || strings.TrimSpace(result) == "" {
		return "", fmt.Errorf("%s must be text", field)
	}
	return strings.TrimSpace(result), nil
}

func leaseToken() ([]byte, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, errors.New("create lease token")
	}
	return token, nil
}
