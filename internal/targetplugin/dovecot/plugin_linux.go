//go:build linux

package dovecot

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/dovecot"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const (
	PluginID             = "org.pbs-plus.dovecot"
	TargetType           = "dovecot"
	ArchiveType          = "dovecot"
	ArchiveFormatVersion = 1

	targetSchemaVersion = 1
	jobSchemaVersion    = 1
	defaultPort         = 24245
	probeTimeout        = 5 * time.Second

	hostField      = "host"
	portField      = "port"
	passwordField  = "password"
	caCertField    = "ca_certificate"
	clientDirField = "default_client_dir"

	usernameField        = "username"
	mailboxField         = "mailbox"
	sourceUsernameField  = "source_username"
	destinationUserField = "destination_username"
	replaceField         = "replace_existing"
)

// Version is the plugin release version reported to the host.
var Version = "1.0.0"

// Descriptor is the identity and form contract this plugin serves.
func Descriptor() targetplugin.Descriptor {
	port := int64(defaultPort)
	return targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        PluginID,
		Version:         Version,
		TargetTypes:     []string{TargetType},
		TargetSchema: targetplugin.FormSchema{Version: targetSchemaVersion, Fields: []targetplugin.FormField{
			{Key: hostField, Label: "Host", Control: targetplugin.ControlText, Required: true},
			{Key: portField, Label: "Port", Control: targetplugin.ControlInteger, Minimum: &port, Maximum: maximumPort()},
			{Key: passwordField, Label: "Doveadm Password", Control: targetplugin.ControlSecret, Required: true},
			{Key: caCertField, Label: "CA Certificate", Control: targetplugin.ControlCertificatePath},
			{Key: clientDirField, Label: "Client Directory", Control: targetplugin.ControlPath},
		}},
		BackupSchema: targetplugin.FormSchema{Version: jobSchemaVersion, Fields: []targetplugin.FormField{
			{Key: usernameField, Label: "Username", Control: targetplugin.ControlText, Required: true},
			{Key: mailboxField, Label: "Mailbox", Control: targetplugin.ControlText},
		}},
		RestoreSchema: targetplugin.FormSchema{Version: jobSchemaVersion, Fields: []targetplugin.FormField{
			{Key: sourceUsernameField, Label: "Source Username", Control: targetplugin.ControlText, Required: true},
			{Key: destinationUserField, Label: "Destination Username", Control: targetplugin.ControlText},
			{Key: mailboxField, Label: "Mailbox", Control: targetplugin.ControlText},
			{Key: replaceField, Label: "Replace Existing", Control: targetplugin.ControlBoolean},
		}},
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
		targetplugin.MethodRestoreConsume: restoreConsume,
	}
}

func health(ctx context.Context, payload []byte) (any, error) {
	if _, err := targetplugin.Request[targetplugin.PluginHealthRequest](payload); err != nil {
		return nil, err
	}
	if _, err := dovecot.SelectClient(ctx, coredb.Target{}); err != nil {
		return targetplugin.PluginHealthResponse{Healthy: true, Message: "client tools unavailable: " + err.Error()}, nil
	}
	return targetplugin.PluginHealthResponse{Healthy: true}, nil
}

func validate(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.TargetValidateRequest](payload)
	if err != nil {
		return nil, err
	}
	config, err := normalizeConfig(request.Target.Config)
	if err != nil {
		return nil, err
	}
	if len(request.Target.Secrets[passwordField]) == 0 {
		return nil, errors.New("password is required")
	}
	return targetplugin.TargetValidateResponse{Config: config}, nil
}

func probe(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.TargetProbeRequest](payload)
	if err != nil {
		return nil, err
	}
	config, err := normalizeConfig(request.Target.Config)
	if err != nil {
		return nil, err
	}
	host, _ := config[hostField].StringValue()
	port, _ := config[portField].IntegerValue()
	address := net.JoinHostPort(host, strconv.FormatInt(port, 10))
	conn, err := net.DialTimeout("tcp", address, probeTimeout)
	if err != nil {
		return targetplugin.TargetProbeResponse{Message: fmt.Sprintf("cannot reach %s: %s", address, err)}, nil
	}
	if err := conn.Close(); err != nil {
		return targetplugin.TargetProbeResponse{Message: err.Error()}, nil
	}
	return targetplugin.TargetProbeResponse{Available: true}, nil
}

func backupOpen(ctx context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.BackupOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	target, password, err := dovecotTarget(request.Job)
	if err != nil {
		return nil, err
	}
	logWriter, err := targetplugin.NewJobEventLog(ctx, request.Operation, "Dovecot")
	if err != nil {
		return nil, err
	}
	client, err := dovecot.SelectClient(ctx, target)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(logWriter, "using Dovecot client %s from %s\n", client.Version, client.Program); err != nil {
		return nil, err
	}
	if err := os.Chmod(request.Job.Workspace, 0o711); err != nil {
		return nil, fmt.Errorf("prepare Dovecot workspace permissions: %w", err)
	}
	username, _ := request.Job.Options[usernameField].StringValue()
	mailbox, _ := request.Job.Options[mailboxField].StringValue()
	staged, err := dovecot.StageBackup(ctx, request.Job.Workspace, target, password, dovecot.BackupOptions{
		Username:  username,
		Mailbox:   mailbox,
		LogWriter: logWriter,
	}, client)
	if err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
		_ = staged.Cleanup()
		return nil, err
	}
	return targetplugin.BackupOpenResponse{
		Kind:         targetplugin.SourceDirectory,
		Path:         staged.ArchiveDir,
		Archive:      targetplugin.Archive{Type: ArchiveType, FormatVersion: ArchiveFormatVersion},
		CleanupToken: token,
	}, nil
}

func restoreOpen(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.RestoreOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	if request.Archive.Type != ArchiveType || request.Archive.FormatVersion != ArchiveFormatVersion {
		return nil, fmt.Errorf("archive %s v%d was not written by this plugin", request.Archive.Type, request.Archive.FormatVersion)
	}
	if _, _, err := dovecotTarget(request.Job); err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
		return nil, err
	}
	return targetplugin.RestoreOpenResponse{Mode: targetplugin.RestoreModeStructured, CleanupToken: token}, nil
}

func restoreConsume(ctx context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.RestoreConsumeRequest](payload)
	if err != nil {
		return nil, err
	}
	target, password, err := dovecotTarget(request.Job)
	if err != nil {
		return nil, err
	}
	logWriter, err := targetplugin.NewJobEventLog(ctx, request.Operation, "Dovecot")
	if err != nil {
		return nil, err
	}
	client, err := dovecot.SelectClient(ctx, target)
	if err != nil {
		return nil, err
	}
	sourceUsername, _ := request.Job.Options[sourceUsernameField].StringValue()
	destinationUsername, _ := request.Job.Options[destinationUserField].StringValue()
	mailbox, _ := request.Job.Options[mailboxField].StringValue()
	replace, _ := request.Job.Options[replaceField].BooleanValue()
	if err := dovecot.RestoreBackup(ctx, request.ArchivePath, target, password, dovecot.RestoreOptions{
		SourceUsername:      sourceUsername,
		DestinationUsername: destinationUsername,
		Mailbox:             mailbox,
		ReplaceExisting:     replace,
		LogWriter:           logWriter,
	}, client); err != nil {
		return nil, err
	}
	return targetplugin.RestoreConsumeResponse{}, nil
}

func dovecotTarget(job targetplugin.JobInput) (coredb.Target, string, error) {
	config, err := normalizeConfig(job.Target.Config)
	if err != nil {
		return coredb.Target{}, "", err
	}
	password := string(job.Target.Secrets[passwordField])
	if password == "" {
		return coredb.Target{}, "", errors.New("password is required")
	}
	host, _ := config[hostField].StringValue()
	port, _ := config[portField].IntegerValue()
	caCertificate, _ := config[caCertField].StringValue()
	clientDir, _ := config[clientDirField].StringValue()
	return coredb.Target{
		Type:                     coredb.TargetTypeDovecot,
		DatabaseHost:             host,
		DatabasePort:             int(port),
		DatabaseCACertificate:    caCertificate,
		DatabaseDefaultClientDir: clientDir,
	}, password, nil
}

func normalizeConfig(config targetplugin.Values) (targetplugin.Values, error) {
	host, ok := config[hostField].StringValue()
	if !ok || host == "" {
		return nil, errors.New("host is required")
	}
	port, ok := config[portField].IntegerValue()
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port %d", port)
	}
	normalized := targetplugin.Values{
		hostField: targetplugin.NewStringScalar(host),
		portField: targetplugin.NewIntegerScalar(port),
	}
	if caCertificate, ok := config[caCertField].StringValue(); ok && caCertificate != "" {
		if !filepath.IsAbs(caCertificate) {
			return nil, errors.New("CA certificate path must be absolute")
		}
		normalized[caCertField] = targetplugin.NewStringScalar(caCertificate)
	}
	if clientDir, ok := config[clientDirField].StringValue(); ok && clientDir != "" {
		if !filepath.IsAbs(clientDir) {
			return nil, errors.New("client directory must be absolute")
		}
		normalized[clientDirField] = targetplugin.NewStringScalar(clientDir)
	}
	return normalized, nil
}

func maximumPort() *int64 {
	maximum := int64(65535)
	return &maximum
}

func leaseToken() ([]byte, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("create lease token: %w", err)
	}
	return token, nil
}
