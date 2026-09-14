//go:build linux

package ldap

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

const (
	PluginID             = "org.pbs-plus.ldap"
	TargetType           = "ldap"
	ArchiveType          = "ldap"
	ArchiveFormatVersion = 1

	targetSchemaVersion = 1
	jobSchemaVersion    = 1
	defaultPort         = 389
	probeTimeout        = 5 * time.Second

	hostField      = "host"
	portField      = "port"
	usernameField  = "username"
	passwordField  = "password"
	baseDNField    = "base_dn"
	tlsModeField   = "tls_mode"
	caCertField    = "ca_certificate"
	clientDirField = "default_client_dir"

	scopeField       = "scope"
	subtreeField     = "subtree_dn"
	sourceField      = "source_dn"
	destinationField = "destination_dn"
	replaceField     = "replace_existing"
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
			{Key: usernameField, Label: "Bind DN", Control: targetplugin.ControlText, Required: true},
			{Key: passwordField, Label: "Password", Control: targetplugin.ControlSecret, Required: true},
			{Key: baseDNField, Label: "Base DN", Control: targetplugin.ControlText, Required: true},
			{Key: tlsModeField, Label: "TLS Mode", Control: targetplugin.ControlSelect, Options: []targetplugin.SelectOption{
				{Label: "Disabled", Value: targetplugin.NewStringScalar("disabled")},
				{Label: "StartTLS", Value: targetplugin.NewStringScalar("starttls")},
				{Label: "LDAPS", Value: targetplugin.NewStringScalar("ldaps")},
			}},
			{Key: caCertField, Label: "CA Certificate", Control: targetplugin.ControlCertificatePath},
			{Key: clientDirField, Label: "Client Directory", Control: targetplugin.ControlPath},
		}},
		BackupSchema: targetplugin.FormSchema{Version: jobSchemaVersion, Fields: []targetplugin.FormField{
			{Key: scopeField, Label: "Scope", Control: targetplugin.ControlSelect, Options: []targetplugin.SelectOption{
				{Label: "Base DN", Value: targetplugin.NewStringScalar("server")},
				{Label: "Subtree", Value: targetplugin.NewStringScalar("database")},
			}},
			{Key: subtreeField, Label: "Subtree DN", Control: targetplugin.ControlText},
		}},
		RestoreSchema: targetplugin.FormSchema{Version: jobSchemaVersion, Fields: []targetplugin.FormField{
			{Key: sourceField, Label: "Source DN", Control: targetplugin.ControlText},
			{Key: destinationField, Label: "Destination DN", Control: targetplugin.ControlText},
			{Key: replaceField, Label: "Replace Existing Subtree", Control: targetplugin.ControlBoolean},
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
	if _, err := database.DiscoverClientBundles(ctx); err != nil {
		return targetplugin.PluginHealthResponse{Message: err.Error()}, nil
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
	target, password, err := databaseTarget(request.Job)
	if err != nil {
		return nil, err
	}
	bundle, err := database.SelectClientBundle(ctx, target, password, nil)
	if err != nil {
		return nil, err
	}
	scope, _ := request.Job.Options[scopeField].StringValue()
	if scope == "" {
		scope = "server"
	}
	name, _ := request.Job.Options[subtreeField].StringValue()
	staged, err := database.StageDump(ctx, request.Job.Workspace, target, password, database.DumpOptions{
		Scope:    scope,
		Database: name,
	}, bundle)
	if err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
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
	if _, _, err := databaseTarget(request.Job); err != nil {
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
	target, password, err := databaseTarget(request.Job)
	if err != nil {
		return nil, err
	}
	bundle, err := database.SelectClientBundle(ctx, target, password, nil)
	if err != nil {
		return nil, err
	}
	source, _ := request.Job.Options[sourceField].StringValue()
	destination, _ := request.Job.Options[destinationField].StringValue()
	replace, _ := request.Job.Options[replaceField].BooleanValue()
	if err := database.RestoreDump(ctx, request.ArchivePath, target, password, database.RestoreOptions{
		SourceDatabase:      source,
		DestinationDatabase: destination,
		ReplaceExisting:     replace,
	}, bundle); err != nil {
		return nil, err
	}
	return targetplugin.RestoreConsumeResponse{}, nil
}

func databaseTarget(job targetplugin.JobInput) (coredb.Target, string, error) {
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
	username, _ := config[usernameField].StringValue()
	baseDN, _ := config[baseDNField].StringValue()
	tlsMode, _ := config[tlsModeField].StringValue()
	caCertificate, _ := config[caCertField].StringValue()
	clientDir, _ := config[clientDirField].StringValue()
	return coredb.Target{
		Type:                     coredb.TargetTypeLDAP,
		DatabaseHost:             host,
		DatabasePort:             int(port),
		DatabaseUsername:         username,
		DatabaseTLSMode:          tlsMode,
		DatabaseCACertificate:    caCertificate,
		DatabaseDefaultClientDir: clientDir,
		LdapBaseDN:               baseDN,
	}, password, nil
}

func normalizeConfig(config targetplugin.Values) (targetplugin.Values, error) {
	host, ok := config[hostField].StringValue()
	if !ok || host == "" {
		return nil, errors.New("host is required")
	}
	username, ok := config[usernameField].StringValue()
	if !ok || username == "" {
		return nil, errors.New("username is required")
	}
	port, ok := config[portField].IntegerValue()
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port %d", port)
	}
	normalized := targetplugin.Values{
		hostField:     targetplugin.NewStringScalar(host),
		portField:     targetplugin.NewIntegerScalar(port),
		usernameField: targetplugin.NewStringScalar(username),
	}
	baseDN, ok := config[baseDNField].StringValue()
	if !ok || baseDN == "" {
		return nil, errors.New("base DN is required")
	}
	normalized[baseDNField] = targetplugin.NewStringScalar(baseDN)
	tlsMode, _ := config[tlsModeField].StringValue()
	if tlsMode == "" {
		tlsMode = "starttls"
	}
	switch tlsMode {
	case "disabled", "starttls", "ldaps":
	default:
		return nil, fmt.Errorf("unsupported LDAP TLS mode %q", tlsMode)
	}
	normalized[tlsModeField] = targetplugin.NewStringScalar(tlsMode)
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
