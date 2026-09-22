//go:build linux

package filesystem

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
	"golang.org/x/sys/unix"
)

const (
	PluginID             = "org.pbs-plus.filesystem"
	TargetTypeLocal      = "local"
	ArchiveType          = "filesystem"
	ArchiveFormatVersion = 1

	schemaVersion = 1
	pathField     = "path"
)

// Version is the plugin release version reported to the host.
var Version = "1.0.0"

// Descriptor is the identity and form contract this plugin serves.
func Descriptor() targetplugin.Descriptor {
	return targetplugin.Descriptor{
		ProtocolVersion: targetplugin.CurrentProtocolVersion,
		PluginID:        PluginID,
		Version:         Version,
		TargetTypes:     []string{TargetTypeLocal},
		TargetSchema: targetplugin.FormSchema{Version: schemaVersion, Fields: []targetplugin.FormField{{
			Key:      pathField,
			Label:    "Path",
			Control:  targetplugin.ControlPath,
			Required: true,
			Help:     "Absolute directory backed up from this server.",
		}}},
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
		targetplugin.MethodBackupCheck:    backupCheck,
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
	path, err := targetPath(request.Target.Config)
	if err != nil {
		return nil, err
	}
	if err := requireDirectory(path); err != nil {
		return nil, err
	}
	return targetplugin.TargetValidateResponse{
		Config: targetplugin.Values{pathField: targetplugin.NewStringScalar(path)},
	}, nil
}

func probe(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.TargetProbeRequest](payload)
	if err != nil {
		return nil, err
	}
	path, err := targetPath(request.Target.Config)
	if err != nil {
		return nil, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return targetplugin.TargetProbeResponse{Message: fmt.Sprintf("cannot read %s: %s", path, err)}, nil
	}
	blockSize := uint64(stat.Bsize)
	return targetplugin.TargetProbeResponse{
		Available: true,
		Size: &targetplugin.Size{
			Total: stat.Blocks * blockSize,
			Used:  (stat.Blocks - stat.Bfree) * blockSize,
			Free:  stat.Bfree * blockSize,
		},
	}, nil
}

func backupOpen(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.BackupOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	path, err := sourcePath(request.Job)
	if err != nil {
		return nil, err
	}
	token, err := targetplugin.NewLeaseToken()
	if err != nil {
		return nil, err
	}
	return targetplugin.BackupOpenResponse{
		Kind:    targetplugin.SourceDirectory,
		Path:    path,
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

func backupCheck(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.BackupCheckRequest](payload)
	if err != nil {
		return nil, err
	}
	if _, err := sourcePath(request.Job); err != nil {
		return nil, err
	}
	return targetplugin.BackupCheckResponse{}, nil
}

func restoreOpen(_ context.Context, payload []byte) (any, error) {
	request, err := targetplugin.Request[targetplugin.RestoreOpenRequest](payload)
	if err != nil {
		return nil, err
	}
	if request.Archive.Type != ArchiveType || request.Archive.FormatVersion != ArchiveFormatVersion {
		return nil, fmt.Errorf("archive %s v%d was not written by this plugin", request.Archive.Type, request.Archive.FormatVersion)
	}
	path, err := sourcePath(request.Job)
	if err != nil {
		return nil, err
	}
	token, err := targetplugin.NewLeaseToken()
	if err != nil {
		return nil, err
	}
	return targetplugin.RestoreOpenResponse{
		Mode:         targetplugin.RestoreModePath,
		Path:         path,
		CleanupToken: token,
	}, nil
}

func sourcePath(job targetplugin.JobInput) (string, error) {
	path, err := targetPath(job.Target.Config)
	if err != nil {
		return "", err
	}
	if err := requireDirectory(path); err != nil {
		return "", err
	}
	return path, nil
}

func targetPath(config targetplugin.Values) (string, error) {
	value, ok := config[pathField]
	if !ok {
		return "", errors.New("target path is required")
	}
	path, ok := value.StringValue()
	if !ok {
		return "", errors.New("target path must be text")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("target path %q must be absolute", path)
	}
	return filepath.Clean(path), nil
}

func requireDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("target path %q is not readable: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("target path %q is not a directory", path)
	}
	return nil
}
