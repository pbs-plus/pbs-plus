package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	datastoreRegex     = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	namespaceRegex     = regexp.MustCompile(`^(?:(?:(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)/){0,7}(?:[A-Za-z0-9_][A-Za-z0-9._\-]*))?$`)
	backupTypeRegex    = regexp.MustCompile(`^(vm|ct|host)$`)
	backupIDRegex      = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	fileNameRegex      = regexp.MustCompile(`^[a-zA-Z0-9_.-]+\.(pxar|img|fidx|didx)$`)
	jobIdRegex         = regexp.MustCompile(`^(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)$`)
	snapshotRegex      = regexp.MustCompile(`^[a-zA-Z0-9/_:.-]+$`)
	subpathRegex       = regexp.MustCompile(`^[a-zA-Z0-9/_. -]*$`)
	exclusionPathRegex = regexp.MustCompile(`^[a-zA-Z0-9/_.\\ *?[\]{},-]+$`)
)

func IsValidNamespace(namespace string) bool {
	return ValidateNamespace(namespace) == nil
}

func IsValidID(id string) bool {
	return ValidateJobId(id) == nil
}

func ValidateJobId(jobId string) error {
	if jobId == "" {
		return errors.New("jobId cannot be empty")
	}

	if len(jobId) > 255 {
		return errors.New("jobId exceeds maximum length")
	}

	if !jobIdRegex.MatchString(jobId) {
		return errors.New("jobId contains invalid characters")
	}

	if strings.Contains(jobId, string(os.PathSeparator)) ||
		strings.Contains(jobId, "/") ||
		strings.Contains(jobId, "\\") {
		return errors.New("jobId cannot contain path separators")
	}

	if jobId == "." || jobId == ".." {
		return errors.New("jobId cannot be '.' or '..'")
	}

	return nil
}

func ValidatePathComponent(name, value string, pattern *regexp.Regexp, maxLen int) error {
	if value == "" {
		return nil
	}

	if len(value) > maxLen {
		return fmt.Errorf("%s exceeds maximum length of %d", name, maxLen)
	}

	if !pattern.MatchString(value) {
		return fmt.Errorf("%s contains invalid characters", name)
	}

	if strings.Contains(value, "..") {
		return fmt.Errorf("%s cannot contain '..'", name)
	}

	if strings.HasPrefix(value, "/") || strings.HasPrefix(value, "\\") {
		return fmt.Errorf("%s cannot be an absolute path", name)
	}

	return nil
}

func ValidateDatastore(datastore string) error {
	if datastore == "" {
		return errors.New("datastore cannot be empty")
	}
	return ValidatePathComponent("datastore", datastore, datastoreRegex, 255)
}

func ValidateNamespace(ns string) error {
	if ns == "" {
		return nil
	}

	if err := ValidatePathComponent("namespace", ns, namespaceRegex, 255); err != nil {
		return err
	}

	parts := strings.Split(ns, "/")
	for _, part := range parts {
		if part == "." || part == ".." || part == "" {
			return errors.New("namespace contains invalid path segments")
		}
	}

	return nil
}

func ValidateBackupType(backupType string) error {
	if backupType == "" {
		return errors.New("backup-type cannot be empty")
	}
	if !backupTypeRegex.MatchString(backupType) {
		return errors.New("backup-type must be 'vm', 'ct', or 'host'")
	}
	return nil
}

func ValidateBackupID(backupID string) error {
	if backupID == "" {
		return errors.New("backup-id cannot be empty")
	}
	return ValidatePathComponent("backup-id", backupID, backupIDRegex, 255)
}

func ValidateFileName(fileName string) error {
	if fileName == "" {
		return errors.New("file-name cannot be empty")
	}
	return ValidatePathComponent("file-name", fileName, fileNameRegex, 255)
}

func ValidateSnapshot(snapshot string) error {
	if snapshot == "" {
		return nil // Optional field
	}
	return ValidatePathComponent("snapshot", snapshot, snapshotRegex, 512)
}

func ValidateSubpath(name, subpath string) error {
	if subpath == "" {
		return nil
	}

	subpath = strings.TrimPrefix(subpath, "/")
	subpath = strings.TrimPrefix(subpath, "\\")

	if err := ValidatePathComponent(name, subpath, subpathRegex, 4096); err != nil {
		return err
	}

	cleaned := filepath.Clean(subpath)
	if strings.HasPrefix(cleaned, "..") {
		return fmt.Errorf("%s contains path traversal", name)
	}

	return nil
}

func ValidateScriptPath(name, scriptPath string) error {
	if scriptPath == "" {
		return nil // Optional field
	}

	if len(scriptPath) > 4096 {
		return fmt.Errorf("%s exceeds maximum length", name)
	}

	if !filepath.IsAbs(scriptPath) {
		return fmt.Errorf("%s must be an absolute path", name)
	}

	cleaned := filepath.Clean(scriptPath)
	if strings.Contains(cleaned, "..") {
		return fmt.Errorf("%s contains path traversal", name)
	}

	return nil
}

func ValidateExclusionPath(path string) error {
	if path == "" {
		return errors.New("exclusion path cannot be empty")
	}

	if len(path) > 4096 {
		return errors.New("exclusion path exceeds maximum length")
	}

	if !exclusionPathRegex.MatchString(path) {
		return errors.New("exclusion path contains invalid characters")
	}

	if strings.Contains(path, "\x00") {
		return errors.New("exclusion path contains null bytes")
	}

	normalizedPath := strings.ReplaceAll(path, "\\", "/")

	if strings.Contains(normalizedPath, "/../") ||
		strings.HasPrefix(normalizedPath, "../") ||
		strings.HasSuffix(normalizedPath, "/..") {
		return errors.New("exclusion path contains path traversal")
	}

	isGlobPattern := strings.HasPrefix(path, "*") || strings.HasPrefix(path, "?")

	isAbsolute := strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\")

	if !isAbsolute && !isGlobPattern {
		return errors.New("exclusion path must be absolute or a valid glob pattern")
	}

	return nil
}

func SanitizeMountPoint(mountPoint, basePath string) error {
	cleanMount := filepath.Clean(mountPoint)
	cleanBase := filepath.Clean(basePath)

	if !IsPathWithin(cleanBase, cleanMount) {
		return fmt.Errorf("mount point is outside allowed base path")
	}

	return nil
}

func IsPathWithin(base, p string) bool {
	base = filepath.Clean(base)
	p = filepath.Clean(p)
	if base == p {
		return true
	}
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
