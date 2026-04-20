package validate

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gobwas/glob"
	"github.com/pbs-plus/pbs-plus/internal/calendarevent"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

var (
	datastoreRegex     = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	namespaceRegex     = regexp.MustCompile(`^(?:(?:(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)/){0,7}(?:[A-Za-z0-9_][A-Za-z0-9._\-]*))?$`)
	backupTypeRegex    = regexp.MustCompile(`^(vm|ct|host)$`)
	backupIDRegex      = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	fileNameRegex      = regexp.MustCompile(`^[a-zA-Z0-9_.-]+\.(pxar|img|fidx|didx)$`)
	jobIdRegex         = regexp.MustCompile(`^(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)$`)
	snapshotRegex      = regexp.MustCompile(`^[a-zA-Z0-9/_:.-]+$`)
	subpathRegex       = regexp.MustCompile(`^[a-zA-Z0-9/_.\\ :~@+()!,=\[\]{} -]*$`)
	exclusionPathRegex = regexp.MustCompile(`^[a-zA-Z0-9/_.\\ :~@+()!,=\[\]{} *?-]+$`)
	restorePathRegex   = regexp.MustCompile(`^[a-zA-Z0-9/_.\\ :*?[\]{},-]+$`)
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

func validatePathComponent(name, value string, pattern *regexp.Regexp, maxLen int) error {
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
	return validatePathComponent("datastore", datastore, datastoreRegex, 255)
}

func ValidateNamespace(ns string) error {
	if ns == "" {
		return nil
	}

	if err := validatePathComponent("namespace", ns, namespaceRegex, 255); err != nil {
		return err
	}

	parts := strings.SplitSeq(ns, "/")
	for part := range parts {
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
	return validatePathComponent("backup-id", backupID, backupIDRegex, 255)
}

func ValidateFileName(fileName string) error {
	if fileName == "" {
		return errors.New("file-name cannot be empty")
	}
	return validatePathComponent("file-name", fileName, fileNameRegex, 255)
}

func ValidateSnapshot(snapshot string) error {
	if snapshot == "" {
		return nil // Optional field
	}
	return validatePathComponent("snapshot", snapshot, snapshotRegex, 512)
}

func ValidateSubpath(name, subpath string) error {
	if subpath == "" {
		return nil
	}

	subpath = strings.TrimPrefix(subpath, "/")
	subpath = strings.TrimPrefix(subpath, "\\")

	if err := validatePathComponent(name, subpath, subpathRegex, 4096); err != nil {
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

	return nil
}

func ValidateRestorePath(name, path string) error {
	if path == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}

	if len(path) > 4096 {
		return fmt.Errorf("%s exceeds maximum length", name)
	}

	if !restorePathRegex.MatchString(path) {
		return fmt.Errorf("%s contains invalid characters", name)
	}

	if strings.Contains(path, "\x00") {
		return fmt.Errorf("%s contains null bytes", name)
	}

	normalizedPath := strings.ReplaceAll(path, "\\", "/")
	if strings.Contains(normalizedPath, "/../") ||
		strings.HasPrefix(normalizedPath, "../") ||
		strings.HasSuffix(normalizedPath, "/..") {
		return fmt.Errorf("%s contains path traversal", name)
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

// Slugify replaces spaces with hyphens.
func Slugify(input string) string {
	runes := []rune(input)
	var hasHyphen, hasSpace bool
	for _, r := range runes {
		if r == '-' {
			hasHyphen = true
		} else if r == ' ' {
			hasSpace = true
		}
		if hasHyphen && hasSpace {
			break
		}
	}
	if !hasSpace {
		return input
	}
	if !hasHyphen {
		for i, r := range runes {
			if r == ' ' {
				runes[i] = '-'
			}
		}
		return string(runes)
	}
	var builder strings.Builder
	builder.Grow(len(runes))
	for i, r := range runes {
		if r == ' ' {
			prevIsHyphen := i > 0 && runes[i-1] == '-'
			nextIsHyphen := i < len(runes)-1 && runes[i+1] == '-'
			if prevIsHyphen || nextIsHyphen {
				continue
			}
			builder.WriteRune('-')
		} else {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func IsValidPathString(path string) bool {
	if path == "" {
		return true
	}
	if strings.Contains(path, "//") {
		return false
	}
	for _, r := range path {
		if r == 0 || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func IsValidPattern(pattern string) bool {
	_, err := glob.Compile(pattern)
	return err == nil
}

func ValidateOnCalendar(value string) error {
	if value == "" {
		return fmt.Errorf("calendar specification cannot be empty")
	}
	_, err := calendarevent.Parse(value)
	if err != nil {
		return fmt.Errorf("invalid calendar specification: %v", err)
	}
	return nil
}

func IsValid(path string) bool {
	if path == "" {
		return false
	}
	cleanPath := filepath.Clean(path)
	if strings.HasPrefix(cleanPath, "..") || strings.Contains(cleanPath, string(filepath.Separator)+"..") {
		return false
	}
	if strings.Contains(path, "\x00") {
		return false
	}
	return true
}

func ValidateTargetPath(path string) bool {
	if after, ok := strings.CutPrefix(path, "agent://"); ok {
		parts := strings.Split(after, "/")
		if len(parts) != 2 {
			return false
		}
		return net.ParseIP(parts[0]) != nil
	}
	return strings.HasPrefix(path, "/")
}

func DecodePath(encoded string) string {
	decoded, err := url.QueryUnescape(encoded)
	if err != nil {
		return encoded
	}
	decoded = strings.ReplaceAll(decoded, "-", "+")
	decoded = strings.ReplaceAll(decoded, "_", "/")
	switch len(decoded) % 4 {
	case 2:
		decoded += "=="
	case 3:
		decoded += "="
	}
	data, err := base64.StdEncoding.DecodeString(decoded)
	if err != nil {
		return encoded
	}
	return string(data)
}

func IsValidShellScriptWithShebang(scriptContent string) bool {
	if scriptContent == "" {
		return false
	}
	lines := strings.Split(scriptContent, "\n")
	if len(lines) == 0 {
		return false
	}
	firstLine := lines[0]
	return strings.HasPrefix(firstLine, "#!") && len(strings.TrimSpace(firstLine)) > 2
}

func SaveScriptToFile(scriptContent string) (string, error) {
	if err := os.MkdirAll(conf.ScriptsBasePath, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", conf.ScriptsBasePath, err)
	}
	tmpfile, err := os.CreateTemp(conf.ScriptsBasePath, "script-*.sh")
	if err != nil {
		return "", fmt.Errorf("failed to create file in directory %s: %w", conf.ScriptsBasePath, err)
	}
	defer tmpfile.Close()
	if _, err := tmpfile.WriteString(scriptContent); err != nil {
		os.Remove(tmpfile.Name())
		return "", fmt.Errorf("failed to write script to file %s: %w", tmpfile.Name(), err)
	}
	return tmpfile.Name(), nil
}

func UpdateScriptContentToFile(filePath string, newScriptContent string) error {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for %s: %w", filePath, err)
	}
	if !strings.HasPrefix(absPath, conf.ScriptsBasePath) {
		return fmt.Errorf("invalid file path: %s is outside the allowed directory", absPath)
	}
	err = os.WriteFile(absPath, []byte(newScriptContent), 0644)
	if err != nil {
		return fmt.Errorf("failed to update file %s: %w", absPath, err)
	}
	return nil
}

func ReadScriptContentFromFile(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	return string(content), nil
}
