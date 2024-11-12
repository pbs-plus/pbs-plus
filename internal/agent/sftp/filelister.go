//go:build windows
// +build windows

package sftp

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/sftp"
)

type FileLister struct {
	files []os.FileInfo
}

func (fl *FileLister) ListAt(fileList []os.FileInfo, offset int64) (int, error) {
	if offset >= int64(len(fl.files)) {
		return 0, io.EOF
	}

	n := copy(fileList, fl.files[offset:])
	if n < len(fileList) {
		return n, io.EOF
	}
	return n, nil
}

func (h *SftpHandler) FileLister(dirPath string) (*FileLister, error) {
	dirEntries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	fileInfos := make([]os.FileInfo, 0, len(dirEntries))
	for _, entry := range dirEntries {
		select {
		case <-h.ctx.Done():
			return &FileLister{files: fileInfos}, nil
		default:
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}

			fullPath := filepath.Join(dirPath, entry.Name())
			if skipFile(fullPath, info) {
				continue
			}
			fileInfos = append(fileInfos, info)
		}
	}

	return &FileLister{files: fileInfos}, nil
}

func (h *SftpHandler) FileStat(filename string) (*FileLister, error) {
	var stat fs.FileInfo
	var err error

	isRoot := strings.TrimPrefix(filename, h.Snapshot.SnapshotPath) == ""

	if isRoot {
		stat, err = os.Stat(filename)
		if err != nil {
			return nil, err
		}
	} else {
		stat, err = os.Lstat(filename)
		if err != nil {
			return nil, err
		}
	}

	if skipFile(filename, stat) {
		return nil, fmt.Errorf("access denied or restricted file: %s", filename)
	}

	return &FileLister{files: []os.FileInfo{stat}}, nil
}

func (h *SftpHandler) setFilePath(r *sftp.Request) {
	r.Filepath = filepath.Join(h.Snapshot.SnapshotPath, filepath.Clean(r.Filepath))
}

func wildcardToRegex(pattern string) string {
	// Escape backslashes and convert path to regex-friendly format
	escapedPattern := regexp.QuoteMeta(pattern)

	escapedPattern = strings.ReplaceAll(escapedPattern, ":", "")

	// Replace double-star wildcard ** with regex equivalent (any directory depth)
	escapedPattern = strings.ReplaceAll(escapedPattern, `\*\*`, `.*`)

	// Replace single-star wildcard * with regex equivalent (any single directory level)
	escapedPattern = strings.ReplaceAll(escapedPattern, `\*`, `[^\\]*`)

	// Ensure the regex matches paths that start with the pattern and allows for subdirectories
	return "^" + escapedPattern + `(\\|$)`
}

func skipFile(path string, fileInfo os.FileInfo) bool {
	restrictedDirs := []string{
		"$RECYCLE.BIN",
		"$WinREAgent",
		"pagefile.sys",
		"swapfile.sys",
		"hiberfil.sys",
		"System Volume Information",
	}

	for _, dir := range restrictedDirs {
		normalizedName := strings.ToUpper(fileInfo.Name())
		if fileInfo.IsDir() && normalizedName == strings.ToUpper(dir) {
			return true
		}
	}

	excludedPaths := []string{
		`C:\Config.Msi`,
		`C:\Documents and Settings`,
		`C:\MSOCache`,
		`C:\PerfLogs`,
		`C:\Program Files`,
		`C:\Program Files (x86)`,
		`C:\ProgramData`,
		`C:\Recovery`,
		`C:\Users\Default`,
		`C:\Users\Public`,
		`C:\Windows`,
		`C:\Users\*\AppData\Local\Temp`,
		`C:\Users\*\AppData\Local\Microsoft\Windows\INetCache`,
		`C:\Users\*\AppData\Local\Microsoft\Windows\History`,
		`C:\Users\*\AppData\Local\Microsoft\Edge`,
		`C:\Users\*\AppData\Local\Google\Chrome\User Data\Default\Cache`,
		`C:\Users\*\AppData\Local\Packages`,
		`C:\Users\*\AppData\Roaming\Microsoft\Windows\Recent`,
		`C:\Users\*\AppData\Local\Mozilla\Firefox\Profiles\*\cache2`,
		`C:\Users\*\AppData\Local\Mozilla\Firefox\Profiles\*\offlineCache`,
		`C:\Users\*\AppData\Local\Mozilla\Firefox\Profiles\*\startupCache`,
		`C:\Users\*\AppData\Local\Thunderbird\Profiles\*\cache2`,
		`C:\Users\*\AppData\Local\Thunderbird\Profiles\*\offlineCache`,
		`C:\Users\*\AppData\Roaming\Thunderbird\Crash Reports`,
		`C:\Users\*\AppData\Roaming\Mozilla\Firefox\Crash Reports`,
		`C:\Users\*\AppData\Local\Microsoft\OneDrive\Temp`,
		`C:\Users\*\AppData\Local\Microsoft\OneDrive\logs`,
		`C:\Users\*\AppData\Local\Spotify\Storage`,
		`C:\Users\*\AppData\Local\Spotify\Data`,
		`C:\Users\*\AppData\Local\Slack\Cache`,
		`C:\Users\*\AppData\Local\Slack\Code Cache`,
		`C:\Users\*\AppData\Local\Slack\GPUCache`,
		`C:\Users\*\AppData\Roaming\Zoom\bin`,
		`C:\Users\*\AppData\Roaming\Zoom\data`,
		`C:\Users\*\AppData\Roaming\Zoom\logs`,
	}

	normalizedPath := strings.TrimPrefix(path, "C:\\Windows\\TEMP\\pbs-plus-vss\\")
	normalizedPath = strings.ToUpper(normalizedPath)

	for _, excludePath := range excludedPaths {
		regexPattern := wildcardToRegex(excludePath)
		regex := regexp.MustCompile("(?i)" + regexPattern)

		if regex.MatchString(normalizedPath) {
			return true
		}
	}

	return false
}
