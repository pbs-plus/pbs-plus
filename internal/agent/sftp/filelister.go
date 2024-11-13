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
	"unicode"

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
			if skipFile(fullPath) {
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

	if skipFile(filename) {
		return nil, fmt.Errorf("access denied or restricted file: %s", filename)
	}

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
	runed := []rune(pattern)
	if strings.Contains(pattern, ":\\") && unicode.IsLetter(runed[0]) {
		escapedPattern = "^" + escapedPattern
	}

	return escapedPattern
}

func compileExcludedPaths() []*regexp.Regexp {
	excludedPaths := []string{
		`:\hiberfil.sys`,
		`:\pagefile.sys`,
		`:\swapfile.sys`,
		`:\autoexec.bat`,
		`:\Config.Msi`,
		`:\Documents and Settings`,
		`:\Recycled`,
		`:\Recycler`,
		`:\$$Recycle.Bin`,
		`:\Recovery`,
		`:\Program Files`,
		`:\Program Files (x86)`,
		`:\ProgramData`,
		`:\PerfLogs`,
		`:\Windows`,
		`:\Windows.old`,
		`:\$$WINDOWS.~BT`,
		`:\$$WinREAgent`,
		"$RECYCLE.BIN",
		"$WinREAgent",
		"System Volume Information",
		"Temporary Internet Files",
		`Microsoft\Windows\Recent`,
		`Microsoft\**\RecoveryStore`,
		`Microsoft\**\Windows\**.edb`,
		`Microsoft\**\Windows\**.log`,
		`Microsoft\**\Windows\Cookies`,
		`Microsoft\**\Logs`,
		`Users\Public\AccountPictures`,
		`I386`,
		`Internet Explorer\`,
		`MSOCache`,
		`NTUSER`,
		`UsrClass.dat`,
		`Thumbs.db`,
		`AppData\Local\Temp`,
		`AppData\Temp`,
		`Local Settings\Temp`,
		`**.tmp`,
		`AppData\**cache`,
		`AppData\**Crash Reports`,
		`AppData\Local\AMD\DxCache`,
		`AppData\Local\Apple Computer\Mobile Sync`,
		`AppData\Local\Comms\UnistoreDB`,
		`AppData\Local\ElevatedDiagnostics`,
		`AppData\Local\Microsoft\Edge\User Data\Default\Cache`,
		`AppData\Local\Microsoft\VSCommon\**SQM`,
		`AppData\Local\Microsoft\Windows\Explorer`,
		`AppData\Local\Microsoft\Windows\INetCache`,
		`AppData\Local\Microsoft\Windows\UPPS`,
		`AppData\Local\Microsoft\Windows\WebCache`,
		`AppData\Local\Microsoft\Windows Store`,
		`AppData\Local\Packages`,
		`Application Data\Apple Computer\Mobile Sync`,
		`Application Data\Application Data`,
		`Dropbox\Dropbox.exe.log`,
		`Dropbox\QuitReports`,
		`Google\Chrome\User Data\Default\Cache`,
		`Google\Chrome\User Data\Default\Cookies`,
		`Google\Chrome\User Data\Default\Cookies-journal`,
		`Google\Chrome\**LOCK`,
		`Google\Chrome\**Current`,
		`Google\Chrome\Safe Browsing`,
		`BraveSoftware\Brave-Browser`,
		`iPhoto Library\iPod Photo Cache`,
		`cookies.sqlite-`,
		`permissions.sqlite-`,
		`Local Settings\History`,
		`OneDrive\.849C9593-D756-4E56-8D6E-42412F2A707B`,
		`Safari\Library\Caches`,
	}

	var compiledRegexes []*regexp.Regexp
	for _, pattern := range excludedPaths {
		regexPattern := wildcardToRegex(pattern)
		compiledRegexes = append(compiledRegexes, regexp.MustCompile("(?i)"+regexPattern))
	}

	return compiledRegexes
}

// Precompiled regex patterns for excluded paths
var excludedPathRegexes = compileExcludedPaths()

func skipFile(path string) bool {
	normalizedPath := strings.TrimPrefix(path, "C:\\Windows\\TEMP\\pbs-plus-vss\\")
	normalizedPath = strings.ToUpper(normalizedPath)

	for _, regex := range excludedPathRegexes {
		if regex.MatchString(normalizedPath) {
			return true
		}
	}

	return false
}
