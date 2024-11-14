//go:build windows

package sftp

import (
	"regexp"
	"strings"
	"unicode"
)

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

	return escapedPattern + `(\\|$)`
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
		`Microsoft\**\RecoveryStore**`,
		`Microsoft\**\Windows\**.edb`,
		`Microsoft\**\Windows\**.log`,
		`Microsoft\**\Windows\Cookies**`,
		`Microsoft\**\Logs**`,
		`Users\Public\AccountPictures`,
		`I386`,
		`Internet Explorer\`,
		`MSOCache`,
		`NTUSER**`,
		`UsrClass.dat`,
		`Thumbs.db`,
		`AppData\Local\Temp**`,
		`AppData\Temp**`,
		`Local Settings\Temp**`,
		`**.tmp`,
		`AppData\**cache**`,
		`AppData\**Crash Reports`,
		`AppData\Local\AMD\DxCache`,
		`AppData\Local\Apple Computer\Mobile Sync`,
		`AppData\Local\Comms\UnistoreDB`,
		`AppData\Local\ElevatedDiagnostics`,
		`AppData\Local\Microsoft\Edge\User Data\Default\Cache`,
		`AppData\Local\Microsoft\VSCommon\**SQM**`,
		`AppData\Local\Microsoft\Windows\Explorer`,
		`AppData\Local\Microsoft\Windows\INetCache`,
		`AppData\Local\Microsoft\Windows\UPPS`,
		`AppData\Local\Microsoft\Windows\WebCache`,
		`AppData\Local\Microsoft\Windows Store`,
		`AppData\Local\Packages`,
		`Application Data\Apple Computer\Mobile Sync`,
		`Application Data\Application Data**`,
		`Dropbox\Dropbox.exe.log`,
		`Dropbox\QuitReports`,
		`Google\Chrome\User Data\Default\Cache`,
		`Google\Chrome\User Data\Default\Cookies`,
		`Google\Chrome\User Data\Default\Cookies-journal`,
		`Google\Chrome\**LOCK**`,
		`Google\Chrome\**Current**`,
		`Google\Chrome\Safe Browsing**`,
		`BraveSoftware\Brave-Browser\User Data\Default\Cache`,
		`BraveSoftware\Brave-Browser\User Data\Default\Cookies`,
		`BraveSoftware\Brave-Browser\User Data\Default\Cookies-journal`,
		`BraveSoftware\Brave-Browser\**LOCK**`,
		`BraveSoftware\Brave-Browser\**Current**`,
		`BraveSoftware\Brave-Browser\Safe Browsing**`,
		`iPhoto Library\iPod Photo Cache`,
		`cookies.sqlite-**`,
		`permissions.sqlite-**`,
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

func (h *SftpHandler) skipFile(path string) bool {
	snapSplit := strings.Split(h.Snapshot.SnapshotPath, "\\")
	snapRoot := strings.Join(snapSplit[:len(snapSplit)-1], "\\")

	pathWithoutSnap := strings.TrimPrefix(path, snapRoot)
	normalizedPath := strings.ToUpper(strings.TrimPrefix(pathWithoutSnap, "\\"))

	if strings.TrimSpace(normalizedPath) == "" {
		return false
	}

	for _, regex := range excludedPathRegexes {
		if regex.MatchString(normalizedPath) {
			return true
		}
	}

	invalidAttr, err := invalidAttributes(path)
	if err != nil || invalidAttr {
		return true
	}

	return false
}
