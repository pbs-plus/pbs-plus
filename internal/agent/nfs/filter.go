package nfs

import (
	"regexp"
	"strings"
)

func wildcardToRegex(pattern string) string {
	// Escape backslashes and convert path to regex-friendly format
	escapedPattern := regexp.QuoteMeta(pattern)

	// Replace double-star wildcard ** with regex equivalent (any directory depth)
	escapedPattern = strings.ReplaceAll(escapedPattern, `\*\*`, `.*`)

	// Replace single-star wildcard * with regex equivalent (any single directory level)
	escapedPattern = strings.ReplaceAll(escapedPattern, `\*`, `[^\/]*`)

	if strings.HasPrefix(escapedPattern, "/") {
		escapedPattern = "^" + escapedPattern
	}

	// Ensure the regex matches paths that start with the pattern and allows for subdirectories
	return escapedPattern + `(\/|$)`
}

func skipPath(path string) bool {
	excludedPaths := []string{
		"$RECYCLE.BIN",
		"$WinREAgent",
		"pagefile.sys",
		"swapfile.sys",
		"hiberfil.sys",
		"System Volume Information",
		`Config.Msi`,
		`/Documents and Settings`,
		`MSOCache`,
		`PerfLogs`,
		`/Program Files`,
		`/Program Files (x86)`,
		`/ProgramData`,
		`/Recovery`,
		`/Users/Default`,
		`/Users/Public`,
		`/Windows`,
		`/Users/*/AppData/Local/Temp`,
		`/Users/*/AppData/Local/Microsoft/Windows/INetCache`,
		`/Users/*/AppData/Local/Microsoft/Windows/History`,
		`/Users/*/AppData/Local/Microsoft/Edge`,
		`/Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache`,
		`/Users/*/AppData/Local/Packages`,
		`/Users/*/AppData/Roaming/Microsoft/Windows/Recent`,
		`/Users/*/AppData/Local/Mozilla/Firefox/Profiles/*/cache2`,
		`/Users/*/AppData/Local/Mozilla/Firefox/Profiles/*/offlineCache`,
		`/Users/*/AppData/Local/Mozilla/Firefox/Profiles/*/startupCache`,
		`/Users/*/AppData/Local/Thunderbird/Profiles/*/cache2`,
		`/Users/*/AppData/Local/Thunderbird/Profiles/*/offlineCache`,
		`/Users/*/AppData/Roaming/Thunderbird/Crash Reports`,
		`/Users/*/AppData/Roaming/Mozilla/Firefox/Crash Reports`,
		`/Users/*/AppData/Local/Microsoft/OneDrive/Temp`,
		`/Users/*/AppData/Local/Microsoft/OneDrive/logs`,
		`/Users/*/AppData/Local/Spotify/Storage`,
		`/Users/*/AppData/Local/Spotify/Data`,
		`/Users/*/AppData/Local/Slack/Cache`,
		`/Users/*/AppData/Local/Slack/Code Cache`,
		`/Users/*/AppData/Local/Slack/GPUCache`,
		`/Users/*/AppData/Roaming/Zoom/bin`,
		`/Users/*/AppData/Roaming/Zoom/data`,
		`/Users/*/AppData/Roaming/Zoom/logs`,
		`/Users/*/AppData/Local/BraveSoftware`,
		`/Users/*/AppData/**log**`,
	}

	normalizedPath := strings.ToUpper(path)

	for _, excludePath := range excludedPaths {
		regexPattern := wildcardToRegex(excludePath)
		regex := regexp.MustCompile("(?i)" + regexPattern)

		if regex.MatchString(normalizedPath) {
			return true
		}
	}

	return false
}
