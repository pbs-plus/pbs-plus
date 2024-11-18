//go:build windows

package cache

import (
	"net/http"
	"regexp"
	"strings"
	"unicode"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
)

type ExclusionData struct {
	Path     string `json:"path"`
	IsGlobal bool   `json:"is_global"`
	Comment  string `json:"comment"`
}

type ExclusionResp struct {
	Data []ExclusionData `json:"data"`
}

func CompileExcludedPaths() []*regexp.Regexp {
	var exclusionResp ExclusionResp
	err := agent.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/d2d/exclusion",
		nil,
		&exclusionResp,
	)
	if err != nil {
		exclusionResp = ExclusionResp{
			Data: []ExclusionData{},
		}
	}

	excludedPaths := []string{}

	for _, userExclusions := range exclusionResp.Data {
		if userExclusions.IsGlobal {
			excludedPaths = append(excludedPaths, userExclusions.Path)
		}
	}

	var compiledRegexes []*regexp.Regexp
	for _, pattern := range excludedPaths {
		regexPattern := wildcardToRegex(pattern)
		compiledRegexes = append(compiledRegexes, regexp.MustCompile("(?i)"+regexPattern))
	}

	return compiledRegexes
}

// Precompiled regex patterns for excluded paths
var ExcludedPathRegexes = CompileExcludedPaths()

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
