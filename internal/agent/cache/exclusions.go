//go:build windows

package cache

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
)

type ExclusionData struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
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

	excludedPatterns := []string{}

	for _, userExclusions := range exclusionResp.Data {
		trimmedLine := strings.TrimSpace(userExclusions.Path)
		excludedPatterns = append(excludedPatterns, trimmedLine)
	}

	var compiledRegexes []*regexp.Regexp

	// Compile excluded patterns
	for _, pattern := range excludedPatterns {
		compiledRegexes = append(compiledRegexes, regexp.MustCompile("(?i)^"+globToRegex(pattern)))
	}

	return compiledRegexes
}

// Converts a glob pattern to a regex pattern
func globToRegex(pattern string) string {
	// Handle leading / or \ for current directory matches
	if strings.HasPrefix(pattern, "/") || strings.HasPrefix(pattern, "\\") {
		pattern = "." + pattern
	} else {
		pattern = "**/" + pattern
	}

	// Escape characters and replace glob wildcards
	regexPattern := regexp.QuoteMeta(pattern)
	regexPattern = strings.ReplaceAll(regexPattern, `\*\*`, `.*`)    // Double star for any depth
	regexPattern = strings.ReplaceAll(regexPattern, `\*`, `[^/\\]*`) // Single star for single level
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, `.`)       // ? for any single character
	regexPattern = strings.ReplaceAll(regexPattern, `\[`, `[`)       // Keep character sets
	regexPattern = strings.ReplaceAll(regexPattern, `\]`, `]`)
	regexPattern = strings.ReplaceAll(regexPattern, `\!`, `!`) // Keep negation sets

	// Replace forward and backward slashes with a platform-agnostic pattern
	regexPattern = strings.ReplaceAll(regexPattern, `/`, `[\\\\/]`)  // Match / or \
	regexPattern = strings.ReplaceAll(regexPattern, `\\`, `[\\\\/]`) // Match / or \

	// Handle trailing slashes for directories
	if strings.HasSuffix(pattern, "/") || strings.HasSuffix(pattern, "\\") {
		regexPattern += `(?:[\\/]|$)` // Match only directories
	} else {
		regexPattern += `(?:$|[\\/]$)` // Match files and subdirectories
	}

	return regexPattern
}

// Precompiled regex patterns for excluded paths
var ExcludedPathRegexes = CompileExcludedPaths()
