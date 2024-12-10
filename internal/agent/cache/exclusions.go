//go:build windows

package cache

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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
		rexp, err := utils.GlobToRegex(pattern)
		if err != nil {
			continue
		}
		compiledRegexes = append(compiledRegexes, regexp.MustCompile("(?i)"+rexp))
	}

	return compiledRegexes
}

// Precompiled regex patterns for excluded paths
var ExcludedPathRegexes = CompileExcludedPaths()
