//go:build windows

package cache

import (
	"net/http"
	"strings"

	"github.com/gobwas/glob"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
)

type ExclusionData struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
}

type ExclusionResp struct {
	Data []ExclusionData `json:"data"`
}

func CompileExcludedPaths() ([]*pattern.GlobPattern, error) {
	var exclusionResp ExclusionResp
	_, err := agent.ProxmoxHTTPRequest(
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

	excludedPatterns := []*pattern.GlobPattern{}

	for _, userExclusions := range exclusionResp.Data {
		trimmedLine := strings.TrimSpace(userExclusions.Path)
		glob, err := glob.Compile(trimmedLine)
		if err != nil {
			continue
		}
		excludedPatterns = append(excludedPatterns, &pattern.GlobPattern{
			Glob: glob,
			RawString: trimmedLine,
		})
	}

	syslog.L.Infof("Retrieved exclusions: %v", excludedPatterns)

	return excludedPatterns, nil
}
