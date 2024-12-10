//go:build windows

package cache

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type PartialFileData struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
}

type PartialFileResp struct {
	Data []PartialFileData `json:"data"`
}

var PartialFilePathRegexes = CompilePartialFileList()

func CompilePartialFileList() []*regexp.Regexp {
	var partialResp PartialFileResp
	err := agent.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/d2d/partial-file",
		nil,
		&partialResp,
	)
	if err != nil {
		partialResp = PartialFileResp{
			Data: []PartialFileData{},
		}
	}

	partialPatterns := []string{}

	for _, partialPattern := range partialResp.Data {
		trimmedLine := strings.TrimSpace(partialPattern.Path)
		partialPatterns = append(partialPatterns, trimmedLine)
	}

	var compiledRegexes []*regexp.Regexp

	// Compile excluded patterns
	for _, pattern := range partialPatterns {
		rexp, err := utils.GlobToRegex(pattern)
		if err != nil {
			continue
		}
		compiledRegexes = append(compiledRegexes, regexp.MustCompile("(?i)"+rexp))
	}

	return compiledRegexes
}
