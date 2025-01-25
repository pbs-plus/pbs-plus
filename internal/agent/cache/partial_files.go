//go:build windows

package cache

import (
	"net/http"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
)

type PartialFileData struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
}

type PartialFileResp struct {
	Data []PartialFileData `json:"data"`
}

func CompilePartialFileList() []*pattern.Pattern {
	var partialResp PartialFileResp
	_, err := agent.ProxmoxHTTPRequest(
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

	syslog.L.Infof("Retrieved partial files: %v", partialPatterns)

	var compiledPatterns []*pattern.Pattern

	// Compile excluded patterns
	for _, patternStr := range partialPatterns {
		ptrn, err := pattern.NewPattern(patternStr)
		if err != nil {
			continue
		}
		compiledPatterns = append(compiledPatterns, ptrn)
	}

	return compiledPatterns
}
