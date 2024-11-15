//go:build windows

package cache

import (
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
)

type PartialFileData struct {
	Substring string `json:"substring"`
	Comment   string `json:"comment"`
}

type PartialFileResp struct {
	Data []PartialFileData `json:"data"`
}

var FileExtensions = CompilePartialFileList()

func CompilePartialFileList() []string {
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

	fileExtensions := []string{}
	for _, partial := range partialResp.Data {
		fileExtensions = append(fileExtensions, partial.Substring)
	}
	return fileExtensions
}
