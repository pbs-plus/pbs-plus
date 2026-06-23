// Package token resolves the local PBS-plus API token used to authenticate
// backup uploads (bkf2pxar, mtfjob, pxar-mount commit listener).
//
// pbs-plus writes the token as JSON {"tokenid","value"} to a well-known path;
// several historical locations are probed. The returned string is in the PBS
// API-token form "tokenid:value".
package token

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// localTokenCandidates are the paths searched for the pbs-plus token JSON.
var localTokenCandidates = []string{
	filepath.Join(conf.DbBasePath, "pbs-plus-token.json"),
	filepath.Join("/etc/proxmox-backup", "pbs-plus-token.json"),
	filepath.Join(conf.StatePrefix, "pbs-plus-token.json"),
}

// DefaultAPIURL is the default PBS REST API base URL.
const DefaultAPIURL = "https://localhost:8007/api2/json"

// ReadLocal reads the pbs-plus token from the first candidate path that
// parses, returning it in PBS API-token form ("tokenid:value"), or "".
func ReadLocal() string {
	for _, p := range localTokenCandidates {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var tok struct {
			TokenID string `json:"tokenid"`
			Value   string `json:"value"`
		}
		if err := json.Unmarshal(data, &tok); err != nil {
			continue
		}
		if tok.Value != "" {
			return tok.TokenID + ":" + tok.Value
		}
	}
	return ""
}
