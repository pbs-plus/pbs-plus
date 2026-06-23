// Package pbstoken resolves the local PBS-plus API token used to authenticate
// backup uploads (bkf2pxar, mtfjob, pxar-mount commit listener).
//
// pbs-plus writes the token as JSON {"tokenid","value"} to a well-known path;
// several historical locations are probed. The returned string is in the PBS
// API-token form "tokenid:value".
package pbstoken

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// localTokenCandidates are the paths searched, in priority order, for the
// pbs-plus token JSON written by the pbs-plus server.
var localTokenCandidates = []string{
	filepath.Join(conf.DbBasePath, "pbs-plus-token.json"),
	filepath.Join("/etc/proxmox-backup", "pbs-plus-token.json"),
	filepath.Join(conf.StatePrefix, "pbs-plus-token.json"),
}

// DefaultAPIURL is the default PBS REST API base URL used by the pbs-plus
// backup upload paths (bkf2pxar, mtf migration, pxar-mount commit).
const DefaultAPIURL = "https://localhost:8007/api2/json"

// ReadLocal reads the pbs-plus token from the first candidate path that exists
// and parses successfully. It returns the token in PBS API-token form
// ("tokenid:value"), or "" if no token file is available.
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
