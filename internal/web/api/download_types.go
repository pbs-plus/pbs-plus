//go:build linux

package api

type VersionResponse struct {
	Version string `json:"version"`
}

type ScriptConfig struct {
	AgentUrl       string
	ServerUrl      string
	BootstrapToken string
}
