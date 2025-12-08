//go:build linux

package plus

type VersionResponse struct {
	Version string `json:"version"`
}

type ScriptConfig struct {
	AgentUrl       string
	UpdaterUrl     string
	ServerUrl      string
	BootstrapToken string
}

