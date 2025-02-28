//go:build linux

//go:generate msgp
//msgp:ignore VersionResponse ScriptConfig

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

type BackupReq struct {
	JobId string `msg:"job_id"`
	Drive string `msg:"drive"`
}
