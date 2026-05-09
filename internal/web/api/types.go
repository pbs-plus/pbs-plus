package api

import (
	"github.com/pbs-plus/pbs-plus/internal/store/database"
)




type BackupsResponse struct {
	Data   []database.Backup `json:"data"`
	Digest string            `json:"digest"`
}

type BackupConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Backup   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupUPIDsResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    []database.Tasks  `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type RestoresResponse struct {
	Data   []database.Restore `json:"data"`
	Digest string             `json:"digest"`
}

type RestoreConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Restore  `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type RestoreRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}



type TargetsResponse struct {
	Data   []database.Target `json:"data"`
	Digest string            `json:"digest"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Target   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type AgentConfigResponse struct {
	Errors  map[string]string  `json:"errors"`
	Message string             `json:"message"`
	Data    database.AgentHost `json:"data"`
	Status  int                `json:"status"`
	Success bool               `json:"success"`
}



type TokensResponse struct {
	Data   []database.AgentToken `json:"data"`
	Digest string                `json:"digest"`
}

type TokenConfigResponse struct {
	Errors  map[string]string   `json:"errors"`
	Message string              `json:"message"`
	Data    database.AgentToken `json:"data"`
	Status  int                 `json:"status"`
	Success bool                `json:"success"`
}



type ExclusionsResponse struct {
	Data   []database.Exclusion `json:"data"`
	Digest string               `json:"digest"`
}

type ExclusionConfigResponse struct {
	Errors  map[string]string   `json:"errors"`
	Message string              `json:"message"`
	Data    *database.Exclusion `json:"data"`
	Status  int                 `json:"status"`
	Success bool                `json:"success"`
}


type VersionResponse struct {
	Version string `json:"version"`
}

type ScriptConfig struct {
	AgentUrl       string
	ServerUrl      string
	BootstrapToken string
}



type ScriptsResponse struct {
	Data   []database.Script `json:"data"`
	Digest string            `json:"digest"`
}

type ScriptConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Script   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
