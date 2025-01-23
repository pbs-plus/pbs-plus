package types

type Target struct {
	Name             string `db:"name" json:"name"`
	Path             string `db:"path" json:"path"`
	IsAgent          bool   `json:"is_agent"`
	ConnectionStatus bool   `json:"connection_status"`
	Auth             string `json:"auth"`
	TokenUsed        string `json:"token_used"`
}

