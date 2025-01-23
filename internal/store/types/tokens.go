package types

type AgentToken struct {
	Token     string `json:"token"`
	Comment   string `json:"comment"`
	CreatedAt int64  `json:"created_at"`
	Revoked   bool   `json:"revoked"`
}

