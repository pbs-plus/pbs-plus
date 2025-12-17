package types

type AgentToken struct {
	Token      string `json:"token"`
	Comment    string `json:"comment"`
	CreatedAt  int    `json:"created_at"`
	Revoked    bool   `json:"revoked"`
	WinInstall string `json:"win_install"`
}
