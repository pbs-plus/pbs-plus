package types

type AgentToken struct {
	Token     string `config:"type=string,required" json:"token"`
	Comment   string `config:"type=string" json:"comment"`
	CreatedAt int    `config:"key=created_at,type=int,required" json:"created_at"`
	Revoked   bool   `config:"type=bool" json:"revoked"`
}
