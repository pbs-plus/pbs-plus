package auth

type AgentRequest struct {
	AgentID string `json:"agent_id"`
	Data    string `json:"data"`
}

type AgentResponse struct {
	Token   string `json:"token,omitempty"`
	Message string `json:"message"`
}

