package testhelpers

// Request represents a request to the server
type Request struct {
	AgentID string `json:"agent_id"`
	Data    string `json:"data,omitempty"`
}

// Response represents a response from the server
type Response struct {
	Token   string `json:"token,omitempty"`
	Message string `json:"message"`
}
