package sync

type TargetStatus struct {
	IsReachable bool   `json:"reachable"`
	IsLocked    bool   `json:"locked"`
	Message     string `json:"message"`
}
