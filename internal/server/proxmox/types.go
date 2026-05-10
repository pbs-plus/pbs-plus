package proxmox

type TaskResponse struct {
	Data  Task `json:"data"`
	Total int  `json:"total"`
}

type Task struct {
	WID        string `json:"id"`
	Node       string `json:"node"`
	PID        int    `json:"pid"`
	PStart     uint64 `json:"pstart"`
	StartTime  int64  `json:"starttime"`
	EndTime    int64  `json:"endtime"`
	TaskId     string `json:"task_id"`
	UPID       string `json:"upid"`
	User       string `json:"user"`
	WorkerType string `json:"worker_type"`
	Status     string `json:"status"`
	ExitStatus string `json:"exitstatus"`
}
