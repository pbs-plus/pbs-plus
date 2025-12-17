package types

type Exclusion struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
	JobID   string `json:"job_id"`
}
