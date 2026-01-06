package types

type Exclusion struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
	JobId   string `json:"job_id"`
}
