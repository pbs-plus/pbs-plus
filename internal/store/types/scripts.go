package types

type Script struct {
	Path        string `config:"type=string,required" json:"path"`
	Description string `json:"description"`
	JobCount    int    `json:"job_count"`
	TargetCount int    `json:"target_count"`
	Script      string `json:"script"`
}
